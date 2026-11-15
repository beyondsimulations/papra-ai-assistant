import argparse
import contextlib
import signal
import sys

from papra_suite import __version__
from papra_suite.config import ConfigError, load_config

# Signals that must stop the service cleanly. SIGTERM is what `docker stop`
# sends; SIGINT is Ctrl-C.
STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)


async def run_serve(config, **kwargs) -> None:
    """Run serve until SIGTERM/SIGINT (or its own stop conditions) end it.

    WHY HANDLERS EXIST. serve already stops on an asyncio.Event, but with no
    handler SIGTERM's DEFAULT disposition is to terminate the process where it
    stands: serve's `finally` (cancel the poll loops and workers, close the
    clients, close the store, and above all the task teardowns for every
    poll loop's regex child) never runs. So a `docker stop` used to leave a
    busy child orphaned at ppid=1 — only the child's own self-deadline ended
    it, after it had burned a core for its whole budget. add_signal_handler is
    the only mechanism that can run loop code from a signal, so the handler
    does the one thing that matters: it sets serve's stop event, and shutdown
    is the same clean path the tests drive. The handlers are removed on the
    way out so a library use of this module does not keep the process's
    signals pointed at a dead loop.
    """
    import asyncio

    from papra_suite.pipeline import serve

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    previous: dict[int, object] = {}
    for sig in STOP_SIGNALS:
        try:
            previous[sig] = signal.getsignal(sig)
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError, ValueError):
            previous.pop(sig, None)   # no signal support here: stop still works
    try:
        await serve(config, stop=stop, **kwargs)
    finally:
        for sig, handler in previous.items():
            with contextlib.suppress(Exception):
                loop.remove_signal_handler(sig)
                signal.signal(sig, handler)   # restore exactly what we replaced


def main() -> int:
    parser = argparse.ArgumentParser(prog="papra-suite")
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check", help="validate config, open and migrate the store, exit")
    sub.add_parser("serve", help="run the pipeline: pollers, OCR/title workers, sweep")

    args = parser.parse_args()

    try:
        config = load_config()
    except ConfigError as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2

    if args.command == "check":
        from papra_suite.store import Store

        config.data_dir.mkdir(parents=True, exist_ok=True)
        store = Store(config.data_dir / "state.db")
        try:
            store.migrate()
        finally:
            store.close()
        print(f"config ok; store migrated; data dir: {config.data_dir}")
        return 0

    if args.command == "serve":
        import asyncio

        asyncio.run(run_serve(config))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
