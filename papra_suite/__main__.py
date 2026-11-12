import argparse
import sys

from papra_suite import __version__
from papra_suite.config import ConfigError, load_config


def main() -> int:
    parser = argparse.ArgumentParser(prog="papra-suite")
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check", help="validate config, open and migrate the store, exit")

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

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
