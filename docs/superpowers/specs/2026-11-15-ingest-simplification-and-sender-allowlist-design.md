# Ingest simplification and sender allow-list — design

Status: draft, awaiting approval. Date: 2026-11-15. Branch: `simplify-ingest` (off `main` at `cd6c474`).
Supersedes: the subject-filter acceptance contract
(`2026-11-14-subject-filter-acceptance-contract-design.md`), which is withdrawn unscheduled;
its implementation lives on the abandoned branch `filter-contract` and is not merged.

## 1 The decision

Subject filters are removed from the product. The intended deployment is one dedicated
inbox that only the operator sends to: a human decides what to forward, and everything
that arrives is meant to be ingested. The mailbox is the filter.

This deletes an attack class instead of defending it. No operator-supplied regex means no
catastrophic backtracking, no pattern-admission question, no migration story for rules
that a stricter validator later refuses, and no mailbox disabled because a validator
disagreed with a saved pattern.

Replacing the filter is not another matcher. It is **who is allowed to put things in
Papra at all**: a sender allow-list.

## 2 Removed

| Area | What goes |
|---|---|
| `papra_suite/mail.py` | `subject_matches`, `validate_subject_filter`, `_subject_filter_match`, `SUBJECT_FILTER_TIMEOUT_S`, `SUBJECT_VALIDATION_TIMEOUT_S` |
| `papra_suite/safematch.py` | the whole module (its only purpose was killable execution of the operator's regex) |
| `papra_suite/regex_worker.py` | the child process itself |
| `papra_suite/regexlint.py`, `filter_policy.py`, `filter_admission.py` | from `filter-contract`, unmerged; simply not carried over |
| `papra_suite/store.py` | reads/writes of `mailboxes.subject_filter` |
| tests | `test_safematch.py`, `test_regexlint.py`, `test_filter_policy.py`, `test_filter_admission.py`, and the filter cases inside `test_mail_poller.py`, `test_mail_parsing.py`, `test_mail_session.py` |

`mailboxes.subject_filter` **stays in the schema**. Dropping a SQLite column means
rebuilding the table for no benefit; the column becomes unread and unused, and old
databases keep opening.

The poller's per-message decision becomes: parse, take the PDF attachments, upload.
Non-PDF attachments stay ignored, as they are today, and stay counted.

## 3 Explicitly preserved, because each one cost a bug to learn

- **No subject-length refusal.** Subject is a label. It must never decide whether a
  document is ingested; a long subject line retiring a valid invoice is a data-loss bug,
  not a protection. Resource bounds belong on the parse and on attachment bytes, where
  they already are.
- **Failures are visible, never silent.** A killed upload, an unreadable message, a
  quarantined file: recorded on the mailbox and counted, because a poll that quietly
  succeeds while dropping mail is the worst failure mode this codebase has produced.
- **Anything that can retire a message must also advance the cursor**, or the message is
  re-fetched forever. (M3 learned this with a frozen poll that never engaged the
  3-strike path, because a frozen poll never raises.)
- **Nothing is retired on an undecided question.** If we cannot determine whether a
  message is allowed, we do not mark it seen.

## 4 The sender allow-list

### Where it lives

`PAPRA_ALLOWED_SENDERS`, a comma-separated list, e.g.

```
PAPRA_ALLOWED_SENDERS=tobi@example.com, family.example.com
```

An entry containing `@` is an exact address match; an entry without `@` is a whole
domain. Matching is case-insensitive, ignores display names and angle brackets, and
compares the address with any `+suffix` stripped (so `tobi+invoice@gmail.com` matches
`tobi@gmail.com`). Global, not per mailbox: there is one drop-box.

M4 may put this behind a dashboard form. Until then it is configuration, and
`papra-suite check` prints the parsed list so a typo is caught at startup rather than
discovered as missing mail.

### The check

IMAP gives us the delivered message and nothing else, so the `From:` header alone is
decoration: anyone can type any address into it. Providers, however, commonly stamp an
`Authentication-Results:` header before delivery recording their own SPF/DKIM/DMARC
verdicts, and that note is written by the provider, not the sender.

So admission is two conditions, not one:

| Provider verdict | Allow-listed sender | Result |
|---|---|---|
| `dmarc=pass` and the DMARC domain is the allowed address/domain | yes | **ingest** |
| `dmarc=pass` | no | ignore |
| present and failing (`dmarc=fail`, `spf=fail`, `dkim=fail`) | — | ignore, warn |
| absent | `PAPRA_REQUIRE_AUTH_HEADER=false` | ingest, warn once per sender |
| absent | `PAPRA_REQUIRE_AUTH_HEADER=true` (default) | ignore, warn |

`PAPRA_REQUIRE_AUTH_HEADER` is the escape hatch, and it exists because provider behaviour
varies: some add the header, some only for inbound-from-internet mail, some phrase it
differently. It defaults strict, because a filter that silently accepts everything when
the header is missing is worse than no filter — it reports protection it is not
providing. Relaxation is a deliberate, logged choice.

`Sender:` is honoured when present, since forwarding commonly sets it.

### What happens to rejected mail

- Nothing is downloaded beyond headers, nothing is uploaded, no dedup row is written.
- The message is marked `\Seen` and the cursor advances, so a busy junk folder does not
  refetch and re-reject the same messages every poll.
- `PollResult.ignored_by_allowlist` counts them; the mailbox row is untouched and the
  poll does **not** fail. Rejection is a normal outcome, not an outage — this is the
  distinction that keeps a healthy mailbox from being disabled by three strikes.
- Log line names the sender address and the reason, and never the mailbox password.

### What this protects against, and what it does not

Stops: strangers who guess or harvest the address, bulk spam, automated probing, and a
forwarded address that leaked into a mailing list. With `dmarc=pass` enforced it also
stops the casual version of spoofing your own domain.

Does not stop: a compromised mailbox of an allowed sender, a provider that cannot
authenticate and the hatch left open, or a malicious document from a sender you trust.
That last one is the real residual, and it is deliberately out of scope here — see §7.

## 5 Open empirical question

Whether the operator's provider writes `Authentication-Results`, and in what wording.
Answered by one forwarded message, before implementation is considered done:
`papra-suite check` should print the parsed verdict for the newest message it can read.
If the header is absent in practice, the default flips to relaxed *deliberately*, and the
spec is amended to say so rather than leaving a strict check that only ever drops mail.

## 6 Testing

- Matching unit tests: exact, domain, case, display names, `+suffix`, `Sender:`,
  malformed headers, empty list (empty list means **deny everything**, not allow: a
  configuration mistake must fail closed and loudly at `check`).
- Verdict parsing: pass, fail, absent, multiple `Authentication-Results` lines, the
  `dmarc=pass` on a non-allow-listed domain that merely appears in the header.
- Poller: allowed sender ingests; disallowed sender marks seen, advances cursor, uploads
  nothing, writes no dedup row, does not fail the poll; undecided verdict does not mark
  seen.
- Regression: a PDF forwarded as an attached `.eml`, the same invoice forwarded twice
  (Papra's content-hash 409 path must treat it as already ingested, not an error), and a
  long subject that must be ingested normally.
- Removal verification: after deletion, grep for `subject_filter|KillableMatcher|
  regex_worker` returns nothing outside the schema and the docs that explain the change.

## 7 Out of scope, recorded so it does not get lost

- **Prompt injection is a downstream authority problem, not a file category.** OCR text
  reaches an LLM for titling today and will reach the assistant in M4. The requirement is
  that document text can never authorise an action — no tool calls, no sends, no
  deletes, no payments — regardless of what it says. Labelling it "untrusted" is
  communication, not enforcement. Belongs to the M4 design.
- Antivirus scanning, encrypted or exotic containers, image-only invoices, page-count
  limits, per-sender quotas.
- Anything that requires the operator to approve each import: the forwarding *is* the
  approval, and a second gate would undo the simplification.

---

## Revision 2 (2026-11-15, after review) — the allow-list was oversold; these corrections bind

The claim that checking the provider's authentication note turns a `From:` allow-list into
a lock was too confident, and the reviewer's objection is correct in a way that changes
the design. **`Authentication-Results:` is a header, and a sender can put any header in a
message they compose**, including `Authentication-Results: dmarc=pass`. Trusting any
header that matches is a bypass, not a verification.

So:

1. **A trust boundary comes first, and a generic parser cannot supply it.** Before any
   authentication check ships, we name *which* authentication-service identifier we trust
   for the chosen provider, and confirm the provider strips or outranks a sender-supplied
   header claiming that identity. Until that is established for the actual provider, the
   verdict is **inconclusive, not authenticated** — and inconclusive behaves like
   `absent` in the table above. A test where a message injects its own
   `dmarc=pass` header is a required case, and it must not be ingested.
2. **DMARC authenticates a domain, not a person.** It proves the visible `From:` *domain*
   aligned. So the allow-list identity is the single outer `From:` address, and
   **`Sender:` is no longer honoured** — DMARC does not authenticate it, so preferring it
   would let an attacker choose the identity we check. For forwarded mail we authorise the
   forwarding message itself, not addresses inside any attached message.
3. **No universal `+suffix` stripping.** Plus-addressing means different things on
   different providers; stripping it widens who is allowed in. Exact addresses first; a
   domain entry is a deliberate, broader permission.
4. **Relaxed mode relaxes only the evidence requirement.** An allow-listed sender is still
   required; what the switch drops is the authentication proof, with a warning that
   spoofing protection is gone. Previously it could have been read as "accept if no
   header", which is wrong.

**Separate mailbox, not an alias.** A shared mailbox makes admission depend on `To:`/`Cc:`,
and those are sender-typed text, not proof of where the provider delivered the message;
Bcc and forwarding can omit the real recipient entirely; and the app would then hold IMAP
credentials to a personal inbox. So the app polls a mailbox that exists only for Papra.
No `PAPRA_EXPECTED_RECIPIENT` is added: a configuration whose correctness depends on a
header the attacker writes is not a configuration worth having.

**Two implementation notes carried forward.** "No body fetch for rejected senders" is a
*requirement*, not a description of existing code — the poller must fetch headers first and
only then bodies. And a message we cannot parse is attacker-controlled input, not an
infrastructure failure: it is skipped and counted, never retried forever, and never
permanently blocks the cursor.

`papra-suite check` stays configuration-only; reading a real message moves to an explicit
diagnostic command that uses `BODY.PEEK`, changes no flags and no cursor, and prints
headers and verdicts rather than message contents.
