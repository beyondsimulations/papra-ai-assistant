"""Sender allow-list: who may put anything into Papra at all.

This is a PERMISSION check, not an authentication check. It reads the visible
From: address, which a sender types themselves — so what it establishes is
"this message claims an address we admitted", which stops strangers, harvested
addresses and bulk mail. It does not stop spoofing, and no test here should be
read as claiming otherwise; the authentication layer is separate and blocked on
a verified provider trust boundary (see the spec's Revision 2).
"""
import pytest

from papra_suite.allowlist import (
    DENY_MALFORMED,
    DENY_SENDER,
    PERMISSION,
    parse_allowlist,
    parse_from_header,
)
from papra_suite.config import ConfigError


class TestParseAllowList:
    def test_entries_split_into_addresses_and_domains(self):
        al = parse_allowlist("Tobi@Example.COM, family.example.com")
        assert al.addresses == frozenset({"tobi@example.com"})
        assert al.domains == frozenset({"family.example.com"})

    def test_empty_list_denies_everything(self):
        # The safe failure for a config typo is a quiet inbox the operator
        # notices at once, not an open door nobody notices.
        al = parse_allowlist("")
        assert al.permits_sender("tobi@example.com") is DENY_SENDER
        al_none = parse_allowlist(None)
        assert al_none.permits_sender("tobi@example.com") is DENY_SENDER
        assert al_none.configured is False

    def test_whitespace_and_trailing_commas_are_tolerated(self):
        al = parse_allowlist("  a@x.com , , b@y.com,  ")
        assert al.addresses == frozenset({"a@x.com", "b@y.com"})

    def test_bare_at_domain_is_an_entry_not_a_syntax_error(self):
        al = parse_allowlist("@family.example.com")
        assert al.domains == frozenset({"family.example.com"})

    def test_nonsense_entry_is_refused_at_parse_time(self):
        # A silently-dropped typo would leave the operator believing an entry
        # they can see in the file is granting access that it does not.
        with pytest.raises(ConfigError):
            parse_allowlist("not an entry")

    def test_plus_address_is_NOT_stripped(self):
        # Plus-addressing means different things per provider; stripping it
        # would widen admission without the operator asking.
        al = parse_allowlist("tobi@example.com")
        assert al.permits_sender("tobi+invoices@example.com") is DENY_SENDER

    def test_rejects_entries_that_cannot_be_an_identity(self):
        for bad in ("a@b@c.com", "a b@c.com", "@", "a@", ".com"):
            with pytest.raises(ConfigError):
                parse_allowlist(bad)


class TestSenderExtraction:
    def test_display_name_and_angle_brackets_are_stripped(self):
        assert parse_from_header("Hans Müller <Tobi@Example.com>") == "tobi@example.com"

    def test_bare_address(self):
        assert parse_from_header("tobi@example.com") == "tobi@example.com"

    def test_missing_or_empty_is_malformed(self):
        assert parse_from_header(None) is None
        assert parse_from_header("") is None
        assert parse_from_header("no-at-sign-here") is None

    def test_multiple_from_addresses_is_malformed(self):
        # One visible From: is the identity. A header offering two cannot be
        # resolved by picking one, because the choice would be the attacker's.
        assert parse_from_header("a@x.com, b@y.com") is None

    def test_encoded_display_name_is_decoded(self):
        assert parse_from_header("=?utf-8?B?UmVjaG51bmc=?= <a@x.com>") == "a@x.com"


class TestPermission:
    def test_exact_address_matches(self):
        al = parse_allowlist("tobi@example.com")
        assert al.permits_sender("tobi@example.com") is PERMISSION

    def test_domain_matches_any_local_part_but_not_a_subdomain(self):
        al = parse_allowlist("example.com")
        assert al.permits_sender("invoice@mail.example.com") is DENY_SENDER
        assert al.permits_sender("anything@example.com") is PERMISSION

    def test_unknown_sender_is_denied(self):
        al = parse_allowlist("tobi@example.com")
        assert al.permits_sender("stranger@other.net") is DENY_SENDER

    def test_malformed_sender_is_denied_as_malformed_not_as_unknown(self):
        # The distinction is what keeps a forged header from being logged as
        # "a sender you might want to allow".
        al = parse_allowlist("tobi@example.com")
        assert al.verdict_for_header("<bad, syntax@x") is DENY_MALFORMED
        assert al.verdict_for_header(None) is DENY_MALFORMED

    def test_description_is_human_readable_for_check_output(self):
        al = parse_allowlist("tobi@example.com, family.example.com")
        text = al.describe()
        assert "tobi@example.com" in text and "family.example.com" in text

    def test_description_of_an_unconfigured_list_says_it_denies(self):
        assert "denies" in parse_allowlist("").describe().lower()


class TestAmbiguousInput:
    """Ambiguity is refused, never interpreted: whichever reading the code
    picked out of an ambiguous header, the sender chose it."""

    def test_an_encoded_word_in_the_address_is_refused(self):
        # Decoding before parsing would turn this into "a@example.com" and
        # admit it. Identity is not prose.
        assert parse_from_header("=?utf-8?Q?a=40example=2Ecom?=") is None

    def test_an_encoded_display_name_is_still_fine(self):
        # Real clients emit these, and the address itself is plain.
        assert parse_from_header("=?utf-8?B?UmVjaG51bmc=?= <a@x.com>") == "a@x.com"

    def test_a_domain_without_a_dot_is_refused(self):
        assert parse_from_header("postmaster@localhost") is None

    def test_a_quoted_local_part_with_a_space_is_refused(self):
        # Legal MIME, but not an identity worth arguing about.
        assert parse_from_header('"tobi m"@example.com') is None

    def test_a_display_name_containing_a_comma_is_still_one_address(self):
        assert parse_from_header('"Müller, Hans" <a@x.com>') == "a@x.com"
