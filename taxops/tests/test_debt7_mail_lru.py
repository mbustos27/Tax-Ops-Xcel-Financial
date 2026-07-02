"""DEBT-7: _processed_uids memo cap in mail_watcher — memory stays bounded.

Rewritten for the holding-area architecture's memo API:
``_uid_in_memo`` (read) / ``_add_uid_to_memo`` (write, FIFO-capped) replace
the old single-call ``_mark_uid_processed``. The memo is a ``set`` for O(1)
membership checks; eviction order is tracked separately in
``_processed_uids_fifo`` (an ``OrderedDict`` used purely as an ordered set).
"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _isolate_memo():
    """Snapshot and restore module-level memo state around every test."""
    import mail_watcher as mw

    orig_set = set(mw._processed_uids)
    orig_fifo_items = list(mw._processed_uids_fifo.items())
    orig_cap = mw._PROCESSED_UIDS_CAP
    mw._processed_uids.clear()
    mw._processed_uids_fifo.clear()
    try:
        yield
    finally:
        mw._PROCESSED_UIDS_CAP = orig_cap
        mw._processed_uids.clear()
        mw._processed_uids.update(orig_set)
        mw._processed_uids_fifo.clear()
        mw._processed_uids_fifo.update(orig_fifo_items)


def test_processed_uids_cap_is_positive():
    """_PROCESSED_UIDS_CAP is a positive integer."""
    from mail_watcher import _PROCESSED_UIDS_CAP
    assert isinstance(_PROCESSED_UIDS_CAP, int)
    assert _PROCESSED_UIDS_CAP > 0


def test_uid_in_memo_false_before_add():
    """A UID not yet added to the memo reports as not-in-memo."""
    import mail_watcher as mw
    assert mw._uid_in_memo("INBOX", "uid_test_new_99999") is False


def test_add_uid_to_memo_makes_uid_in_memo_true():
    """After _add_uid_to_memo, _uid_in_memo reports True for that (folder, uid)."""
    import mail_watcher as mw
    mw._add_uid_to_memo("INBOX", "uid_test_new_99999")
    assert mw._uid_in_memo("INBOX", "uid_test_new_99999") is True


def test_add_uid_to_memo_duplicate_is_noop():
    """Adding the same (folder, uid) twice does not grow the memo or raise."""
    import mail_watcher as mw
    mw._add_uid_to_memo("INBOX", "uid_dup_88888")
    mw._add_uid_to_memo("INBOX", "uid_dup_88888")
    assert len(mw._processed_uids) == 1
    assert len(mw._processed_uids_fifo) == 1


def test_lru_evicts_oldest_at_cap(monkeypatch: pytest.MonkeyPatch):
    """When the memo exceeds _PROCESSED_UIDS_CAP, the oldest entry is evicted
    from both _processed_uids (membership set) and _processed_uids_fifo (order)."""
    import mail_watcher as mw

    monkeypatch.setattr(mw, "_PROCESSED_UIDS_CAP", 5)
    for i in range(5):
        mw._add_uid_to_memo("INBOX", f"uid_{i}")

    # This 6th add should evict uid_0 (the oldest).
    mw._add_uid_to_memo("INBOX", "uid_overflow")

    assert mw._uid_in_memo("INBOX", "uid_0") is False
    assert ("INBOX", "uid_0") not in mw._processed_uids_fifo
    assert mw._uid_in_memo("INBOX", "uid_overflow") is True
    assert len(mw._processed_uids) <= 5
    assert len(mw._processed_uids_fifo) <= 5


def test_fifo_backing_store_is_ordered_dict():
    """_processed_uids_fifo is an OrderedDict so FIFO eviction is O(1)."""
    from collections import OrderedDict
    from mail_watcher import _processed_uids_fifo
    assert isinstance(_processed_uids_fifo, OrderedDict)


def test_processed_uids_is_a_set_for_o1_membership():
    """_processed_uids is a plain set — membership checks don't need ordering."""
    from mail_watcher import _processed_uids
    assert isinstance(_processed_uids, set)


def test_different_folders_are_independent_keys():
    """(folder1, uid) and (folder2, uid) are distinct memo entries."""
    import mail_watcher as mw
    mw._add_uid_to_memo("INBOX", "uid_shared")
    assert mw._uid_in_memo("Sent", "uid_shared") is False, (
        "Same uid in a different folder must be treated as a distinct, unseen entry"
    )
