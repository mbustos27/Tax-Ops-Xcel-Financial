"""DEBT-7: _processed_uids LRU cap in mail_watcher — memory stays bounded."""
from __future__ import annotations

import pytest


def test_processed_uids_cap_is_positive():
    """_PROCESSED_UIDS_CAP is a positive integer."""
    from mail_watcher import _PROCESSED_UIDS_CAP
    assert isinstance(_PROCESSED_UIDS_CAP, int)
    assert _PROCESSED_UIDS_CAP > 0


def test_mark_uid_processed_returns_true_for_new():
    """First call for a uid returns True (newly claimed)."""
    import mail_watcher as mw
    original = dict(mw._processed_uids)
    mw._processed_uids.clear()
    try:
        result = mw._mark_uid_processed("INBOX", "uid_test_new_99999")
        assert result is True
    finally:
        mw._processed_uids.clear()
        mw._processed_uids.update(original)


def test_mark_uid_processed_returns_false_for_duplicate():
    """Second call for the same uid returns False (already seen)."""
    import mail_watcher as mw
    original = dict(mw._processed_uids)
    mw._processed_uids.clear()
    try:
        mw._mark_uid_processed("INBOX", "uid_dup_88888")
        result = mw._mark_uid_processed("INBOX", "uid_dup_88888")
        assert result is False
    finally:
        mw._processed_uids.clear()
        mw._processed_uids.update(original)


def test_lru_evicts_oldest_at_cap():
    """When the dict exceeds _PROCESSED_UIDS_CAP, the oldest entry is evicted."""
    import mail_watcher as mw
    original = dict(mw._processed_uids)
    mw._processed_uids.clear()
    cap = mw._PROCESSED_UIDS_CAP
    try:
        # Fill to cap using a small synthetic cap via monkeypatching.
        synthetic_cap = 5
        original_cap = mw._PROCESSED_UIDS_CAP
        mw._PROCESSED_UIDS_CAP = synthetic_cap
        try:
            for i in range(synthetic_cap):
                mw._mark_uid_processed("INBOX", f"uid_{i}")
            # This should evict uid_0
            mw._mark_uid_processed("INBOX", "uid_overflow")
            # uid_0 should be gone
            assert ("INBOX", "uid_0") not in mw._processed_uids
            # uid_overflow should be present
            assert ("INBOX", "uid_overflow") in mw._processed_uids
            # total length should not exceed cap
            assert len(mw._processed_uids) <= synthetic_cap
        finally:
            mw._PROCESSED_UIDS_CAP = original_cap
    finally:
        mw._processed_uids.clear()
        mw._processed_uids.update(original)


def test_lru_uses_ordered_dict():
    """_processed_uids is an OrderedDict so eviction is O(1) FIFO."""
    from collections import OrderedDict
    from mail_watcher import _processed_uids
    assert isinstance(_processed_uids, OrderedDict)


def test_different_folders_are_independent_keys():
    """(folder1, uid) and (folder2, uid) are distinct keys."""
    import mail_watcher as mw
    original = dict(mw._processed_uids)
    mw._processed_uids.clear()
    try:
        mw._mark_uid_processed("INBOX", "uid_shared")
        result = mw._mark_uid_processed("Sent", "uid_shared")
        assert result is True, "Same uid in different folder should be treated as new"
    finally:
        mw._processed_uids.clear()
        mw._processed_uids.update(original)
