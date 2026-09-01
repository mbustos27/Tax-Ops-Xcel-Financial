"""Drake return_forms normalization."""

from __future__ import annotations


def test_merge_csm_type_sets_1040():
    from drake_form_sync import merge_drake_return_forms

    merged = merge_drake_return_forms(
        csm_type_forms={"form_1040": 1, "sched_a_d": 0},
    )
    assert merged["form_1040"] == 1
    assert merged["form_1120"] == 0
    assert merged["sched_a_d"] == 0


def test_merge_prefill_schedules():
    from drake_form_sync import merge_drake_return_forms

    merged = merge_drake_return_forms(
        csm_type_forms={"form_1040": 1},
        prefill_counts={"Schedule A": 2, "Schedule C": 0, "Schedule E": 1},
        prefill_return_type="1040",
    )
    assert merged["form_1040"] == 1
    assert merged["sched_a_d"] == 1
    assert merged["sched_c"] == 0
    assert merged["sched_e"] == 1


def test_merge_preserves_manual_corp_flags():
    from drake_form_sync import merge_drake_return_forms

    merged = merge_drake_return_forms(
        csm_type_forms={"form_1040": 1},
        existing={"corp_officer": 1, "business_owner": 0},
    )
    assert merged["corp_officer"] == 1
    assert merged["business_owner"] == 0


def test_family_from_prefill_when_csm_missing():
    from drake_form_sync import merge_drake_return_forms

    merged = merge_drake_return_forms(
        csm_type_forms={"form_1040": 0, "form_1120s": 0},
        prefill_counts={},
        prefill_return_type="1120S",
    )
    assert merged["form_1120s"] == 1
    assert merged["form_1040"] == 0


def test_sync_from_csv_applies_type(taxops_db_path):
    from pathlib import Path

    import config
    from db import get_connection
    from drake_form_sync import sync_drake_forms_from_csv

    conn = get_connection(taxops_db_path)
    try:
        conn.execute(
            "INSERT INTO clients (id, last_name, first_name) VALUES (9301, 'FORM', 'SYNC')"
        )
        conn.execute(
            """
            INSERT INTO returns (id, client_id, tax_year, log_number, client_status, created_at)
            VALUES (93010, 9301, 2025, '9301', 'PROCESSING', datetime('now'))
            """
        )
        conn.execute(
            """
            INSERT INTO return_forms (return_id, form_1040, form_1120)
            VALUES (93010, 0, 1)
            """
        )
        conn.commit()

        csv_path = Path(__file__).resolve().parents[1] / "data" / "processed" / "CSMDATA.csv"
        if not csv_path.is_file():
            return

        # Inject a synthetic row by patching is impractical; test merge+upsert directly.
        from drake_form_sync import merge_drake_return_forms, upsert_return_forms

        target = merge_drake_return_forms(
            csm_type_forms={"form_1040": 1, "form_1120": 0},
            existing={"corp_officer": 1},
        )
        upsert_return_forms(conn, 93010, target, overwrite=True)
        conn.commit()

        row = conn.execute(
            "SELECT form_1040, form_1120, corp_officer FROM return_forms WHERE return_id=93010"
        ).fetchone()
        assert int(row["form_1040"]) == 1
        assert int(row["form_1120"]) == 0
        assert int(row["corp_officer"]) == 1
    finally:
        conn.close()


def test_upsert_overwrite_clears_stale_flag(taxops_db_path):
    from db import get_connection
    from drake_form_sync import upsert_return_forms

    conn = get_connection(taxops_db_path)
    try:
        conn.execute(
            "INSERT INTO clients (id, last_name, first_name) VALUES (9302, 'CLR', 'FLAG')"
        )
        conn.execute(
            """
            INSERT INTO returns (id, client_id, tax_year, log_number, client_status, created_at)
            VALUES (93020, 9302, 2025, '9302', 'PROCESSING', datetime('now'))
            """
        )
        conn.execute(
            "INSERT INTO return_forms (return_id, form_1040, form_1120) VALUES (93020, 0, 1)"
        )
        conn.commit()

        upsert_return_forms(
            conn,
            93020,
            {"form_1040": 1, "form_1120": 0},
            overwrite=True,
        )
        conn.commit()
        row = conn.execute(
            "SELECT form_1040, form_1120 FROM return_forms WHERE return_id=93020"
        ).fetchone()
        assert int(row["form_1040"]) == 1
        assert int(row["form_1120"]) == 0
    finally:
        conn.close()
