from datetime import datetime, timedelta, timezone

from oncopulse import db
from oncopulse.services.run_pipeline import RunOptions, resolve_incremental_days_back


def test_get_last_successful_run_respects_mode_and_sources(tmp_path):
    conn = db.get_conn(str(tmp_path / "inc.db"))
    db.init_db(conn)

    r1 = db.create_run(conn, "lung", "Immunotherapy", mode_name="m1", sources_key="papers,trials", resolved_days_back=7)
    db.finish_run(conn, r1, "success", 10, 8)

    r2 = db.create_run(conn, "lung", "Immunotherapy", mode_name="m2", sources_key="papers", resolved_days_back=7)
    db.finish_run(conn, r2, "success", 10, 8)

    row = db.get_last_successful_run(conn, "lung", "Immunotherapy", mode_name="m1", sources_key="papers,trials")
    assert row is not None
    assert row["mode_name"] == "m1"
    assert row["sources_key"] == "papers,trials"


def test_resolve_incremental_days_back_from_last_success(tmp_path):
    conn = db.get_conn(str(tmp_path / "inc2.db"))
    db.init_db(conn)
    run_id = db.create_run(conn, "lung", "Immunotherapy", mode_name="m1", sources_key="papers", resolved_days_back=30)
    db.finish_run(conn, run_id, "success", 10, 8)
    old_finished = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    conn.execute("UPDATE run_history SET finished_at = ? WHERE id = ?", (old_finished, run_id))
    conn.commit()

    options = RunOptions(
        days_back=30,
        incremental_cap_days=30,
        mode_name="m1",
        include_papers=True,
        include_trials=False,
        include_preprints=False,
        include_journal_rss=False,
        include_fda_approvals=False,
    )
    resolved_days, _ = resolve_incremental_days_back(conn, "lung", "Immunotherapy", options)
    assert resolved_days in (2, 3)


def test_resolve_incremental_days_back_force_full_refresh(tmp_path):
    conn = db.get_conn(str(tmp_path / "inc3.db"))
    db.init_db(conn)
    options = RunOptions(days_back=14, force_full_refresh=True)
    resolved_days, last = resolve_incremental_days_back(conn, "lung", "Immunotherapy", options)
    assert resolved_days == 14
    assert last is None
