from oncopulse import db


def test_custom_mode_profile_roundtrip(tmp_path):
    conn = db.get_conn(str(tmp_path / "custom_modes.db"))
    db.init_db(conn)
    cfg = {
        "include_papers": True,
        "include_trials": False,
        "phase_2_3_only": True,
        "scoring_weights": {"phase_iii": 11},
    }
    db.upsert_custom_mode_profile(conn, "My Mode", cfg)
    rows = db.list_custom_mode_profiles(conn)
    assert rows
    assert rows[0]["name"] == "My Mode"
    assert rows[0]["config"]["phase_2_3_only"] is True
    db.delete_custom_mode_profile(conn, "My Mode")
    assert not db.list_custom_mode_profiles(conn)
