from oncopulse.extract_fields import detect_endpoints, detect_phase, detect_sample_size, detect_study_type


def test_detect_phase():
    assert detect_phase("Randomized phase III trial") == "Phase III"
    assert detect_phase("Early dose-finding phase 1 study") == "Phase I"
    assert detect_phase("No phase provided") == "Unknown"


def test_detect_endpoints():
    out = detect_endpoints("Overall survival and progression-free survival improved; toxicity was manageable.")
    assert "OS" in out
    assert "PFS" in out
    assert "Toxicity" in out


def test_detect_sample_size():
    assert detect_sample_size("Patients = 245 were enrolled.") == "N~245"
    assert detect_sample_size("No enrollment details.") == "Unknown"


def test_detect_study_type():
    assert detect_study_type("Randomized multicenter trial") == "Randomized trial"
    assert detect_study_type("Systematic review and meta-analysis") == "Meta-analysis/Systematic review"
    assert detect_study_type("Brief note") == "Unknown"
