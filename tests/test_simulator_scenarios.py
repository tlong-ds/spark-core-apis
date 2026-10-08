from simulator.scenarios import SCENARIOS, get_scenario


def test_scenarios_have_complete_semantic_expectations():
    required_counts = {"valid", "malformed", "unknown", "http_200"}
    assert set(SCENARIOS) == {
        "malformed_burst",
        "unknown_cidr",
        "accumulator_recompute",
        "task_retry_once",
        "rdd_task_failure",
        "dataframe_ansi_cast_failure",
    }
    for scenario in SCENARIOS.values():
        assert set(scenario.expected_counts) == required_counts
        assert scenario.lines


def test_unknown_scenario_explains_available_choices():
    try:
        get_scenario("not-real")
    except ValueError as error:
        assert "malformed_burst" in str(error)
    else:
        raise AssertionError("unknown scenario should fail")
