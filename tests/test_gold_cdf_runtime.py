import os
from pathlib import Path

import pytest

pytest.importorskip("dbt.cli.main")

from flux.validation.gold_cdf_consume_drill import consume
from flux.validation.gold_cdf import run
from flux.validation.dbt_pilot import bootstrap, execute
from test_dbt_pilot import baseline

pytestmark = pytest.mark.dbt


def test_actual_silver_cdf_updates_all_marts_and_recovers_before_cursor_publication(tmp_path):
    source = os.environ.get("FLUX_CDF_EXPORT_ROOT")
    reference = os.environ.get("FLUX_CDF_REFERENCE_GOLD")
    if not source or not reference:
        pytest.skip("actual Spark CDF exports and completed reference Gold are required")
    result = consume(Path(source), Path(reference), tmp_path / "consumer", Path(__file__).resolve().parents[1] / "dbt/flux_gold")
    assert result["completed"] and result["reference_unchanged"] and result["last_cursor_unchanged"]
    assert len(result["scenarios"]) == 9
    assert all(result["guards"].values())
    assert result["scenarios"][0]["plays"] + 1 == result["scenarios"][-1]["plays"]
    for scenario in result["scenarios"]:
        assert scenario["checks"]["tables_checked"] == 14


def test_actual_delta_cdf_preserves_nonempty_main_and_featured_artist_metrics(baseline, tmp_path):
    import duckdb

    source = os.environ.get("FLUX_CDF_CONTROL_EXPORT_ROOT")
    if not source:
        pytest.skip("actual controlled Spark CDF exports are required")
    source = Path(source)
    project = Path(__file__).resolve().parents[1] / "dbt/flux_gold"
    reference = tmp_path / "reference"
    bootstrap(baseline, reference)
    assert execute(reference, project)["completed"] is True
    initial, changed = tmp_path / "initial", tmp_path / "changed"
    assert run(source / "initial", initial, project, reference=reference)["completed"]
    result = run(source / "changed", changed, project, previous=initial)
    assert result["completed"], result.get("failure")
    with duckdb.connect(str(changed / "gold/pilot.duckdb"), read_only=True) as connection:
        assert connection.execute("select artist_mbid,listens,listeners from analytics.monthly_artist_metrics order by artist_mbid").fetchall() == [("a", 3, 1), ("b", 3, 1)]
        assert connection.execute("select period_listens,linked_listens from analytics.monthly_coverage").fetchone() == (3, 3)
        assert connection.execute("select listens,listeners from analytics.yearly_artist_metrics where artist_mbid='a'").fetchone() == (3, 1)
    replay = run(source / "replay", tmp_path / "replayed", project, previous=changed)
    assert replay["completed"] and replay["no_changes"]
