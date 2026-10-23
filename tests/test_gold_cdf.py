import json
from copy import deepcopy

import pytest

from flux.validation.gold_cdf import apply_feed, bundle, extend_context, reconcile
from flux.validation.pinning import sha256
from flux.validation.silver_cdf_export import KIND, NAMES


def row(month="2026-01", count=2, user="u"):
    return dict(user_name=user, recording_msid="s", listened_month=month, play_count=count)


def event(kind, version=1, **kwargs):
    return {**row(**kwargs), "_change_type": kind, "_commit_version": version}


def test_cdf_applies_updates_in_commit_order_and_retains_before_and_after_periods():
    feed = [event("update_postimage", 2, count=5), event("update_preimage", 1),
            event("update_preimage", 2, count=3), event("update_postimage", 1, count=3)]
    assert apply_feed([row()], feed, start=0, end=2) == [row(count=5)]


def test_key_move_delete_and_insert_do_not_become_additive_counts():
    feed = [event("update_preimage"), event("update_postimage", month="2027-02"),
            event("delete", 2, month="2027-02"), event("insert", 2, user="other", count=1)]
    assert apply_feed([row()], feed, start=0, end=2) == [row(user="other", count=1)]


@pytest.mark.parametrize("fault", ["wrong_before", "duplicate_after", "missing_pair", "outside", "negative", "bool"])
def test_invalid_cdf_is_rejected_before_a_gold_build(fault):
    feed = [event("update_preimage"), event("update_postimage", count=3)]
    if fault == "wrong_before": feed[0]["play_count"] = 9
    elif fault == "duplicate_after": feed.append(event("insert", count=1))
    elif fault == "missing_pair": feed.pop(0)
    elif fault == "outside": feed[0]["_commit_version"] = 0
    elif fault == "negative": feed[1]["play_count"] = -1
    else: feed[1]["play_count"] = True
    with pytest.raises(ValueError): apply_feed([row()], feed, start=0, end=1)


def test_empty_cdf_and_replay_keep_exact_values():
    assert apply_feed([row()], [], start=1, end=1) == [row()]
    assert apply_feed([row()], [], start=1, end=2) == [row()]


def test_reconciliation_requires_old_and_end_snapshots_to_match():
    data = dict(before=[row()], after=[row(count=3)],
                cdf=[event("update_preimage"), event("update_postimage", count=3)])
    assert reconcile([row()], data, start=0, end=1) == [row(count=3)]
    for field in ["before", "after"]:
        bad = deepcopy(data); bad[field][0]["play_count"] = 99
        with pytest.raises(ValueError): reconcile([row()], bad, start=0, end=1)


def test_deleting_the_last_row_produces_an_empty_snapshot():
    assert apply_feed([row()], [event("delete")], start=0, end=1) == []


def test_unreviewed_new_msids_keep_variants_and_never_gain_automatic_artist_credit():
    data = dict(monthly=[], dimension=[], labels=[], credits=[])
    new = [row()]
    meta = [dict(recording_msid="s", track_name="Song (Sped Up Remix)", artist_name="Artist", recording_mbid="unverified")]
    result = extend_context(data, new, meta)
    assert result["dimension"][0]["song_key"] == "msid:s"
    assert result["dimension"][0]["recording_mbid"] is None
    assert result["labels"][0]["variant"] == "sped_up+remix"
    assert result["credits"] == []
    assert data == dict(monthly=[], dimension=[], labels=[], credits=[])


def test_unknown_msid_without_metadata_is_blocked_and_conflicting_versions_are_visible():
    data = dict(monthly=[], dimension=[], labels=[], credits=[])
    with pytest.raises(ValueError): extend_context(data, [row()], [])
    result = extend_context(data, [row()], [dict(recording_msid="s", track_name=name) for name in ["Song", "Song (Live)"]])
    assert result["labels"][0]["variant"] == "conflicting_labels"


@pytest.mark.parametrize("fault", ["artifact", "no_hash", "incomplete", "source", "range", "policy", "bool_version", "regressed"])
def test_bundle_integrity_and_cursor_contract_reject_invalid_exports(tmp_path, fault):
    state = dict(source_id="b", version=0, key=["user_name", "recording_msid", "listened_month"],
                 minimum_ms_played=30000, policy_version=1, timezone="UTC")
    old = dict(root="/experiment/silver", state=state,
               tables={name: dict(id=name, version=0, log_sha256="a" * 64) for name in NAMES})
    new = deepcopy(old)
    for table in new["tables"].values(): table["version"] = 1
    payload = dict(before=[row()], after=[row()], cdf=[], metadata=[])
    target = tmp_path / "payload.json"
    target.write_text(json.dumps(payload))
    report = dict(completed=True, kind=KIND, range_read=True, reader="pyspark-delta", production_applied=False,
                  source_cursor=new, previous_cursor=old, starting_version=0, ending_version=1,
                  artifacts={"payload.json": sha256(target)})
    (tmp_path / "result.json").write_text(json.dumps(report))
    assert bundle(tmp_path)[0]["completed"] is True
    if fault == "artifact": target.write_text("changed")
    elif fault == "no_hash": report["artifacts"] = {}
    elif fault == "incomplete": report["completed"] = False
    elif fault == "source": new["tables"][NAMES[0]]["id"] = "replaced"
    elif fault == "range": report["starting_version"] = 1
    elif fault == "policy": new["state"]["minimum_ms_played"] = 10000
    elif fault == "bool_version": new["tables"][NAMES[0]]["version"] = True
    else: old["tables"][NAMES[1]]["version"] = 2
    (tmp_path / "result.json").write_text(json.dumps(report))
    with pytest.raises(ValueError): bundle(tmp_path)
