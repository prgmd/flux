"""탐색 화면의 순수 판단: 표본 사용자 고르기, 칸의 출처 경로."""

import random
from pathlib import Path

import pytest

from flux.explorer import pick, provenance
from flux.explorer.pick import UserStats

BUILD_SOURCE = (Path(__file__).resolve().parents[1] / "src" / "flux" / "silver" / "build.py").read_text(
    encoding="utf-8")


def user(name, listens=2000, importer=0.5, clients=1, removed=0, collab=0.0):
    return UserStats(name, listens, importer, clients, removed, collab)


def crowd(count=300, seed=1):
    rng = random.Random(seed)
    return [
        user(f"u{i:03}", listens=rng.randint(500, 12000), importer=rng.choice([0, 0.5, 1.0]),
             clients=rng.randint(1, 4), removed=rng.randint(0, 100), collab=rng.random() * 0.4)
        for i in range(count)
    ]


def test_유형마다_정원만큼_뽑는다():
    chosen = pick.pick_users(crowd())
    counts = {}
    for label in chosen.values():
        counts[label] = counts.get(label, 0) + 1
    assert counts == {label: quota for label, quota, _ in pick.KINDS}
    assert len(chosen) == 40


def test_한_사용자는_한_유형에만_들어간다():
    chosen = pick.pick_users(crowd())
    assert len(chosen) == len(set(chosen))


def test_청취_범위_밖의_사용자는_뽑지_않는다():
    stats = crowd() + [user("tiny", listens=pick.MIN_LISTENS - 1, importer=1.0),
                       user("huge", listens=pick.MAX_LISTENS + 1, importer=1.0)]
    chosen = pick.pick_users(stats)
    assert "tiny" not in chosen and "huge" not in chosen


def test_범위의_양끝은_포함한다():
    stats = [user("low", listens=pick.MIN_LISTENS, importer=1.0),
             user("high", listens=pick.MAX_LISTENS, importer=1.0)]
    assert set(pick.pick_users(stats)) == {"low", "high"}


def test_같은_씨앗이면_입력_순서와_상관없이_같은_결과다():
    stats = crowd()
    shuffled = list(stats)
    random.Random(9).shuffle(shuffled)
    assert pick.pick_users(stats, seed=5) == pick.pick_users(shuffled, seed=5)


def test_씨앗이_다르면_다른_사용자가_뽑힌다():
    assert pick.pick_users(crowd(), seed=1) != pick.pick_users(crowd(), seed=2)


def test_유형의_조건을_지킨다():
    stats = {u.user_name: u for u in crowd()}
    for name, label in pick.pick_users(stats.values()).items():
        rule = {kind[0]: kind[2] for kind in pick.KINDS}[label]
        assert rule(stats[name])


def test_앞_유형이_먼저_뽑고_뒤_유형은_남은_사람에서_뽑는다():
    stats = [user(f"m{i}", importer=1.0, clients=5, removed=100) for i in range(12)]
    kinds = (("앞", 10, lambda u: u.removed_duplicates >= 10), ("뒤", 10, lambda u: u.importer_share >= 0.9))
    chosen = pick.pick_users(stats, kinds=kinds)
    assert sum(1 for label in chosen.values() if label == "앞") == 10
    assert sum(1 for label in chosen.values() if label == "뒤") == 2


def test_가장_까다로운_유형이_맨_앞에_있다():
    assert pick.KINDS[0][0] == "중복 묶음이 있음"


def test_모자란_유형을_알려_준다():
    chosen = pick.pick_users([user("only", importer=1.0)])
    gaps = pick.shortfalls(chosen)
    assert gaps["중복 묶음이 있음"] == 10
    assert gaps["실시간 앱 중심"] == 10
    assert "협업곡이 많음" in gaps


def test_후보가_없으면_빈_결과다():
    assert pick.pick_users([]) == {}


def test_JSON_경로로_값을_꺼낸다():
    doc = {"a": {"b": {"c": 5}}, "x": 1}
    assert provenance.json_path_value(doc, "$.a.b.c") == 5
    assert provenance.json_path_value(doc, "$.x") == 1


def test_없는_경로와_객체가_아닌_곳을_지나면_None이다():
    doc = {"a": "글자", "n": None}
    assert provenance.json_path_value(doc, "$.nothing") is None
    assert provenance.json_path_value(doc, "$.a.b") is None
    assert provenance.json_path_value(doc, "$.n.b") is None


def test_지원하지_않는_경로는_거부한다():
    with pytest.raises(ValueError):
        provenance.json_path_value({}, "a.b")


def test_출처_경로가_Silver_코드가_읽는_경로와_같다():
    for column, paths, _ in provenance.PROVENANCE:
        if column in provenance.UNCHECKED_AGAINST_BUILD:
            continue
        for path in paths:
            assert f'"{path}"' in BUILD_SOURCE, f"{column}: {path}가 build.py에 없다"


def test_Silver_코드가_읽는_경로를_빠짐없이_소개한다():
    import re
    used = set(re.findall(r'_json(?:_array)?\(\s*"(\$[^"]+)"', BUILD_SOURCE))
    declared = {path for _, paths, _ in provenance.PROVENANCE for path in paths} | set(provenance.QUALITY_PATHS)
    assert used <= declared, f"소개하지 않은 경로: {sorted(used - declared)}"
