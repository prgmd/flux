"""덤프 하나에 파일이 여럿일 때 합치는 규칙.

실제 적재에서 잡힌 버그를 고정한다. 9월 1일자 증분 덤프에는 8월 파일과
9월 파일이 함께 들어 있었고, 파일마다 따로 쓰는 바람에 두 번째 쓰기가
첫 번째 쓰기를 통째로 지웠다. 417만 행이 11행이 됐다.

Spark 없이 검증하려고 데이터프레임 흉내를 낸다. 여기서 보는 것은
**몇 번 합치고 무엇을 다시 매기는가**뿐이고, 실제 행이 맞는지는
대조 불변식이 매 실행마다 본다.
"""

import pytest

from flux.bronze import merge


class FakeFrame:
    """unionByName과 withColumn만 흉내 내는 가짜 데이터프레임."""

    def __init__(self, name):
        self.name = name
        self.merged = []
        self.recolumned = []

    def unionByName(self, other):
        merged = FakeFrame(f"{self.name}+{other.name}")
        merged.merged = self.merged + [other.name]
        merged.recolumned = list(self.recolumned)
        return merged

    def withColumn(self, column, _value):
        self.recolumned.append(column)
        return self


def test_파일이_하나면_그대로_둔다():
    frame = FakeFrame("8월")
    assert merge.combine([frame], record_id="가짜").merged == []


def test_파일이_여럿이면_모두_합친다():
    """하나라도 빠지면 그 파일의 행이 통째로 사라진다."""
    frames = [FakeFrame("8월"), FakeFrame("9월")]
    assert merge.combine(frames, record_id="가짜").merged == ["9월"]


def test_세_개도_순서대로_합친다():
    frames = [FakeFrame(n) for n in ("a", "b", "c")]
    assert merge.combine(frames, record_id="가짜").merged == ["b", "c"]


def test_합친_뒤_행_식별자를_다시_매긴다():
    """파일마다 따로 매긴 번호는 합치면 겹칠 수 있다."""
    result = merge.combine([FakeFrame("8월"), FakeFrame("9월")], record_id="가짜")
    assert "_record_id" in result.recolumned


def test_파일이_하나여도_식별자를_다시_매긴다():
    """경로가 갈리면 한쪽만 검증되고 다른 쪽이 조용히 달라진다."""
    result = merge.combine([FakeFrame("8월")], record_id="가짜")
    assert "_record_id" in result.recolumned


def test_빈_목록은_거부한다():
    """조용히 넘어가면 그 덤프가 통째로 빈 채 대조를 통과할 수 있다."""
    with pytest.raises(ValueError):
        merge.combine([])


def test_제너레이터도_받는다():
    frames = (FakeFrame(n) for n in ("a", "b"))
    assert merge.combine(frames, record_id="가짜").merged == ["b"]
