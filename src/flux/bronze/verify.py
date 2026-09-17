"""적재 결과를 원본과 대조한다.

핵심 불변식은 하나다.

    원본 줄 수 == Bronze 행 수 + 격리 행 수

한 줄이라도 새거나 늘면 이 등식이 깨진다. 행 수가 맞는지만 보는 것보다
강한 검사다. 유효한 줄이 조용히 사라지고 격리된 줄이 늘어나는 식의
어긋남까지 잡아내기 때문이다.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Reconciliation:
    source_lines: int
    bronze_rows: int
    quarantine_rows: int

    @property
    def accounted(self) -> int:
        return self.bronze_rows + self.quarantine_rows

    @property
    def missing(self) -> int:
        """설명되지 않은 줄. 음수면 원본보다 많이 생긴 것이다."""
        return self.source_lines - self.accounted

    @property
    def ok(self) -> bool:
        return self.missing == 0

    @property
    def quarantine_ratio(self) -> float:
        if not self.source_lines:
            return 0.0
        return round(self.quarantine_rows / self.source_lines, 6)

    def explain(self) -> str:
        if self.ok:
            return (
                f"원본 {self.source_lines:,}줄 = "
                f"Bronze {self.bronze_rows:,} + 격리 {self.quarantine_rows:,}"
            )
        if self.missing > 0:
            return (
                f"{self.missing:,}줄이 사라졌다. "
                f"원본 {self.source_lines:,}, 설명된 것 {self.accounted:,}"
            )
        return (
            f"{-self.missing:,}줄이 늘었다. 중복 적재를 의심한다. "
            f"원본 {self.source_lines:,}, 설명된 것 {self.accounted:,}"
        )


def reconcile(source_lines: int, bronze_rows: int, quarantine_rows: int) -> Reconciliation:
    for label, value in (("원본", source_lines), ("Bronze", bronze_rows), ("격리", quarantine_rows)):
        if value < 0:
            raise ValueError(f"{label} 건수가 음수다: {value}")
    return Reconciliation(source_lines, bronze_rows, quarantine_rows)


def count_rows(spark, path: str, source_dump: str | None = None) -> int:
    """Delta 테이블의 행 수. 테이블이 아직 없으면 0으로 본다.

    처음에는 load()를 try로 감싸고 예외 메시지에서 "is not a Delta table"을
    찾았다. 그 방식은 두 가지가 나쁘다.

    - `except (AnalysisException, Exception)`이 결국 모든 예외를 잡는다.
      디스크 오류나 권한 문제도 "테이블이 없다"로 읽힐 수 있었다
    - 메시지 문자열은 Delta 버전이 올라가면 바뀐다. 바뀌면 조용히 raise로 떨어지고,
      왜 첫 적재가 실패하는지 알기 어렵다

    `isDeltaTable`이 정확히 이 질문에 답한다. 없는 경로에도 False를 준다.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    if not DeltaTable.isDeltaTable(spark, path):
        return 0

    df = spark.read.format("delta").load(path)
    if source_dump is not None:
        df = df.filter(F.col("_source_dump") == source_dump)
    return df.count()


def count_lines(path, *, chunk_size: int = 4 * 1024 * 1024) -> int:
    """텍스트 파일의 줄 수. 마지막 줄에 개행이 없어도 센다."""
    rows = 0
    trailing = False
    with open(path, "rb") as fp:
        while chunk := fp.read(chunk_size):
            rows += chunk.count(b"\n")
            trailing = not chunk.endswith(b"\n")
    if trailing:
        rows += 1
    return rows
