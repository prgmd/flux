"""중복 제거로 무엇을 잃었는지 센다.

docs/deduplication.md가 "합치지 않고 하나만 남긴다"를 고른 근거로 내세운 것이
**잃는 것을 기록해 두면 나중에 판단할 수 있다**였다. 이 스크립트가 그 기록이다.

궁금한 것은 지워진 행 수가 아니라 **지워진 행이 무엇을 들고 있었나**다.
지워진 쪽이 전부 MBID 없는 행이면 잃은 것이 사실상 없고, 지워진 쪽에 MBID가
많으면 "합쳤어야 했나"를 다시 물어야 한다.

세는 방법은 뺄셈이다. 중복 제거 전후로 mbid_source별 행 수를 세고 차를 본다.
윈도우 함수를 한 번 더 돌리지 않으므로 전체를 캐시할 필요가 없다.

적재 잡에 넣지 않고 따로 둔 이유는 이것이 **설계 판단을 위한 일회성 측정**이지
매번 볼 지표가 아니기 때문이다. 48분짜리 잡을 더 늘릴 이유가 없다.

    docker run ... -e FLUX_STORAGE_ROOT=file:///data flux-spark \
        python scripts/measure_dedup_loss.py
"""

import json
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from flux.common import config
from flux.common.spark import session as spark_session
from flux.bronze import schema as bronze_schema
from flux.silver import build, schema

SOURCES = ("MAPPING", "CLIENT", "LASTFM", "NONE")


def counts_by_source(frame) -> dict:
    rows = frame.groupBy("mbid_source").count().collect()
    return {r["mbid_source"]: r["count"] for r in rows}


def _recorded_removed():
    """적재 잡이 남긴 중복 제거 건수. 뺄셈이 성립하는지 대조하는 데 쓴다.

    여기서 세는 "제거 전"은 지금 Bronze에서 다시 계산한 값이고 "제거 후"는
    디스크에 있는 Silver다. 그 사이 Bronze가 늘었으면 둘이 같은 시점이
    아니므로 차가 중복 제거 건수가 아니게 된다. 적재 때 기록해 둔 값과
    맞춰 보면 그 어긋남이 드러난다.
    """
    path = config.local_root() / config.MEASUREMENTS / "silver-build.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8")).get("duplicates_removed")


def main() -> int:
    bronze_path = config.bronze(bronze_schema.TABLE_NAME)
    silver_path = config.silver(schema.TABLE_NAME)

    spark = spark_session("flux-dedup-loss", shuffle_partitions=200)
    started = time.time()
    try:
        bronze = spark.read.format("delta").load(bronze_path)
        judged = build.with_quality(bronze)
        valid = judged.filter(F.col("_reason").isNull())
        normalized = build.normalize(valid, built_at=datetime.now(timezone.utc))

        before = counts_by_source(normalized)
        after = counts_by_source(spark.read.format("delta").load(silver_path))

        before_total = sum(before.values())
        after_total = sum(after.values())
        dropped_total = before_total - after_total

        print(f"격리 뒤 행 : {before_total:,}")
        print(f"Silver 행  : {after_total:,}")
        print(f"지워진 행  : {dropped_total:,}  "
              f"({dropped_total / before_total * 100:.4f}%)")

        recorded = _recorded_removed()
        if recorded is not None:
            print(f"적재 때 기록: {recorded:,}  "
                  f"{'일치' if recorded == dropped_total else '어긋남'}")
            if recorded != dropped_total:
                print()
                print("Silver를 만든 뒤 Bronze가 바뀌었다. 뺄셈이 성립하지 않는다.")
                print("Silver를 다시 만든 뒤 이 측정을 돌린다.")
                return 1
        print()
        print("mbid_source별로 얼마나 지워졌나")
        print(f"  {'출처':8} {'제거 전':>14} {'제거 후':>14} "
              f"{'지워짐':>12} {'그 출처 중':>9}")

        dropped = {}
        for name in SOURCES:
            b, a = before.get(name, 0), after.get(name, 0)
            gone = b - a
            dropped[name] = gone
            share = gone / b * 100 if b else 0.0
            print(f"  {name:8} {b:>14,} {a:>14,} {gone:>12,} {share:>8.2f}%")

        with_mbid = dropped_total - dropped.get("NONE", 0)
        print()
        print(f"지워진 행 중 MBID를 갖고 있던 것 : {with_mbid:,}  "
              f"({with_mbid / dropped_total * 100:.2f}%)")

        elapsed = time.time() - started
        print()
        print(f"소요 : {elapsed:.0f}초")

        config.write_measurement("dedup-loss", {
            "elapsed_seconds": round(elapsed, 1),
            "rows_before": before_total,
            "rows_after": after_total,
            "dropped_rows": dropped_total,
            "before_by_source": before,
            "after_by_source": after,
            "dropped_by_source": dropped,
            "dropped_with_mbid": with_mbid,
        })
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
