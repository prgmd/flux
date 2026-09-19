"""물리 계획을 뽑아 본다. **재기 전에 무엇을 재는지 확인하는 단계다.**

## 왜 이걸 먼저 하는가

스큐 실험을 `groupBy` 집계로 돌렸다가 파티션 수준 스큐가 1.3배밖에 안 나왔다.
키 수준은 500,143배였는데도 그랬다.

이유는 계획에 있었다. **Spark는 `groupBy`를 셔플 전에 한 번 접는다**(부분 집계,
map-side combine). 입력 파티션 안에서 `(아티스트, 날짜)`로 먼저 묶은 뒤 그
결과만 셔플하므로, BTS의 300만 행이 셔플에 도달할 때는 이미 수천 행이다.
**스큐가 셔플에 닿기 전에 사라진다.**

`explain()`을 한 번만 봤으면 알 수 있었다. `HashAggregate` 두 개 사이에
`Exchange`가 끼어 있는 모양이 그것이다.

그래서 이 스크립트를 실험보다 먼저 둔다. **계획을 읽고 나서 무엇을 잴지
정한다.**

## 무엇을 보는가

    groupBy 마트    부분 집계가 있는가 (HashAggregate - Exchange - HashAggregate)
    셔플 조인       SortMergeJoin인가 BroadcastHashJoin인가

조인이 브로드캐스트로 잡히면 셔플이 없어 스큐도 없다. 그러면 실험 대상이
아니므로 `autoBroadcastJoinThreshold`를 꺼야 한다.

    docker run ... -e FLUX_STORAGE_ROOT=file:///data flux-spark \\
        python scripts/explain_plans.py
"""

import argparse
import sys

from pyspark.sql import functions as F

from flux.common import config
from flux.common.spark import session as spark_session
from flux.gold import fact, mart_build, scd2

# 계획만 뽑으므로 데이터를 읽지 않는다. 셔플 파티션은 실험과 같게 둬야
# 계획에 찍히는 수가 실험과 맞는다.
SHUFFLE_PARTITIONS = 200


def dump(spark, name: str, frame, root) -> str:
    """계획을 찍고 파일로도 남긴다.

    `mode="formatted"`를 쓴다. 기본 출력은 한 줄이 길어 읽기 어렵고,
    formatted는 연산자마다 블록으로 나눠 준다.
    """
    plan = frame._jdf.queryExecution().explainString(
        spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString(
            "formatted"))
    target = root / "plans" / f"{name}.txt"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(plan, encoding="utf-8")

    print(f"=== {name} ===")
    print(plan)
    print()
    return plan


def summarize(name: str, plan: str) -> dict:
    """계획에서 우리가 보려던 것만 센다.

    문자열로 세는 것이 거칠지만, 여기서 알고 싶은 것은 "부분 집계가 있나"와
    "어떤 조인인가" 둘뿐이다. 구조를 파싱할 만큼의 질문이 아니다.
    """
    counts = {
        "HashAggregate": plan.count("HashAggregate"),
        "Exchange": plan.count("Exchange"),
        "SortMergeJoin": plan.count("SortMergeJoin"),
        "BroadcastHashJoin": plan.count("BroadcastHashJoin"),
        "ShuffledHashJoin": plan.count("ShuffledHashJoin"),
    }
    print(f"{name}")
    for key, value in counts.items():
        if value:
            print(f"  {key:22} {value}")
    return counts


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shuffle-partitions", type=int,
                        default=SHUFFLE_PARTITIONS)
    args = parser.parse_args()

    fact_path = config.gold(fact.TABLE_NAME)
    dim_path = config.gold(scd2.TABLE_NAME)
    root = config.local_root()

    spark = spark_session("flux-explain",
                          shuffle_partitions=args.shuffle_partitions)
    try:
        dim = spark.read.format("delta").load(dim_path)

        # 1) 지금까지 실험이 재던 것
        grouped = mart_build.plain(spark, fact_path)
        plan_grouped = dump(spark, "01-groupby-mart", grouped, root)

        # 2) 소금을 친 집계
        salted = mart_build.salted(spark, fact_path)
        plan_salted = dump(spark, "02-groupby-salted", salted, root)

        # 3) 브로드캐스트를 강제한 이름 조인 (지금 마트가 쓰는 것)
        joined_bc = mart_build.with_names(grouped, dim, broadcast=True)
        plan_bc = dump(spark, "03-join-broadcast", joined_bc, root)

        # 4) 브로드캐스트를 안 쓴 이름 조인 — 실험 후보
        joined_shuffle = mart_build.with_names(grouped, dim, broadcast=False)
        plan_shuffle = dump(spark, "04-join-shuffle", joined_shuffle, root)

        # 5) 팩트를 차원에 직접 조인 — 집계 전이라 접힌 것이 없다
        #    스큐가 실제로 나는 자리는 여기다.
        raw_join = (
            spark.read.format("delta").load(fact_path)
            .filter(F.col("artist_mbid").isNotNull())
            .join(dim.select(scd2.BUSINESS_KEY, "name"),
                  on=scd2.BUSINESS_KEY, how="left")
        )
        plan_raw = dump(spark, "05-fact-join-raw", raw_join, root)

        print("=" * 60)
        print("요약")
        print()
        summary = {
            "groupby_mart": summarize("groupBy 마트", plan_grouped),
            "groupby_salted": summarize("groupBy + 소금", plan_salted),
            "join_broadcast": summarize("이름 조인 (브로드캐스트)", plan_bc),
            "join_shuffle": summarize("이름 조인 (셔플)", plan_shuffle),
            "fact_join_raw": summarize("팩트 직접 조인", plan_raw),
        }

        print()
        print("판정")
        g = summary["groupby_mart"]
        folds = g["HashAggregate"] >= 2 and g["Exchange"] >= 1
        print(f"  groupBy에 부분 집계가 있는가 : "
              f"{'있다' if folds else '없다'}  "
              f"(HashAggregate {g['HashAggregate']}, Exchange {g['Exchange']})")
        if folds:
            print("    → 셔플 전에 접히므로 스큐가 셔플에 도달하지 않는다")

        for key, label in (("join_shuffle", "이름 조인 (셔플)"),
                           ("fact_join_raw", "팩트 직접 조인")):
            row = summary[key]
            if row["BroadcastHashJoin"]:
                verdict = "브로드캐스트 — 셔플이 없어 스큐도 없다"
            elif row["SortMergeJoin"] or row["ShuffledHashJoin"]:
                verdict = "셔플 조인 — 같은 키가 한 파티션에 모인다"
            else:
                verdict = "조인 연산자를 못 찾았다"
            print(f"  {label:22} {verdict}")

        config.write_measurement("explain-plans", {
            "shuffle_partitions": args.shuffle_partitions,
            "summary": summary,
            "groupby_folds_before_shuffle": folds,
        })
        print()
        print(f"계획 원문은 {root / 'plans'}에 있다")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
