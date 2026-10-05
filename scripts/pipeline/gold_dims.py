"""날짜 차원과 사용자 차원을 만들고 확인한다.

    날짜 차원    팩트의 첫 청취 날부터 마지막 청취 날까지 하루 한 줄. 줄 수가 일수와 같아야 한다
    사용자 차원  Silver의 사용자 번호마다 한 줄. 팩트의 모든 번호가 있어야 한다

둘 다 작아서 매번 통째로 다시 만든다.

    ./scripts/spark_run.sh -m 10g -- python scripts/pipeline/gold_dims.py
"""

import sys
import time
from datetime import datetime, timezone

from flux.common import config
from flux.common.spark import session as spark_session
from flux.gold import dates, dim_date, dim_user, fact
from flux.silver import schema as silver_schema


def main() -> int:
    silver_path = config.silver(silver_schema.TABLE_NAME)
    fact_path = config.gold(fact.TABLE_NAME)
    date_path = config.gold(dates.DATE_TABLE)
    user_path = config.gold(dates.USER_TABLE)
    built_at = datetime.now(timezone.utc)

    spark = spark_session("flux-gold-dims", shuffle_partitions=200)
    started = time.time()
    try:
        listens = spark.read.format("delta").load(fact_path)
        first, last = dim_date.listened_range(listens)
        dim_date.write(dim_date.build(spark, first, last), date_path)
        date_rows = spark.read.format("delta").load(date_path).count()
        expected_days = len(dates.span(first, last))

        silver = spark.read.format("delta").load(silver_path)
        dim_user.write(dim_user.build(silver, built_at=built_at), user_path)
        user_rows = spark.read.format("delta").load(user_path).count()
        missing = dim_user.users_missing(spark, fact_path, user_path)
        elapsed = time.time() - started

        print(f"날짜 차원  : {first} ~ {last}, {date_rows:,}행 (기간 {expected_days:,}일)")
        print(f"사용자 차원: {user_rows:,}행, 팩트에만 있는 번호 {missing:,}")
        print(f"소요       : {elapsed:.0f}초")

        checks = {"date_rows_off_by": date_rows - expected_days, "users_missing": missing}
        config.write_measurement("gold-dims", {
            "built_at": built_at.isoformat(),
            "elapsed_seconds": round(elapsed, 1),
            "date_range": [str(first), str(last)],
            "date_rows": date_rows,
            "user_rows": user_rows,
            "checks": checks,
        })
        if any(checks.values()):
            print("확인 실패")
            return 1
        print("확인 통과")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
