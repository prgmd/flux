"""스테이징의 MusicBrainz 표를 읽어 silver.mb_artist를 만든다.

다섯 표를 엮는다. artist의 type, area, gender가 숫자 외래 키라
조회 표를 붙여야 값이 된다.

    artist  ×  artist_type   →  type 이름
            ×  gender        →  gender 이름
            ×  area          →  지역 이름
            ×  iso_3166_1    →  국가 코드

**area는 국가가 아니다.** 나라, 주, 도시를 모두 담는 표라
iso_3166_1에 직접 걸리는 것만 country로 인정한다.

    docker run ... -e FLUX_STORAGE_ROOT=file:///data flux-spark \
        python scripts/silver_musicbrainz.py
"""

import argparse
import sys
import time

from pyspark.sql import functions as F
from pyspark.sql import types as T

from flux.common import config
from flux.common.spark import session as spark_session
from flux.silver import musicbrainz as mb

STAGING = "staging/musicbrainz"
TABLE_NAME = "mb_artist"


def read_table(spark, directory, table):
    """헤더 없는 탭 구분 파일을 선언된 컬럼 이름으로 읽는다.

    스키마를 명시해야 한다. 추론에 맡기면 컬럼 개수가 맞는지 확인할 수 없고,
    빈 파일에서 컬럼이 사라진다.
    """
    names = mb.columns(table)
    schema = T.StructType([T.StructField(n, T.StringType(), True) for n in names])
    return (
        spark.read
        .option("sep", "\t")
        .option("header", "false")
        .option("quote", "")          # 따옴표를 특별 취급하지 않는다. COPY 형식이다
        .option("escape", "")
        .schema(schema)
        .csv(f"{directory}/{table}")
    )


def check_column_count(spark, directory, table) -> int:
    """선언한 컬럼 개수와 다른 줄이 몇 개인지 센다.

    스키마를 명시해 읽으면 남는 컬럼은 조용히 버려지고 모자란 컬럼은 NULL이 된다.
    헤더가 없는 파일이라 컬럼이 한 칸 밀려도 그대로 적재되고, 이름 자리에
    다른 값이 들어간 채 아무 데서도 안 걸린다.

    원문을 줄 단위로 읽어 탭 개수를 세는 것이 유일하게 확실한 확인이다.
    표만큼 한 번 더 훑지만 다섯 표를 합쳐 430MB라 몇 초면 끝난다.
    """
    expected = len(mb.columns(table))
    lines = spark.read.text(f"{directory}/{table}")
    return lines.filter(
        F.size(F.split(F.col("value"), "\t", -1)) != expected).count()


def unnull(column):
    r"""덤프의 NULL 표기를 NULL로. flux.silver.musicbrainz.unnull의 Spark 판이다.

    표기 자체는 그 모듈에서 가져온다. 여기에 `"\N"`을 다시 적으면 원천이
    표기를 바꿀 때 한쪽만 고치게 되고, 그러면 `\N`이 값으로 적재된다.
    타입 검사에도 안 걸리고 집계에 문자열 하나로 섞여 든다.
    """
    return F.when(column != F.lit(mb.NULL), column)


def partial_date(prefix: str):
    """있는 만큼만의 날짜 문자열. flux.silver.musicbrainz.partial_date와 같은 규칙.

    연도만 아는 경우가 흔한데 1월 1일로 채우면 없던 정밀도를 지어내는 것이다.
    """
    year = unnull(F.col(f"{prefix}_year"))
    month = unnull(F.col(f"{prefix}_month"))
    day = unnull(F.col(f"{prefix}_day"))
    yyyy = F.lpad(year, 4, "0")
    mm = F.lpad(month, 2, "0")
    dd = F.lpad(day, 2, "0")
    return (
        F.when(year.isNull(), F.lit(None))
         .when(month.isNull(), yyyy)
         .when(day.isNull(), F.concat_ws("-", yyyy, mm))
         .otherwise(F.concat_ws("-", yyyy, mm, dd))
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release", default=None)
    parser.add_argument("--shuffle-partitions", type=int, default=64)
    args = parser.parse_args()

    root = config.local_root()
    staging_root = root / STAGING
    releases = sorted(p.name for p in staging_root.glob("*") if p.is_dir())
    if args.release:
        releases = [r for r in releases if r == args.release]
    if not releases:
        raise SystemExit(f"스테이징이 비어 있다: {staging_root}")
    release = releases[-1]
    directory = f"{config.storage_root()}/{STAGING}/{release}"

    print(f"릴리스 : {release}")
    print(f"입력   : {directory}")
    print()

    spark = spark_session("flux-silver-musicbrainz", shuffle_partitions=args.shuffle_partitions, memory="6g")
    started = time.time()
    try:
        artist = read_table(spark, directory, "artist")
        artist_type = read_table(spark, directory, "artist_type")
        gender = read_table(spark, directory, "gender")
        area = read_table(spark, directory, "area")
        iso = read_table(spark, directory, "iso_3166_1")

        counts = {
            "artist": artist.count(),
            "artist_type": artist_type.count(),
            "gender": gender.count(),
            "area": area.count(),
            "iso_3166_1": iso.count(),
        }
        for name, n in counts.items():
            print(f"  {name:14} {n:>10,}행")

        print()
        print("컬럼 개수 검사")
        broken = {}
        for name in mb.REQUIRED_TABLES:
            bad = check_column_count(spark, directory, name)
            broken[name] = bad
            mark = "통과" if bad == 0 else f"{bad:,}줄 어긋남"
            print(f"  {name:14} {mark}")
        if any(broken.values()):
            print()
            print("컬럼 개수가 선언과 다른 줄이 있다. 스키마 정의를 확인해야 한다")
            return 1

        # 조회 표는 작으므로 브로드캐스트한다. 297만 행을 셔플하지 않는다.
        # 결과 컬럼에 밑줄 접두어를 붙인다. artist에 gender, type, area가
        # 숫자 FK로 이미 있어서, 같은 이름을 쓰면 조인 뒤 참조가 모호해진다.
        types = F.broadcast(
            artist_type.select(F.col("id").alias("_type_id"),
                               F.col("name").alias("_type_name")))
        genders = F.broadcast(
            gender.select(F.col("id").alias("_gender_id"),
                          F.col("name").alias("_gender_name")))
        areas = F.broadcast(
            area.select(F.col("id").alias("_area_id"),
                        F.col("name").alias("_area_name")))
        countries = F.broadcast(
            iso.select(F.col("area").alias("_iso_area_id"),
                       F.col("code").alias("_country_code")))

        resolved = (
            artist
            .withColumn("_type_key", unnull(F.col("type")))
            .withColumn("_gender_key", unnull(F.col("gender")))
            .withColumn("_area_key", unnull(F.col("area")))
            .join(types, F.col("_type_key") == F.col("_type_id"), "left")
            .join(genders, F.col("_gender_key") == F.col("_gender_id"), "left")
            .join(areas, F.col("_area_key") == F.col("_area_id"), "left")
            .join(countries, F.col("_area_key") == F.col("_iso_area_id"), "left")
            .select(
                F.col("gid").alias("artist_mbid"),
                F.col("name"),
                F.col("sort_name"),
                F.col("_country_code").alias("country"),
                F.col("_area_name").alias("area_name"),
                F.col("_type_name").alias("type"),
                F.col("_gender_name").alias("gender"),
                partial_date("begin_date").alias("begin_date"),
                partial_date("end_date").alias("end_date"),
                (F.col("ended") == F.lit("t")).alias("ended"),
                unnull(F.col("comment")).alias("comment"),
                unnull(F.col("last_updated")).alias("mb_last_updated"),
                F.col("id").cast("long").alias("mb_artist_id"),
            )
            .withColumn("_loaded_at", F.current_timestamp())
            .withColumn("_release", F.lit(release))
        )

        path = config.silver(TABLE_NAME)
        (
            resolved.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(path)
        )
        elapsed = time.time() - started

        written = spark.read.format("delta").load(path)
        rows = written.count()
        filled = written.agg(
            F.count(F.when(F.col("country").isNotNull(), 1)).alias("country"),
            F.count(F.when(F.col("area_name").isNotNull(), 1)).alias("area_name"),
            F.count(F.when(F.col("type").isNotNull(), 1)).alias("type"),
            F.count(F.when(F.col("gender").isNotNull(), 1)).alias("gender"),
            F.count(F.when(F.col("begin_date").isNotNull(), 1)).alias("begin_date"),
            F.count(F.when(F.col("end_date").isNotNull(), 1)).alias("end_date"),
            F.count(F.when(F.col("ended"), 1)).alias("ended_true"),
        ).collect()[0]

        print()
        print(f"적재 시간 : {elapsed:.0f}초")
        print(f"mb_artist : {rows:,}행")
        print()
        print("채워진 비율")
        for name in ("country", "area_name", "type", "gender",
                     "begin_date", "end_date", "ended_true"):
            n = filled[name]
            print(f"  {name:12} {n:>10,}  {n / rows * 100:6.2f}%")

        # area는 있는데 국가가 아닌 경우. 도시나 주에 소속된 아티스트다.
        area_not_country = written.filter(
            F.col("area_name").isNotNull() & F.col("country").isNull()).count()
        print()
        print(f"지역은 있으나 국가가 아님 : {area_not_country:,}  "
              f"({area_not_country / rows * 100:.2f}%)")

        if rows != counts["artist"]:
            print()
            print(f"대조 실패: 원본 {counts['artist']:,} != 적재 {rows:,}")
            return 1

        payload = {
            "release": release,
            "elapsed_seconds": round(elapsed, 1),
            "source_counts": counts,
            "column_count_mismatches": broken,
            "rows": rows,
            "filled": {name: filled[name] for name in
                       ("country", "area_name", "type", "gender",
                        "begin_date", "end_date", "ended_true")},
            "area_not_country": area_not_country,
        }
        config.write_measurement("mb-artist-load", payload)

        print()
        print("대조 통과")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
