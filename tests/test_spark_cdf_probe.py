import pytest

pytest.importorskip('pyspark')
pytest.importorskip('delta')

from pyspark.sql import functions as F

from flux.validation.cdf_drill import load_probe,partial_read_rejected

pytestmark=pytest.mark.spark


def test_fixed_parquet_is_cast_to_the_existing_bronze_schema(spark,tmp_path):
    path=tmp_path/'probe.parquet'
    frame=spark.range(1).select(F.lit('2026-10-22 00:00:00').cast('timestamp_ntz').alias('_ingested_at'),
        F.lit('dump').alias('_source_dump'),F.lit('file').alias('_source_file'),F.lit(1).alias('_record_id'),
        F.lit(1).cast('long').alias('listened_at'),F.lit('u').alias('user_name'),F.lit('{}').alias('raw_json'),
        F.lit('2026-10-22').cast('date').alias('ingested_date'))
    frame.write.parquet(str(path))
    result=load_probe(spark,path)
    assert result.schema['_ingested_at'].dataType.simpleString()=='timestamp'
    assert result.schema['_record_id'].dataType.simpleString()=='bigint'
    assert result.first()['_ingested_at']==frame.first()['_ingested_at']


def test_active_writer_and_incomplete_checkpoint_are_distinct_read_guards(spark,tmp_path):
    from flux.silver.monthly_state import exclusive

    root=tmp_path/'silver'
    with exclusive(root):
        assert partial_read_rejected(spark,root)=='active_writer'
    assert partial_read_rejected(spark,root)=='incomplete_checkpoint'
