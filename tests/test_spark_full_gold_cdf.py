import json

import pytest

pytest.importorskip('pyspark')
pytest.importorskip('delta')

from flux.silver import monthly_listens as monthly
from flux.validation.full_gold_cdf import produce_probe

pytestmark=pytest.mark.spark


def test_full_probe_uses_actual_cdf_without_collecting_full_snapshots(spark,tmp_path):
    root=tmp_path/'silver';state=monthly.state_for('controlled-bronze',0)
    values=[('a','s1','2010-01',2),('b','s2','2010-01',1),('c','s3','2010-01',3)]
    frame=spark.createDataFrame(values,'user_name string,recording_msid string,listened_month string,play_count long')
    monthly.create(frame,root/monthly.TABLE,state,partition='listened_month')
    monthly.create(spark.createDataFrame([('s1',)],'recording_msid string'),root/monthly.METADATA,state)
    monthly.create(spark.createDataFrame([],'reason string'),root/monthly.QUARANTINE,state)
    actions=[dict(user_name=u,recording_msid=s,listened_month=m,play_count=n,action=a,new_month=next_month,new_count=count)
             for (u,s,m,n),a,next_month,count in zip(values,['update','delete','update'],['2010-01','2010-01','1990-02'],[4,1,3])]
    actions.append(dict(actions[0],user_name='new',action='insert',new_count=1))
    changes=tmp_path/'changes.json';changes.write_text(json.dumps(actions),encoding='utf-8')
    result=produce_probe(spark,root,tmp_path/'export',changes)
    assert result['completed'] and result['cdf_rows']==6
    assert result['counts']=={'before':{'rows':3,'plays':6},'after':{'rows':3,'plays':8}}
    assert result['cdf_actions']=={'insert':1,'delete':1,'update_preimage':2,'update_postimage':2}
    assert result['before_versions'][monthly.TABLE]<result['after_versions'][monthly.TABLE]
    rows=spark.read.parquet(str(tmp_path/'export/cdf')).count()
    assert rows==6
