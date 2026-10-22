import io
import json
import tarfile

import pytest

from flux.validation.cdf_inputs import prepare,sample_archive
from flux.validation.cdf_drill import verify_inputs
from flux.validation.pinning import sha256


def test_archive_sampling_is_bounded_and_preserves_source(tmp_path):
    from compression import zstd

    payload=b'{"user_name":"one"}\n{"user_name":"two"}\n{"user_name":"three"}\n'
    archive=tmp_path/'dump.tar.zst'
    with zstd.ZstdFile(archive,'wb') as stream,tarfile.open(fileobj=stream,mode='w|') as target:
        info=tarfile.TarInfo('dump/2026-08.listens');info.size=len(payload)
        target.addfile(info,io.BytesIO(payload))
    before=sha256(archive)
    assert sample_archive(archive,tmp_path/'sample.jsonl',2)==2
    assert (tmp_path/'sample.jsonl').read_bytes()==payload.splitlines(keepends=True)[0]+payload.splitlines(keepends=True)[1]
    assert sha256(archive)==before


def test_overlapping_output_and_unbounded_samples_are_rejected_before_writes(tmp_path):
    source=tmp_path/'source';source.mkdir()
    with pytest.raises(ValueError):prepare(source,source/'output')
    with pytest.raises(ValueError):prepare(source,tmp_path/'output',limit=0)
    assert not (tmp_path/'output').exists()


def test_drill_requires_verified_local_artifacts(tmp_path):
    (tmp_path/'input.txt').write_text('original')
    info={'prepared':True,'production_applied':False,'artifacts':{'input.txt':sha256(tmp_path/'input.txt')}}
    (tmp_path/'inputs.json').write_text(json.dumps(info))
    assert verify_inputs(tmp_path)==info
    (tmp_path/'input.txt').write_text('changed')
    with pytest.raises(ValueError,match='artifact'):verify_inputs(tmp_path)
    info['artifacts']={'../outside':None};(tmp_path/'inputs.json').write_text(json.dumps(info))
    with pytest.raises(ValueError):verify_inputs(tmp_path)
