import importlib.util
import io
import tarfile
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location('snapshot',Path(__file__).resolve().parents[1]/'scripts/investigations/extract_credit_snapshot.py')
snapshot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(snapshot)


def test_extract_whitelist_only_and_preserve_bytes(tmp_path):
    archive=tmp_path/'input.tar.bz2'
    with tarfile.open(archive,'w:bz2') as t:
        for name in ['artist','artist_credit','artist_credit_name','recording','../escape']:
            value=b'1\tvalue\n'
            info=tarfile.TarInfo('mbdump/'+name); info.size=len(value)
            t.addfile(info,io.BytesIO(value))
    result=snapshot.extract(archive,tmp_path/'out')
    assert result['completed']
    assert (tmp_path/'out'/'recording').read_bytes()==b'1\tvalue\n'
    assert not (tmp_path/'escape').exists()
    with pytest.raises(FileExistsError): snapshot.extract(archive,tmp_path/'out')


def test_missing_table_does_not_report_complete(tmp_path):
    archive=tmp_path/'input.tar.bz2'
    with tarfile.open(archive,'w:bz2'): pass
    with pytest.raises(ValueError,match='missing'): snapshot.extract(archive,tmp_path/'out')
    assert not (tmp_path/'out'/'result.json').exists()


def test_redirect_only_and_reject_unknown_table(tmp_path):
    archive=tmp_path/'input.tar.bz2'
    with tarfile.open(archive,'w:bz2') as t:
        value=b'old\t42\t2026-01-01\n'
        info=tarfile.TarInfo('mbdump/recording_gid_redirect'); info.size=len(value)
        t.addfile(info,io.BytesIO(value))
    result=snapshot.extract(archive,tmp_path/'out',{'recording_gid_redirect'})
    assert set(result['tables'])=={'recording_gid_redirect'}
    with pytest.raises(ValueError,match='unsupported'):
        snapshot.extract(archive,tmp_path/'unsafe',{'../escape'})
    assert not (tmp_path/'unsafe').exists()
