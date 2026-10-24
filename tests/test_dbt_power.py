import ctypes
from types import SimpleNamespace

import pytest

from flux.validation import dbt_full


@pytest.mark.parametrize('enabled,expected',[(True,0x80000003),(False,0x80000000)])
def test_windows_execution_request_covers_display_and_system_and_releases(monkeypatch,enabled,expected):
    calls=[]
    def request(flags):
        calls.append(flags)
        return 0x80000000
    monkeypatch.setattr(dbt_full,'os',SimpleNamespace(name='nt'))
    monkeypatch.setattr(ctypes,'windll',SimpleNamespace(kernel32=SimpleNamespace(SetThreadExecutionState=request)),raising=False)
    assert dbt_full.awake(enabled) is True
    assert calls==[expected]


def test_other_platforms_do_not_request_windows_execution_state(monkeypatch):
    monkeypatch.setattr(dbt_full,'os',SimpleNamespace(name='posix'))
    assert dbt_full.awake(True) is None
