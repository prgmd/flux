import os
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from flux.common.execution import Journal, current, inspect_run, recorded, record_inputs
from flux.validation.dbt_pilot import save


@pytest.fixture
def logs(monkeypatch, tmp_path):
    root = tmp_path / 'executions'
    monkeypatch.setenv('FLUX_EXECUTION_ROOT', str(root))
    return root


def only_run(root):
    paths = list(root.glob('*.sqlite3'))
    assert len(paths) == 1
    return inspect_run(paths[0])


def test_versions_stages_counts_and_times_are_committed_before_completion(logs, tmp_path):
    output = tmp_path / 'output'
    output.mkdir()

    @recorded('test')
    def run(output):
        record_inputs({'silver': {'id': 'table', 'version': 7}, 'rules_sha256': 'abc'})
        save(output, {'completed': False, 'stage': 'apply', 'impact': {'cdf_rows': 6}})
        running = only_run(logs)
        assert running['status'] == 'unfinished'
        assert running['inputs']['silver']['version'] == 7
        assert running['stage'] == 'apply'
        assert running['metrics']['impact']['cdf_rows'] == 6
        save(output, {'completed': False, 'stage': 'compare', 'seconds': {'apply': .25}})
        return {'completed': True}

    assert run(output)['completed']
    result = only_run(logs)
    assert result['status'] == 'succeeded' and result['elapsed_seconds'] >= 0
    assert [e['stage'] for e in result['events'] if e['kind'] == 'stage_finished'] == ['apply', 'compare']
    assert all(e['at'].endswith('+00:00') for e in result['events'])
    assert not (output / 'execution.sqlite3').exists()


@pytest.mark.parametrize('mode', ['exception', 'failed-report', 'interrupt', 'exit'])
def test_errors_keep_their_original_behavior_and_failed_stage(logs, tmp_path, mode):
    output = tmp_path / 'output'
    output.mkdir()

    @recorded('test')
    def run(output):
        save(output, {'completed': False, 'stage': 'build'})
        if mode == 'exception':
            raise ValueError('failure probe')
        if mode == 'interrupt':
            raise KeyboardInterrupt('stop')
        if mode == 'exit':
            raise SystemExit(2)
        return {'completed': False, 'failure': 'quality check failed'}

    if mode == 'failed-report':
        assert not run(output)['completed']
    else:
        with pytest.raises({'exception': ValueError, 'interrupt': KeyboardInterrupt, 'exit': SystemExit}[mode]):
            run(output)
    result = only_run(logs)
    assert result['status'] == ('interrupted' if mode == 'interrupt' else 'failed')
    assert result['error'] and result['stage'] == 'build'
    assert result['events'][-2]['data']['status'] == result['status']


def test_retry_has_a_separate_id_and_preflight_failure_is_recorded(logs, tmp_path):
    @recorded('preflight')
    def run(output):
        raise ValueError('bad input version')

    output = tmp_path / 'never-created'
    for _ in range(2):
        with pytest.raises(ValueError):
            run(output)
    results = [inspect_run(p) for p in logs.glob('*.sqlite3')]
    assert len({r['run_id'] for r in results}) == 2
    assert all(r['status'] == 'failed' for r in results)
    assert not output.exists()


def test_nested_saves_cannot_finish_or_replace_the_outer_stage(logs, tmp_path):
    output = tmp_path / 'output'
    output.mkdir()
    child = output / 'child'
    child.mkdir()

    @recorded('outer')
    def run(output):
        save(output, {'completed': False, 'stage': 'outer-stage'})
        save(child, {'completed': True, 'stage': 'completed'})
        assert only_run(logs)['stage'] == 'outer-stage'
        return {'completed': True}

    run(output)


def test_disk_samples_survive_without_finish_and_keep_peak(tmp_path):
    journal = Journal(tmp_path, 'probe', {})
    journal.sample({'workspace_bytes': 100, 'spill_bytes': 40})
    journal.sample({'workspace_bytes': 80, 'spill_bytes': 0})
    result = inspect_run(journal.path)
    assert result['status'] == 'unfinished'
    assert result['metrics']['peak_workspace_bytes'] == 100
    assert result['metrics']['sampled_peak_spill_bytes'] == 40


def test_recording_failure_is_visible(logs, tmp_path, monkeypatch):
    journal = Journal(logs, 'test', {})
    monkeypatch.setattr(journal, '_append', lambda *a, **k: (_ for _ in ()).throw(OSError('disk full')))
    with pytest.raises(OSError):
        journal.checkpoint(tmp_path, {'stage': 'copy'})


def test_finish_write_error_does_not_hide_the_original_exception(logs, tmp_path, monkeypatch):
    @recorded('failure')
    def run(output):
        monkeypatch.setattr(current(), 'finish', lambda *a: (_ for _ in ()).throw(OSError('disk full')))
        raise ValueError('original error')

    with pytest.raises(ValueError, match='original error'):
        run(tmp_path / 'output')
    assert only_run(logs)['status'] == 'unfinished'


def test_unwritable_journal_prevents_work(logs, tmp_path):
    logs.write_text('blocked', encoding='utf-8')
    called = []

    @recorded('blocked')
    def run(output):
        called.append(True)

    with pytest.raises(FileExistsError):
        run(tmp_path / 'output')
    assert called == []


def test_disk_meter_persists_samples_from_its_background_thread(logs, tmp_path):
    from flux.validation.dbt_full import DiskMeter

    output = tmp_path / 'output'
    (output / 'spill').mkdir(parents=True)
    (output / 'spill/data.bin').write_bytes(b'x' * 32)

    @recorded('meter')
    def run(output):
        meter = DiskMeter(output)
        meter.thread.start()
        sample = threading.Thread(target=meter.sample)
        sample.start()
        sample.join(timeout=5)
        assert not sample.is_alive()
        assert only_run(logs)['metrics']['sampled_peak_spill_bytes'] == 32
        meter.stop()
        assert only_run(logs)['metrics']['sampled_peak_spill_bytes'] == 32
        return {'completed': True}

    run(output)


def test_disk_meter_reports_background_recording_errors(logs, tmp_path, monkeypatch):
    from flux.validation.dbt_full import DiskMeter

    @recorded('meter')
    def run(output):
        meter = DiskMeter(output)
        monkeypatch.setattr(current(), 'sample', lambda values: (_ for _ in ()).throw(OSError('full disk')))
        meter.thread.start()
        with pytest.raises(OSError, match='full disk'):
            meter.stop()
        return {'completed': False, 'failure': 'full disk'}

    assert not run(tmp_path)['completed']


def test_real_sql_comparison_persists_progress(logs, tmp_path):
    import duckdb
    from flux.validation.full_gold_cdf import compare_relations

    @recorded('comparison')
    def run(output):
        with duckdb.connect() as connection:
            connection.execute("create table t as select '2026-01' as month, 's' as song, 5 as plays")
            result = compare_relations(connection, 't', 't', ['month', 'song'], 'month')
            assert result['matches']
            assert only_run(logs)['last_progress']['rows_checked'] == 1
        return {'completed': True}

    run(tmp_path / 'output')


def test_forced_process_termination_preserves_last_commit(logs, tmp_path):
    output = tmp_path / 'output'
    output.mkdir()
    code = '''
import sys, time
from pathlib import Path
from flux.common.execution import recorded, record_inputs
from flux.validation.dbt_pilot import save
@recorded('killed-child')
def run(output):
    record_inputs({'silver': {'id': 'table', 'version': 12}})
    save(output, {'completed': False, 'stage': 'build', 'impact': {'cdf_rows': 6}})
    print('ready', flush=True)
    time.sleep(60)
run(Path(sys.argv[1]))
'''
    env = {**os.environ, 'PYTHONPATH': str(Path(__file__).resolve().parents[1] / 'src')}
    process = subprocess.Popen([sys.executable, '-u', '-c', code, str(output)], env=env,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert process.stdout.readline().strip() == 'ready'
        process.terminate()
        process.communicate(timeout=15)
        assert process.returncode != 0
        result = only_run(logs)
        assert result['status'] == 'unfinished' and result['error'] is None
        assert result['stage'] == 'build' and result['inputs']['silver']['version'] == 12
        assert result['metrics']['impact']['cdf_rows'] == 6
        assert not any(e['kind'] == 'run_finished' for e in result['events'])
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=15)


def test_inspection_preserves_committed_contents_and_does_not_create_missing_files(tmp_path):
    missing = tmp_path / 'missing.sqlite3'
    with pytest.raises(FileNotFoundError):
        inspect_run(missing)
    assert not missing.exists()
    journal = Journal(tmp_path, 'probe', {})
    before = journal.path.read_bytes()
    inspect_run(journal.path)
    assert before == journal.path.read_bytes()


def test_inspect_cli_prints_summary_and_keeps_events_optional(tmp_path):
    import json

    journal = Journal(tmp_path, 'cli', {})
    journal.finish('succeeded')
    root = Path(__file__).resolve().parents[1]
    env = {**os.environ, 'PYTHONPATH': str(root / 'src')}
    result = subprocess.run([sys.executable, str(root / 'scripts/inspect_execution.py'), str(journal.path)],
                            env=env, check=True, capture_output=True, text=True, encoding='utf-8')
    summary = json.loads(result.stdout)
    assert summary['status'] == 'succeeded' and 'events' not in summary


def test_termination_during_sqlite_write_keeps_the_last_committed_event(tmp_path):
    code = '''
import sqlite3, sys, time
from flux.common.execution import Journal
journal = Journal(sys.argv[1], 'mid-write', {})
journal.progress({'rows_checked': 42})
connection = sqlite3.connect(journal.path)
connection.execute('pragma cache_size=1')
connection.execute('begin immediate')
connection.execute("insert into events(kind,at,elapsed_seconds,data) values('partial','now',1,?)", ['x' * 2000000])
print('ready', flush=True)
time.sleep(60)
'''
    env = {**os.environ, 'PYTHONPATH': str(Path(__file__).resolve().parents[1] / 'src')}
    process = subprocess.Popen([sys.executable, '-u', '-c', code, str(tmp_path)], env=env,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert process.stdout.readline().strip() == 'ready'
        process.terminate()
        process.communicate(timeout=15)
        result = only_run(tmp_path)
        assert result['status'] == 'unfinished'
        assert result['last_progress'] == {'rows_checked': 42}
        assert len(result['events']) == 2
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=15)


def test_existing_cdf_and_dbt_entrypoints_record_preflight_failures(logs, tmp_path):
    from flux.validation.dbt_full import run_full
    from flux.validation.full_gold_cdf import resume_full_cdf, run_full_cdf
    from flux.validation.gold_cdf import run

    for function, args in [(run_full, [tmp_path/'missing', tmp_path/'out1', tmp_path/'project']),
                           (run_full_cdf, [tmp_path/'missing', tmp_path/'out2', tmp_path/'project']),
                           (resume_full_cdf, [tmp_path/'missing', tmp_path/'out3', tmp_path/'project']),
                           (run, [tmp_path/'missing', tmp_path/'out4', tmp_path/'project'])]:
        with pytest.raises((ValueError, FileNotFoundError)):
            function(*args)
    assert len(list(logs.glob('*.sqlite3'))) == 4
    assert all(inspect_run(p)['status'] == 'failed' for p in logs.glob('*.sqlite3'))
