import inspect
import json
import os
import sqlite3
import sys
import threading
import time
import traceback
import uuid
from contextlib import closing
from contextvars import ContextVar
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path


_current = ContextVar('flux_execution', default=None)
PIN_FIELDS = ('context', 'source_versions', 'source_id', 'source_cursor', 'project_sha256',
              'rules_sha256', 'input_manifest_sha256', 'source_manifest_sha256', 'probe_manifest_sha256')
METRIC_FIELDS = ('inputs', 'impact', 'checks', 'comparisons', 'starting_input', 'ending_input', 'final_comparison',
                 'build_nodes', 'rebuild_nodes',
                 'base_rows', 'base_plays', 'cdf_rows', 'cdf_actions', 'seconds', 'storage',
                 'peak_workspace_bytes', 'sampled_peak_spill_bytes')


class Journal:
    def __init__(self, directory, job, parameters, output=None):
        directory = Path(directory).resolve()
        directory.mkdir(parents=True, exist_ok=True)
        self.run_id = str(uuid.uuid4())
        self.path = directory / (self.run_id + '.sqlite3')
        self.output = None if output is None else Path(output).resolve()
        self.lock = threading.RLock()
        self.started = time.perf_counter()
        self.stage = None
        self.stage_started = None
        self.finished = False
        self.peak = self.spill_peak = 0
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute('pragma journal_mode=DELETE')
            connection.execute('pragma synchronous=FULL')
            connection.execute('create table events(seq integer primary key, kind text not null, '
                               'stage text, at text not null, elapsed_seconds real not null, '
                               'stage_seconds real, data text not null)')
            connection.commit()
        self._append('run_started', {'run_id': self.run_id, 'job': job, 'parameters': parameters,
                                    'pid': os.getpid(), 'schema_version': 1})

    def _append(self, kind, data):
        payload = json.dumps(data, ensure_ascii=False, allow_nan=False)
        with self.lock:
            if self.finished:
                raise ValueError('execution already finished')
            now = time.perf_counter()
            with closing(sqlite3.connect(self.path, timeout=10)) as connection:
                connection.execute('pragma synchronous=FULL')
                with connection:
                    connection.execute('insert into events(kind,stage,at,elapsed_seconds,stage_seconds,data) '
                                       'values(?,?,?,?,?,?)', (kind, self.stage,
                                       datetime.now(timezone.utc).isoformat(), now - self.started,
                                       None if self.stage_started is None else now - self.stage_started, payload))

    def inputs(self, values):
        self._append('inputs', values)

    def progress(self, values):
        self._append('progress', values)

    def _end_stage(self, status, error=None):
        if self.stage is not None:
            self._append('stage_finished', {'status': status, 'error': error})
            self.stage = self.stage_started = None

    def checkpoint(self, root, report):
        if self.output is not None and Path(root).resolve() != self.output:
            return
        with self.lock:
            stage = report.get('stage')
            if stage and stage != 'completed' and stage != self.stage:
                self._end_stage('succeeded')
                self.stage, self.stage_started = stage, time.perf_counter()
                self._append('stage_started', {})
            pins = {key: report[key] for key in PIN_FIELDS if key in report}
            metrics = {key: report[key] for key in METRIC_FIELDS if key in report}
            self._append('checkpoint', {'inputs': pins, 'metrics': metrics})

    def sample(self, values):
        with self.lock:
            if self.finished:
                return
            self.peak = max(self.peak, values['workspace_bytes'])
            self.spill_peak = max(self.spill_peak, values['spill_bytes'])
            self._append('sample', {**values, 'peak_workspace_bytes': self.peak,
                                    'sampled_peak_spill_bytes': self.spill_peak})

    def finish(self, status, error=None, detail=None):
        if status not in {'succeeded', 'failed', 'interrupted'}:
            raise ValueError('invalid terminal execution status')
        with self.lock:
            self._end_stage(status, error)
            self._append('run_finished', {'status': status, 'error': error, 'detail': detail})
            self.finished = True


def current():
    return _current.get()


def record_inputs(values):
    journal = current()
    if journal is not None:
        journal.inputs(values)


def progress(values):
    journal = current()
    if journal is not None:
        journal.progress(values)


def checkpoint(root, report):
    journal = current()
    if journal is not None:
        journal.checkpoint(root, report)


def recorded(job):
    def decorate(function):
        signature = inspect.signature(function)

        @wraps(function)
        def wrapped(*args, **kwargs):
            if current() is not None:
                return function(*args, **kwargs)
            arguments = signature.bind(*args, **kwargs)
            arguments.apply_defaults()
            parameters = {key: str(value.resolve()) if isinstance(value, Path) else value
                          for key, value in arguments.arguments.items()}
            output = arguments.arguments.get('output', arguments.arguments.get('root'))
            journal = Journal(os.environ.get('FLUX_EXECUTION_ROOT', 'data/executions'), job, parameters, output=output)
            token = _current.set(journal)
            try:
                result = function(*args, **kwargs)
            except BaseException as error:
                try:
                    journal.finish('interrupted' if isinstance(error, KeyboardInterrupt) else 'failed',
                                   type(error).__name__ + ': ' + str(error)[:4000],
                                   ''.join(traceback.format_exception(error))[-16000:])
                except Exception as recording_error:
                    print('execution recording failed: ' + type(recording_error).__name__, file=sys.stderr)
                raise
            else:
                if isinstance(result, dict):
                    journal.checkpoint(output, result)
                    succeeded = result.get('completed') is True
                    journal.finish('succeeded' if succeeded else 'failed',
                                   None if succeeded else str(result.get('failure', 'incomplete result'))[:4000])
                else:
                    journal.finish('succeeded' if result == 0 else 'failed')
                return result
            finally:
                _current.reset(token)

        return wrapped
    return decorate


def inspect_run(path):
    path = Path(path).resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    with closing(sqlite3.connect(path.as_uri() + '?mode=rw', uri=True)) as connection:
        events = [dict(seq=row[0], kind=row[1], stage=row[2], at=row[3], elapsed_seconds=row[4],
                       stage_seconds=row[5], data=json.loads(row[6]))
                  for row in connection.execute('select * from events order by seq')]
    if not events or events[0]['kind'] != 'run_started':
        raise ValueError('execution journal has no start record')
    result = dict(events[0]['data'], status='unfinished', error=None, inputs={}, metrics={}, stage=None,
                  started_at=events[0]['at'], last_recorded_at=events[-1]['at'],
                  elapsed_seconds=events[-1]['elapsed_seconds'], events=events)
    for event in events:
        if event['stage'] is not None:
            result['stage'] = event['stage']
        if event['kind'] == 'inputs':
            result['inputs'].update(event['data'])
        elif event['kind'] == 'checkpoint':
            result['inputs'].update(event['data']['inputs'])
            result['metrics'].update(event['data']['metrics'])
        elif event['kind'] == 'sample':
            result['metrics'].update(event['data'])
        elif event['kind'] == 'progress':
            result['last_progress'] = event['data']
        elif event['kind'] == 'run_finished':
            result.update(event['data'], finished_at=event['at'])
    return result
