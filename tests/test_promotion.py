"""Independent behavioral checks for durable schedule promotion.

Faults are injected at SQLite's boundary and durable state is observed with
separate connections. No reference implementation is imported or inspected.
"""
import contextlib
import datetime as dt
import importlib
import logging
import multiprocessing as mp
import os
from pathlib import Path
import random
import signal
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

# Huey's naive scheduling datetimes follow the local timezone before UTC
# normalization. Use the same explicit timezone in all spawned test processes.
os.environ['TZ'] = 'UTC'
time.tzset()
sys.dont_write_bytecode = True

SOURCE = Path(os.environ.get('TASK_SOURCE', '/app/huey')).resolve()
sys.path.insert(0, str(SOURCE))
from huey import FileHuey, MemoryHuey, SqliteHuey
from huey.consumer import Scheduler
from huey.serializer import Serializer
from huey.signals import SIGNAL_ENQUEUED

NOW = dt.datetime(2025, 3, 8, 12, 30, 15)
CTX = mp.get_context('spawn')


class CaseTimeout(BaseException):
    """Cannot be swallowed by application-level exception isolation."""


class AbortScope(BaseException):
    """An application cancellation, distinct from the test deadline."""


def echo(value=None, **kwargs):
    return value, kwargs

# Stable registry module name across spawn and unittest execution.
echo.__module__ = 'promotion_fixture'


def register(huey):
    return huey.task(name='echo')(echo)


def make_huey(path, name='main', **kwargs):
    huey = SqliteHuey(name, filename=str(path), strict_fifo=True, timeout=3,
                     **kwargs)
    return huey, register(huey)


def snapshot(path):
    with sqlite3.connect(str(path), timeout=3) as conn:
        return {table: conn.execute('SELECT * FROM ' + table + ' ORDER BY 1,2').fetchall()
                for table in ('schedule', 'task', 'kv', 'counter')}


def rows(path, table, queue='main'):
    order = 'timestamp,id' if table == 'schedule' else 'priority DESC,id'
    with sqlite3.connect(str(path), timeout=3) as conn:
        return conn.execute('SELECT data FROM %s WHERE queue=? ORDER BY %s' %
                            (table, order), (queue,)).fetchall()


def task_ids(huey, path, table='task', queue='main'):
    return [huey.deserialize_task(data).id for data, in rows(path, table, queue)]


def add_batch(huey, task, count, prefix='job', seed=7301):
    rng = random.Random(seed)
    selected = []
    for i in range(count):
        eta = NOW - dt.timedelta(seconds=rng.randrange(7))
        priority = rng.choice([None, -9, -0.5, 0, 0.25, 6, 101])
        item = task.s({'index': i, 'nonce': rng.randrange(2**40)},
                      id='%s-%d' % (prefix, i), eta=eta, priority=priority)
        huey.add_schedule(item)
        selected.append((eta, i, item))
    return [item for _, _, item in sorted(selected, key=lambda x: x[:2])]


def new_database(path):
    # The unmodified release's on-disk schema, independently provisioned.
    with sqlite3.connect(str(path)) as conn:
        conn.executescript('''
          PRAGMA journal_mode=WAL;
          CREATE TABLE kv (queue TEXT NOT NULL,key TEXT NOT NULL,value BLOB NOT NULL,PRIMARY KEY(queue,key));
          CREATE TABLE schedule (id INTEGER NOT NULL PRIMARY KEY,queue TEXT NOT NULL,data BLOB NOT NULL,timestamp REAL NOT NULL);
          CREATE INDEX schedule_queue_timestamp ON schedule(queue,timestamp);
          CREATE TABLE task (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,queue TEXT NOT NULL,data BLOB NOT NULL,priority REAL NOT NULL DEFAULT 0.0);
          CREATE INDEX task_queue_priority_id ON task(queue,priority DESC,id);
          CREATE TABLE counter(queue TEXT NOT NULL,key TEXT NOT NULL,value INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(queue,key));
        ''')


@contextlib.contextmanager
def configure_connections(huey, configure):
    """Apply a real SQLite fault to current and newly opened connections."""
    original = sqlite3.connect
    connections = [huey.storage.conn]
    configure(connections[0])

    def connect(*args, **kwargs):
        conn = original(*args, **kwargs)
        configure(conn)
        connections.append(conn)
        return conn

    try:
        with mock.patch.object(sqlite3, 'connect', connect):
            yield
    finally:
        for conn in connections:
            try:
                conn.set_authorizer(None)
            except sqlite3.Error:
                pass


def promote_worker(path, start, output):
    try:
        h, _ = make_huey(path)
        start.wait(5)
        count = h.promote_schedule(NOW)
        output.put(('ok', count))
        h.storage.close()
    except BaseException as exc:
        output.put(('error', type(exc).__name__ + ': ' + str(exc)))


def crash_worker(path, reached):
    h, _ = make_huey(path)

    def stop_before_commit():
        reached.set()
        # Parent terminates this process while a real SQLite statement runs.
        time.sleep(30)
        return 0

    with configure_connections(h, lambda c: c.create_function('promotion_gate', 0, stop_before_commit)):
        h.promote_schedule(NOW)


class FramedSerializer(Serializer):
    def __init__(self):
        super().__init__(compression=True)
        self.fail_encoding = False

    def serialize(self, message):
        if self.fail_encoding and message.id.endswith('-3'):
            raise ValueError('serializer rejected selected task')
        return b'framed-v1\0' + super().serialize(message)

    def deserialize(self, data):
        if not data.startswith(b'framed-v1\0'):
            raise ValueError('missing serializer frame')
        return super().deserialize(data[len(b'framed-v1\0'):])


class ForkCheckedConnection(sqlite3.Connection):
    inherited_closes = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.creator_pid = os.getpid()

    def close(self):
        if self.creator_pid != os.getpid():
            self.inherited_closes.append((self.creator_pid, os.getpid()))
            raise RuntimeError('native close on a parent-owned SQLite connection')
        return super().close()


def fork_promote_worker(hueys, gate, output, close_first=False, extra=None):
    signal.alarm(9)
    try:
        if not gate.wait(4):
            raise RuntimeError('parent did not release the database')
        seen = []
        for h in hueys:
            h.signal(SIGNAL_ENQUEUED)(
                lambda signal_name, item, name=h.name: seen.append((name, item.id)))
            if close_first:
                h.storage.close()
        if extra is not None:
            h = hueys[0]
            task, task_id = extra
            h.add_schedule(task.s('child payload', id=task_id, eta=NOW))
        session_tables = []
        for h in hueys:
            with h.storage.db(commit=False) as cursor:
                cursor.execute("SELECT name FROM sqlite_temp_master WHERE type='table'")
                session_tables.append([row[0] for row in cursor.fetchall()])
        with hueys[0].storage.db(commit=True):
            counts = [h.promote_schedule(NOW) for h in hueys]
        output.send({'counts': counts, 'seen': seen,
                     'ready': [h.storage.queue_size() for h in hueys],
                     'scheduled': [h.storage.schedule_size() for h in hueys],
                     'session_tables': session_tables,
                     'inherited_closes': list(ForkCheckedConnection.inherited_closes)})
        for h in hueys:
            h.storage.close()
    except BaseException as exc:
        output.send({'error': type(exc).__name__ + ': ' + str(exc)})
    finally:
        output.close()


def fork_generation_worker(h, task, gate, output):
    signal.alarm(12)
    child = None
    try:
        if not gate.wait(3):
            raise RuntimeError('first generation was not released')
        seen = []
        h.signal(SIGNAL_ENQUEUED)(lambda signal_name, item: seen.append(item.id))
        ctx = mp.get_context('fork')
        child_gate = ctx.Event()
        reader, writer = ctx.Pipe(duplex=False)
        with h.storage.db(commit=False):
            committed = h.promote_schedule(NOW)
            child = ctx.Process(target=fork_promote_worker,
                                args=([h], child_gate, writer, True,
                                      (task, 'grandchild-job')))
            child.start()
        child_gate.set()
        if not reader.poll(6):
            raise RuntimeError('descendant did not finish')
        result = reader.recv()
        child.join(2)
        if child.is_alive() or child.exitcode != 0:
            raise RuntimeError('descendant exited abnormally')
        output.send({'committed': committed, 'own_signals': seen,
                     'descendant': result})
        reader.close()
        writer.close()
        h.storage.close()
    except BaseException as exc:
        output.send({'error': type(exc).__name__ + ': ' + str(exc)})
    finally:
        if child is not None and child.is_alive():
            child.kill()
            child.join(2)
        output.close()


class PromotionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='promotion-case-')
        self.path = Path(self.temp.name) / 'existing broker.sqlite'
        new_database(self.path)
        self.h, self.task = make_huey(self.path)
        self.connections = [self.h]
        self.children = []
        self.previous_alarm = signal.signal(signal.SIGALRM, self.timeout)
        signal.alarm(18)

    def timeout(self, *_):
        raise CaseTimeout('behavioral case deadline exceeded (possible storage deadlock)')

    def tearDown(self):
        signal.alarm(0)
        signal.signal(signal.SIGALRM, self.previous_alarm)
        for child in self.children:
            if child.is_alive():
                child.kill()
            child.join(2)
        for h in self.connections:
            # A failed deadlock check can leave the connection locked; do not
            # reacquire its Python lock during cleanup.
            conn = getattr(h.storage, '_conn', None)
            if conn is not None:
                with contextlib.suppress(Exception):
                    conn.close()
        self.temp.cleanup()

    def events(self):
        events = []
        self.h.signal(SIGNAL_ENQUEUED)(lambda signal_name, task: events.append(task.id))
        return events

    def seed_preserved(self):
        self.h.enqueue(self.task.s('already-ready', id='already-ready', priority=200))
        # TaskWrapper.s() normalizes eta to whole seconds. The contract is
        # defined by the stored timestamp, so seed the subsecond boundary
        # directly rather than accidentally turning this row into a due job.
        future_time = NOW + dt.timedelta(microseconds=1)
        future = self.task.s('future', id='future')
        future.eta = future_time
        self.h.storage.add_to_schedule(self.h.serialize_task(future), future_time)
        other, other_task = make_huey(self.path, name='another queue')
        self.connections.append(other)
        other.add_schedule(other_task.s('foreign-due', id='foreign-due', eta=NOW))
        other.enqueue(other_task.s('foreign-ready', id='foreign-ready'))
        self.h.storage.put_data('retained-result', b'result-value', is_result=True)
        self.h.revoke_by_id('revoked-opaque-id', revoke_once=True)
        self.h.storage.incr('retained-counter', 19)
        return snapshot(self.path)

    def assert_promoted(self, expected, count):
        result = self.h.promote_schedule(NOW)
        self.assertIs(type(result), int)
        self.assertEqual(result, count)
        actual = task_ids(self.h, self.path)
        self.assertCountEqual(actual, [task.id for task in expected])
        return actual

    def test_boundaries_order_priorities_and_existing_database(self):
        self.assertEqual(self.h.promote_schedule(NOW), 0)
        preserved = self.seed_preserved()
        selected = add_batch(self.h, self.task, 509, seed=662941)
        boundary = self.task.s('boundary', id='at-boundary', eta=NOW, priority=0)
        self.h.storage.add_to_schedule(self.h.serialize_task(boundary), NOW)
        selected.append(boundary)
        events = self.events()
        self.assertEqual(self.h.promote_schedule(NOW), 510)
        expected = ['already-ready'] + [t.id for t in sorted(selected, key=lambda t: -(t.priority or 0))]
        self.assertEqual(task_ids(self.h, self.path), expected)
        self.assertEqual(events, [t.id for t in selected])
        after = snapshot(self.path)
        self.assertEqual(after['schedule'], preserved['schedule'])
        self.assertEqual(after['kv'], preserved['kv'])
        self.assertEqual(after['counter'], preserved['counter'])
        self.assertEqual([r for r in after['task'] if r[1] == 'another queue'],
                         [r for r in preserved['task'] if r[1] == 'another queue'])
        self.assertEqual(self.h.promote_schedule(NOW), 0)
        self.assertEqual(len(events), 510)

    def test_duplicate_rows_and_metadata_survive(self):
        expires = dt.datetime(2035, 6, 7, 8, 9)
        t = self.task.s({'nested': [1, 'x', b'opaque']}, flag=False,
                       id='duplicate-id', eta=NOW, retries=7, retry_delay=13,
                       retry_backoff=2, priority=-3.75, expires=expires, timeout=19)
        t.on_complete = self.task.s('chain', id='next-step', priority=8)
        t.on_error = self.task.s('recover', id='error-step', retries=2)
        for _ in range(3):
            self.h.add_schedule(t)
        events = self.events()
        self.assertEqual(self.h.promote_schedule(NOW), 3)
        ready = [self.h.deserialize_task(data) for data, in rows(self.path, 'task')]
        self.assertEqual(len(ready), 3)
        for item in ready:
            for attr in ('id', 'args', 'kwargs', 'eta', 'retries', 'retry_delay',
                         'retry_backoff', 'priority', 'expires', 'timeout'):
                self.assertEqual(getattr(item, attr), getattr(t, attr), attr)
            self.assertEqual(item.expires_resolved, expires)
            self.assertEqual(item.on_complete.id, 'next-step')
            self.assertEqual(item.on_complete.args, ('chain',))
            self.assertEqual(item.on_error.id, 'error-step')
            self.assertEqual(item.on_error.retries, 2)
        self.assertEqual(events, ['duplicate-id'] * 3)

    def test_omitted_timestamp_and_relative_expiration(self):
        current = self.h._get_timestamp()
        self.h.add_schedule(self.task.s('due', eta=current - dt.timedelta(days=1),
                                       id='relative-expiry', expires=120))
        self.h.add_schedule(self.task.s('future', eta=current + dt.timedelta(days=1), id='future'))
        before = self.h._get_timestamp()
        self.assertEqual(self.h.promote_schedule(), 1)
        after = self.h._get_timestamp()
        ready = self.h.deserialize_task(rows(self.path, 'task')[0][0])
        self.assertGreaterEqual(ready.expires_resolved, before + dt.timedelta(seconds=120))
        self.assertLessEqual(ready.expires_resolved, after + dt.timedelta(seconds=120))
        self.assertEqual(task_ids(self.h, self.path, 'schedule'), ['future'])

    def test_insert_failure_rolls_back_entire_large_batch_and_retries(self):
        self.seed_preserved()
        selected = add_batch(self.h, self.task, 507)
        before = snapshot(self.path)
        events = self.events()
        with sqlite3.connect(str(self.path)) as conn:
            conn.execute('''CREATE TRIGGER injected_insert BEFORE INSERT ON task
              WHEN NEW.queue='main' AND (SELECT count(*) FROM task WHERE queue='main') >= 503
              BEGIN SELECT RAISE(ABORT, 'queue write rejected'); END''')
        with self.assertRaises(Exception):
            self.h.promote_schedule(NOW)
        self.assertEqual(snapshot(self.path), before)
        self.assertEqual(events, [])
        with sqlite3.connect(str(self.path)) as conn:
            conn.execute('DROP TRIGGER injected_insert')
        self.assertEqual(self.h.promote_schedule(NOW), 507)
        self.assertCountEqual(task_ids(self.h, self.path), ['already-ready'] + [t.id for t in selected])

    def test_delete_failure_rolls_back_ready_writes(self):
        selected = add_batch(self.h, self.task, 31)
        before = snapshot(self.path)
        events = self.events()
        target = before['schedule'][17][0]
        with sqlite3.connect(str(self.path)) as conn:
            conn.execute('''CREATE TRIGGER injected_delete BEFORE DELETE ON schedule
              WHEN OLD.id=%d BEGIN SELECT RAISE(ABORT, 'schedule removal rejected'); END''' % target)
        with self.assertRaises(Exception):
            self.h.promote_schedule(NOW)
        self.assertEqual(snapshot(self.path), before)
        self.assertEqual(events, [])
        with sqlite3.connect(str(self.path)) as conn:
            conn.execute('DROP TRIGGER injected_delete')
        self.assert_promoted(selected, 31)

    def test_commit_failure_rolls_back_and_connection_remains_usable(self):
        selected = add_batch(self.h, self.task, 13)
        before = snapshot(self.path)
        events = self.events()
        denied = []

        def authorizer(action, first, second, db, source):
            if action == sqlite3.SQLITE_TRANSACTION and first == 'COMMIT':
                denied.append(first)
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        with configure_connections(self.h, lambda c: c.set_authorizer(authorizer)):
            with self.assertRaises(Exception):
                self.h.promote_schedule(NOW)
        self.assertTrue(denied, 'fault must reach a real SQLite COMMIT')
        self.assertEqual(snapshot(self.path), before)
        self.assertEqual(events, [])
        self.assert_promoted(selected, 13)

    def test_malformed_due_payload_is_recoverable(self):
        add_batch(self.h, self.task, 7)
        self.h.storage.add_to_schedule(b'not-a-pickled-message', NOW - dt.timedelta(seconds=3))
        before = snapshot(self.path)
        events = self.events()
        with self.assertRaises(Exception):
            self.h.promote_schedule(NOW)
        self.assertEqual(snapshot(self.path), before)
        self.assertEqual(events, [])
        replacement = self.h.serialize_task(self.task.s('repaired', id='repaired', eta=NOW))
        with sqlite3.connect(str(self.path)) as conn:
            conn.execute('UPDATE schedule SET data=? WHERE data=?', (replacement, b'not-a-pickled-message'))
        self.assertEqual(self.h.promote_schedule(NOW), 8)

    def test_unregistered_task_rolls_back_until_registered(self):
        add_batch(self.h, self.task, 4)
        special = self.h.task(name='register-later')(echo)
        self.h.add_schedule(special.s('recoverable', id='unknown-until-repaired', eta=NOW))
        special.unregister()
        before = snapshot(self.path)
        events = self.events()
        with self.assertRaises(Exception):
            self.h.promote_schedule(NOW)
        self.assertEqual(snapshot(self.path), before)
        self.assertEqual(events, [])
        self.h.task(name='register-later')(echo)
        self.assertEqual(self.h.promote_schedule(NOW), 5)

    def test_custom_serializer_encode_failure_and_recovery(self):
        serializer = FramedSerializer()
        self.h.serializer = serializer
        selected = add_batch(self.h, self.task, 8, prefix='frame')
        before = snapshot(self.path)
        events = self.events()
        serializer.fail_encoding = True
        with self.assertRaises(Exception):
            self.h.promote_schedule(NOW)
        self.assertEqual(snapshot(self.path), before)
        self.assertEqual(events, [])
        serializer.fail_encoding = False
        self.assert_promoted(selected, 8)
        self.assertTrue(all(data.startswith(b'framed-v1\0') for data, in rows(self.path, 'task')))

    def test_signals_are_postcommit_reentrant_and_failure_isolated(self):
        selected = add_batch(self.h, self.task, 9)
        observations, seen, errors = [], [], []
        extra = self.task.s('created-in-receiver', id='receiver-created', eta=NOW)

        @self.h.signal(SIGNAL_ENQUEUED)
        def receiver(signal_name, task):
            try:
                seen.append(task.id)
                observations.append((self.h.storage.queue_size(), len(rows(self.path, 'task'))))
                if len(seen) == 1:
                    self.h.add_schedule(extra)
            except BaseException as exc:
                errors.append(repr(exc))
                raise

        @self.h.signal(SIGNAL_ENQUEUED)
        def failing_receiver(signal_name, task):
            raise RuntimeError('receiver deliberately failed')

        with self.assertLogs('huey', level='ERROR'):
            self.assertEqual(self.h.promote_schedule(NOW), 9)
        self.assertEqual(errors, [])
        self.assertEqual(observations, [(9, 9)] * 9)
        self.assertEqual(seen, [t.id for t in selected])
        self.assertEqual(task_ids(self.h, self.path, 'schedule'), ['receiver-created'])
        with self.assertLogs('huey', level='ERROR'):
            self.assertEqual(self.h.promote_schedule(NOW), 1)
        self.assertEqual(seen[-1], 'receiver-created')

    def test_concurrent_processes_claim_each_row_once(self):
        selected = add_batch(self.h, self.task, 613, seed=91873)
        start, output = CTX.Event(), CTX.Queue()
        self.h.storage.close()
        self.children = [CTX.Process(target=promote_worker, args=(str(self.path), start, output)) for _ in range(3)]
        for child in self.children:
            child.start()
        start.set()
        results = [output.get(timeout=8) for _ in self.children]
        for child in self.children:
            child.join(3)
            self.assertEqual(child.exitcode, 0)
        self.assertTrue(all(kind == 'ok' for kind, value in results), results)
        self.assertEqual(sum(value for kind, value in results), len(selected))
        self.assertCountEqual(task_ids(self.h, self.path), [t.id for t in selected])
        self.assertEqual(rows(self.path, 'schedule'), [])

    def test_termination_before_commit_preserves_durable_state(self):
        selected = add_batch(self.h, self.task, 27)
        before = snapshot(self.path)
        with sqlite3.connect(str(self.path)) as conn:
            conn.execute('''CREATE TRIGGER precommit_pause AFTER INSERT ON task
              WHEN NEW.queue='main' AND (SELECT count(*) FROM task WHERE queue='main')=5
              BEGIN SELECT promotion_gate(); END''')
        self.h.storage.close()
        reached = CTX.Event()
        child = CTX.Process(target=crash_worker, args=(str(self.path), reached))
        self.children = [child]
        child.start()
        self.assertTrue(reached.wait(8), 'process did not reach the real in-transaction fault point')
        child.kill()
        child.join(3)
        self.assertEqual(child.exitcode, -signal.SIGKILL)
        self.assertEqual(snapshot(self.path), before)
        with sqlite3.connect(str(self.path)) as conn:
            conn.execute('DROP TRIGGER precommit_pause')
        self.assert_promoted(selected, 27)

    def test_scheduler_logs_failed_promotion_then_recovers_and_runs_periodic(self):
        selected = add_batch(self.h, self.task, 5)
        before = snapshot(self.path)
        stop = threading.Event()
        stop.set()
        scheduler = Scheduler(self.h, stop, 1, False)
        with sqlite3.connect(str(self.path)) as conn:
            conn.execute('''CREATE TRIGGER consumer_fault BEFORE INSERT ON task
              BEGIN SELECT RAISE(ABORT, 'consumer queue unavailable'); END''')
        with self.assertLogs('huey.consumer', level='ERROR'):
            scheduler.loop(now=NOW)
        self.assertEqual(snapshot(self.path), before)
        with sqlite3.connect(str(self.path)) as conn:
            conn.execute('DROP TRIGGER consumer_fault')
        scheduler.loop(now=NOW)
        self.assertCountEqual(task_ids(self.h, self.path), [t.id for t in selected])
        periodic = self.h.periodic_task(lambda timestamp: True, name='periodic')(echo)
        scheduler.periodic = True
        scheduler._next_periodic = time.monotonic() - 1
        scheduler.loop(now=NOW)
        ready = [self.h.deserialize_task(data) for data, in rows(self.path, 'task')]
        self.assertEqual(sum(type(t).__name__ == periodic.task_class.__name__ for t in ready), 1)

    def test_scheduler_preserves_cadence_after_failed_promotion(self):
        for interval, work in [(1, 0.125), (3, 0.5), (5, 1.25), (15, 2.5), (60, 5.0)]:
            with self.subTest(interval=interval):
                queue = 'cadence-%d' % interval
                huey, task = make_huey(self.path, name=queue)
                self.connections.append(huey)
                selected = add_batch(huey, task, 4, prefix=queue)
                clock = [1000.0]
                starts, waits = [], []
                timer = mock.Mock()
                timer.monotonic.side_effect = lambda: clock[0]
                stop = mock.Mock()

                def wait(seconds):
                    waits.append(seconds)
                    clock[0] += seconds

                stop.wait.side_effect = wait
                original = huey.promote_schedule

                def promotion(timestamp):
                    starts.append(clock[0])
                    clock[0] += work
                    if len(starts) == 1:
                        raise RuntimeError('temporary promotion failure')
                    return original(timestamp)

                with mock.patch('huey.consumer.time', timer):
                    scheduler = Scheduler(huey, stop, interval, False)
                    with mock.patch.object(huey, 'promote_schedule', promotion):
                        with self.assertLogs('huey.consumer', level='ERROR'):
                            scheduler.loop(now=NOW)
                        scheduler.loop(now=NOW + dt.timedelta(seconds=interval))
                        scheduler.loop(now=NOW + dt.timedelta(seconds=2 * interval))
                self.assertEqual(starts, [1000.0, 1000.0 + interval, 1000.0 + 2 * interval])
                self.assertEqual(waits, [interval - work] * 3)
                self.assertCountEqual(task_ids(huey, self.path, queue=queue), [t.id for t in selected])
                self.assertEqual(rows(self.path, 'schedule', queue=queue), [])

    def test_scheduler_periodic_cadence_does_not_burst_after_stall(self):
        self.h.periodic_task(lambda timestamp: True, name='timed-periodic')(echo)
        clock = [1000.0]
        timer = mock.Mock()
        timer.monotonic.side_effect = lambda: clock[0]
        stop = mock.Mock()
        stop.wait.side_effect = lambda seconds: clock.__setitem__(0, clock[0] + seconds)

        with mock.patch('huey.consumer.time', timer):
            scheduler = Scheduler(self.h, stop, 5, True)
            for _ in range(12):
                scheduler.loop(now=NOW + dt.timedelta(seconds=clock[0] - 1000))
            self.assertEqual(len(rows(self.path, 'task')), 1)
            scheduler.loop(now=NOW + dt.timedelta(seconds=60))
            self.assertEqual(len(rows(self.path, 'task')), 2)
            clock[0] += 300
            for _ in range(3):
                scheduler.loop(now=NOW + dt.timedelta(seconds=clock[0] - 1000))
            self.assertEqual(len(rows(self.path, 'task')), 3)


    def test_legacy_destructive_read_and_memory_backend(self):
        selected = add_batch(self.h, self.task, 6)
        self.h.add_schedule(self.task.s('future', id='future', eta=NOW + dt.timedelta(seconds=1)))
        self.assertEqual([t.id for t in self.h.read_schedule(NOW)], [t.id for t in selected])
        self.assertEqual(rows(self.path, 'task'), [])
        self.assertEqual(task_ids(self.h, self.path, 'schedule'), ['future'])
        self.h.storage.add_to_schedule(b'raw-storage-value', NOW)
        self.assertEqual(self.h.storage.read_schedule(NOW), [b'raw-storage-value'])
        memory = MemoryHuey('memory')
        memory_task = register(memory)
        memory.add_schedule(memory_task.s('x', flag=False, payload={'nested': [b'bytes', 73]}, id='memory-due', eta=NOW))
        memory.add_schedule(memory_task.s('y', id='memory-future', eta=NOW + dt.timedelta(seconds=1)))
        self.assertEqual(memory.promote_schedule(NOW), 1)
        promoted = memory.dequeue()
        self.assertEqual(promoted.id, 'memory-due')
        self.assertEqual(promoted.args, ('x',))
        self.assertEqual(promoted.kwargs, {'flag': False, 'payload': {'nested': [b'bytes', 73]}})
        self.assertEqual(promoted.execute(), ('x', promoted.kwargs))
        self.assertEqual(memory.promote_schedule(NOW), 0)
        self.assertEqual([t.id for t in memory.read_schedule(NOW + dt.timedelta(seconds=1))], ['memory-future'])

    def test_nested_commit_visibility_and_notification_order(self):
        signal.alarm(10)
        first = add_batch(self.h, self.task, 7, prefix='first')
        before = snapshot(self.path)
        seen, observations = [], []

        @self.h.signal(SIGNAL_ENQUEUED)
        def observe(signal_name, task):
            seen.append(task.id)
            observations.append((self.h.storage.queue_size(), len(rows(self.path, 'task'))))

        with self.h.storage.db(commit=True) as outer:
            self.h.storage.put_data('outer-marker', b'preserved')
            self.assertEqual(self.h.promote_schedule(NOW), 7)
            self.assertEqual(self.h.promote_schedule(NOW), 0)
            with self.h.storage.db(commit=False) as reader:
                reader.execute('SELECT count(*) FROM task WHERE queue=?', ('main',))
                self.assertEqual(reader.fetchone()[0], 7)
            second = add_batch(self.h, self.task, 4, prefix='second')
            with self.h.storage.db(commit=True):
                self.assertEqual(self.h.promote_schedule(NOW), 4)
                self.h.storage.incr('nested-counter', 23)
            self.assertEqual(self.h.storage.queue_size(), 11)
            self.assertEqual(snapshot(self.path), before)
            self.assertEqual(seen, [])
            outer.execute('SELECT count(*) FROM schedule')
            self.assertEqual(outer.fetchone()[0], 0)
        self.assertEqual(seen, [t.id for t in first + second])
        self.assertEqual(observations, [(11, 11)] * 11)
        self.assertEqual(self.h.storage.peek_data('outer-marker'), b'preserved')
        self.assertEqual(self.h.storage.incr('nested-counter', 0), 23)

    def test_nested_rollback_keeps_parent_and_discards_only_its_notifications(self):
        signal.alarm(10)
        first = add_batch(self.h, self.task, 3, prefix='survives')
        seen = self.events()
        with self.h.storage.db(commit=True):
            self.assertEqual(self.h.promote_schedule(NOW), 3)
            later = add_batch(self.h, self.task, 5, prefix='retry')
            self.h.storage.incr('outer-counter', 7)
            try:
                with self.h.storage.db(commit=True):
                    self.assertEqual(self.h.promote_schedule(NOW), 5)
                    self.h.storage.incr('outer-counter', 100)
                    with self.h.storage.db(commit=True):
                        self.h.storage.put_data('rolled-back', b'discard')
                    raise AbortScope('cancel inner unit of work')
            except AbortScope:
                pass
            self.assertEqual(self.h.storage.queue_size(), 3)
            self.assertEqual(self.h.storage.schedule_size(), 5)
            self.assertEqual(self.h.storage.incr('outer-counter', 0), 7)
            self.assertEqual(seen, [])
            self.assertEqual(self.h.promote_schedule(NOW), 5)
        self.assertEqual(seen, [t.id for t in first + later])
        self.assertEqual(self.h.storage.incr('outer-counter', 0), 7)
        self.assertFalse(any(r[1] == 'rolled-back' for r in snapshot(self.path)['kv']))

    def test_outer_cancellation_restores_nested_promotions_and_notifications(self):
        signal.alarm(10)
        selected = add_batch(self.h, self.task, 9)
        before = snapshot(self.path)
        seen = self.events()
        with self.assertRaises(AbortScope):
            with self.h.storage.db(commit=True):
                self.h.storage.incr('discard-counter', 31)
                with self.h.storage.db(commit=True):
                    self.assertEqual(self.h.promote_schedule(NOW), 9)
                self.assertEqual(seen, [])
                raise AbortScope('cancel outer unit of work')
        self.assertEqual(snapshot(self.path), before)
        self.assertEqual(seen, [])
        self.assert_promoted(selected, 9)
        self.assertEqual(seen, [t.id for t in selected])

    def test_failed_nested_transfer_can_retry_without_losing_parent_writes(self):
        signal.alarm(10)
        selected = add_batch(self.h, self.task, 17)
        before = snapshot(self.path)
        seen = self.events()
        target = before['schedule'][8][0]
        with sqlite3.connect(str(self.path)) as conn:
            conn.execute('''CREATE TRIGGER nested_delete_fault BEFORE DELETE ON schedule
                WHEN OLD.id=%d BEGIN SELECT RAISE(ABORT, 'nested delete failed'); END''' % target)
        with self.h.storage.db(commit=True) as cursor:
            self.h.storage.put_data('outer-survives', b'yes')
            with self.assertRaises(Exception):
                self.h.promote_schedule(NOW)
            self.assertEqual(self.h.storage.queue_size(), 0)
            self.assertEqual(self.h.storage.schedule_size(), 17)
            self.assertEqual(self.h.storage.peek_data('outer-survives'), b'yes')
            self.assertEqual(seen, [])
            self.assertEqual(snapshot(self.path), before)
            cursor.execute('DROP TRIGGER nested_delete_fault')
            self.assertEqual(self.h.promote_schedule(NOW), 17)
        self.assertEqual(seen, [t.id for t in selected])
        self.assertEqual(self.h.storage.peek_data('outer-survives'), b'yes')

    def test_nested_completion_failure_recovers_without_committing_parent(self):
        signal.alarm(10)
        selected = add_batch(self.h, self.task, 6)
        seen = self.events()
        denied = []

        def authorizer(action, first, second, db, source):
            if action == sqlite3.SQLITE_SAVEPOINT and first == 'RELEASE' and not denied:
                denied.append(second)
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        with self.h.storage.db(commit=True):
            self.h.storage.put_data('parent-marker', b'safe')
            error = None
            with configure_connections(self.h, lambda c: c.set_authorizer(authorizer)):
                try:
                    count = self.h.promote_schedule(NOW)
                except Exception as exc:
                    error = exc
            if denied:
                self.assertIsNotNone(error)
                self.assertEqual(self.h.storage.schedule_size(), 6)
                self.assertEqual(self.h.storage.queue_size(), 0)
            else:
                # A compliant design need not expose this particular SQL
                # fault point; its nesting semantics are checked separately.
                self.assertIsNone(error)
                self.assertEqual(count, 6)
            self.assertEqual(self.h.storage.peek_data('parent-marker'), b'safe')
            self.assertEqual(seen, [])
            self.assertEqual(self.h.promote_schedule(NOW), 6 if denied else 0)
        self.assertEqual(seen, [t.id for t in selected])

    def test_outer_commit_failure_discards_all_deferred_notifications(self):
        signal.alarm(10)
        selected = add_batch(self.h, self.task, 8)
        before = snapshot(self.path)
        seen = self.events()
        denied = []

        def authorizer(action, first, second, db, source):
            if action == sqlite3.SQLITE_TRANSACTION and first == 'COMMIT':
                denied.append(first)
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        with configure_connections(self.h, lambda c: c.set_authorizer(authorizer)):
            with self.assertRaises(Exception):
                with self.h.storage.db(commit=True):
                    self.h.storage.put_data('uncommitted-marker', b'no')
                    self.assertEqual(self.h.promote_schedule(NOW), 8)
                    self.assertEqual(seen, [])
        self.assertTrue(denied)
        self.assertEqual(snapshot(self.path), before)
        self.assertEqual(seen, [])
        self.assert_promoted(selected, 8)
        self.assertEqual(seen, [t.id for t in selected])

    def test_nested_close_is_deferred_and_signal_dispatch_releases_all_locks(self):
        signal.alarm(10)
        add_batch(self.h, self.task, 4)
        observations, workers = [], []

        @self.h.signal(SIGNAL_ENQUEUED)
        def cross_thread_receiver(signal_name, task):
            values = []
            thread = threading.Thread(target=lambda: values.append(self.h.storage.queue_size()), daemon=True)
            workers.append(thread)
            thread.start()
            thread.join(0.5)
            observations.append((thread.is_alive(), list(values)))

        with self.h.storage.db(commit=True) as outer:
            connection = outer.connection
            with self.h.storage.db(commit=True, close=True):
                self.assertEqual(self.h.promote_schedule(NOW), 4)
                with self.h.storage.db(commit=False, close=True) as reader:
                    reader.execute('SELECT count(*) FROM task WHERE queue=?', ('main',))
                    self.assertEqual(reader.fetchone()[0], 4)
            outer.execute('SELECT count(*) FROM task WHERE queue=?', ('main',))
            self.assertEqual(outer.fetchone()[0], 4)
            self.assertEqual(observations, [])
        for thread in workers:
            thread.join(1)
        self.assertEqual(observations, [(False, [4])] * 4)
        with self.assertRaises(sqlite3.ProgrammingError):
            connection.execute('SELECT 1')
        self.assertEqual(self.h.storage.queue_size(), 4)

    def test_shared_instance_threads_have_separate_transaction_lifetimes(self):
        signal.alarm(10)
        selected = add_batch(self.h, self.task, 12)
        seen = self.events()
        entered, release, competing = threading.Event(), threading.Event(), threading.Event()
        second_entered = threading.Event()
        outcomes = []

        def rollback_thread():
            try:
                with self.h.storage.db(commit=True):
                    self.assertEqual(self.h.promote_schedule(NOW), 12)
                    entered.set()
                    if not release.wait(3):
                        raise RuntimeError('coordination timed out')
                    raise AbortScope('first owner cancelled')
            except AbortScope:
                outcomes.append(('rollback', True))
            except BaseException as exc:
                outcomes.append(('error', repr(exc)))
                entered.set()

        def commit_thread():
            competing.set()
            try:
                with self.h.storage.db(commit=True):
                    second_entered.set()
                    count = self.h.promote_schedule(NOW)
                outcomes.append(('commit', count))
            except BaseException as exc:
                outcomes.append(('error', repr(exc)))

        first = threading.Thread(target=rollback_thread, daemon=True)
        second = threading.Thread(target=commit_thread, daemon=True)
        first.start()
        try:
            self.assertTrue(entered.wait(3))
            second.start()
            self.assertTrue(competing.wait(1))
            self.assertFalse(second_entered.wait(0.15), 'second thread joined an active transaction')
        finally:
            release.set()
            first.join(3)
            if second.ident is not None:
                second.join(3)
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertCountEqual(outcomes, [('rollback', True), ('commit', 12)])
        self.assertEqual(seen, [t.id for t in selected])
        self.assertCountEqual(task_ids(self.h, self.path), [t.id for t in selected])

    def test_file_backend_fallback_preserves_existing_behavior(self):
        file_huey = FileHuey('file-fallback', path=str(Path(self.temp.name) / 'file queue'))
        task = register(file_huey)
        file_huey.enqueue(task.s('existing', id='file-existing'))
        file_huey.add_schedule(task.s('due', flag=True, payload={'nested': [b'opaque', -19]}, id='file-due', eta=NOW))
        file_huey.add_schedule(task.s('future', id='file-future', eta=NOW + dt.timedelta(seconds=1)))
        result = file_huey.promote_schedule(NOW)
        self.assertIs(type(result), int)
        self.assertEqual(result, 1)
        existing = file_huey.dequeue()
        promoted = file_huey.dequeue()
        self.assertEqual(existing.id, 'file-existing')
        self.assertEqual(existing.args, ('existing',))
        self.assertEqual(promoted.id, 'file-due')
        self.assertEqual(promoted.args, ('due',))
        self.assertEqual(promoted.kwargs, {'flag': True, 'payload': {'nested': [b'opaque', -19]}})
        self.assertEqual(promoted.execute(), ('due', promoted.kwargs))
        self.assertIsNone(file_huey.dequeue())
        self.assertEqual(file_huey.promote_schedule(NOW), 0)
        self.assertEqual([t.id for t in file_huey.read_schedule(NOW + dt.timedelta(seconds=1))], ['file-future'])

    def peer(self, name='peer', path=None, **kwargs):
        h, task = make_huey(self.path if path is None else path, name=name, **kwargs)
        self.connections.append(h)
        return h, task

    def watch_queues(self, *queues):
        seen = []
        for h in queues:
            h.signal(SIGNAL_ENQUEUED)(
                lambda signal_name, task, queue=h.name: seen.append((queue, task.id)))
        return seen

    def test_same_file_queues_join_one_transaction_and_keep_payloads(self):
        signal.alarm(10)
        peer, peer_task = self.peer(serializer=FramedSerializer())
        first = add_batch(self.h, self.task, 3, prefix='main')
        second = add_batch(peer, peer_task, 4, prefix='peer')
        before = snapshot(self.path)
        seen = self.watch_queues(self.h, peer)
        with self.h.storage.db(commit=True) as outer:
            self.h.storage.put_data('main-marker', b'one')
            self.assertEqual(self.h.promote_schedule(NOW), 3)
            with peer.storage.db(commit=True):
                peer.storage.put_data('peer-marker', b'two')
                self.assertEqual(peer.promote_schedule(NOW), 4)
                self.h.storage.incr('main-count', 17)
            with peer.storage.db(commit=False) as reader:
                reader.execute('SELECT count(*) FROM task')
                self.assertEqual(reader.fetchone()[0], 7)
            self.assertEqual(peer.storage.queue_size(), 4)
            self.assertEqual(peer.promote_schedule(NOW), 0)
            self.assertEqual(snapshot(self.path), before)
            self.assertEqual(seen, [])
            outer.execute('SELECT count(*) FROM schedule')
            self.assertEqual(outer.fetchone()[0], 0)
        self.assertEqual(seen, [('main', t.id) for t in first] + [('peer', t.id) for t in second])
        self.assertEqual(peer.storage.peek_data('peer-marker'), b'two')
        self.assertEqual(self.h.storage.peek_data('main-marker'), b'one')
        self.assertEqual(self.h.storage.incr('main-count', 0), 17)
        expected = {t.id: (t.args, t.kwargs) for t in second}
        for _ in second:
            item = peer.dequeue()
            self.assertEqual((item.args, item.kwargs), expected.pop(item.id))
        self.assertEqual(expected, {})

    def test_alias_inner_rollback_restores_both_queues_but_keeps_parent(self):
        signal.alarm(10)
        peer, peer_task = self.peer()
        first = add_batch(self.h, self.task, 2, prefix='survive')
        other = add_batch(peer, peer_task, 5, prefix='peer')
        seen = self.watch_queues(self.h, peer)
        with self.h.storage.db(commit=True):
            self.assertEqual(self.h.promote_schedule(NOW), 2)
            later = add_batch(self.h, self.task, 3, prefix='retry')
            self.h.storage.incr('count', 9)
            with self.assertRaises(AbortScope):
                with peer.storage.db(commit=True):
                    self.assertEqual(peer.promote_schedule(NOW), 5)
                    self.assertEqual(self.h.promote_schedule(NOW), 3)
                    self.h.storage.incr('count', 100)
                    peer.storage.put_data('discard', b'no')
                    raise AbortScope('cancel both queues in the child scope')
            self.assertEqual(self.h.storage.queue_size(), 2)
            self.assertEqual(self.h.storage.schedule_size(), 3)
            self.assertEqual(peer.storage.queue_size(), 0)
            self.assertEqual(peer.storage.schedule_size(), 5)
            self.assertEqual(self.h.storage.incr('count', 0), 9)
            self.assertEqual(seen, [])
            self.assertEqual(peer.promote_schedule(NOW), 5)
            self.assertEqual(self.h.promote_schedule(NOW), 3)
        self.assertEqual(seen, [('main', t.id) for t in first] +
                         [('peer', t.id) for t in other] + [('main', t.id) for t in later])
        self.assertFalse(any(r[1] == 'discard' for r in snapshot(self.path)['kv']))

    def test_alias_outer_abort_discards_every_participant(self):
        signal.alarm(10)
        peer, task = self.peer()
        first = add_batch(self.h, self.task, 3)
        second = add_batch(peer, task, 4, prefix='peer')
        before = snapshot(self.path)
        seen = self.watch_queues(self.h, peer)
        with self.assertRaises(AbortScope):
            with peer.storage.db(commit=True):
                self.assertEqual(self.h.promote_schedule(NOW), 3)
                with self.h.storage.db(commit=True):
                    self.assertEqual(peer.promote_schedule(NOW), 4)
                    peer.storage.incr('discard', 8)
                raise AbortScope('cancel outer owner')
        self.assertEqual(snapshot(self.path), before)
        self.assertEqual(seen, [])
        self.assertEqual(self.h.promote_schedule(NOW), 3)
        self.assertEqual(peer.promote_schedule(NOW), 4)
        self.assertEqual(seen, [('main', t.id) for t in first] + [('peer', t.id) for t in second])

    def test_instance_created_inside_transaction_defers_connection_close(self):
        signal.alarm(10)
        first = add_batch(self.h, self.task, 3)
        seen = self.watch_queues(self.h)
        with self.h.storage.db(commit=True) as outer:
            connection = outer.connection
            self.assertEqual(self.h.promote_schedule(NOW), 3)
            peer, task = self.peer(name='created-inside')
            peer.signal(SIGNAL_ENQUEUED)(lambda signal_name, t: seen.append((peer.name, t.id)))
            selected = add_batch(peer, task, 2)
            with peer.storage.db(commit=True, close=True):
                self.assertEqual(peer.promote_schedule(NOW), 2)
            outer.execute('SELECT count(*) FROM task')
            self.assertEqual(outer.fetchone()[0], 5)
            self.assertEqual(seen, [])
            self.assertEqual(len(rows(self.path, 'task')), 0)
        self.assertEqual(seen, [('main', t.id) for t in first] + [(peer.name, t.id) for t in selected])
        with self.assertRaises(sqlite3.ProgrammingError):
            connection.execute('SELECT 1')
        self.assertEqual(self.h.storage.queue_size(), 3)
        self.assertEqual(peer.storage.queue_size(), 2)

    def test_relative_and_symlink_paths_share_the_same_transaction(self):
        signal.alarm(10)
        relative, rel_task = self.peer(name='relative', path=os.path.relpath(self.path))
        link = Path(self.temp.name) / 'alias.sqlite'
        link.symlink_to(self.path)
        alias, alias_task = self.peer(name='link', path=link)
        first = add_batch(relative, rel_task, 2)
        second = add_batch(alias, alias_task, 3)
        before = snapshot(self.path)
        seen = self.watch_queues(relative, alias)
        with self.h.storage.db(commit=True):
            self.assertEqual(relative.promote_schedule(NOW), 2)
            with alias.storage.db(commit=True):
                self.assertEqual(alias.promote_schedule(NOW), 3)
            self.assertEqual(alias.storage.queue_size(), 3)
            self.assertEqual(snapshot(self.path), before)
            self.assertEqual(seen, [])
        self.assertEqual(seen, [('relative', t.id) for t in first] + [('link', t.id) for t in second])

    def test_distinct_database_transaction_is_not_captured_by_another_file(self):
        signal.alarm(10)
        other_path = Path(self.temp.name) / 'independent.sqlite'
        new_database(other_path)
        other, task = self.peer(name='main', path=other_path)
        add_batch(self.h, self.task, 3)
        independent = add_batch(other, task, 2, prefix='independent')
        before = snapshot(self.path)
        seen = self.watch_queues(self.h, other)
        with self.assertRaises(AbortScope):
            with self.h.storage.db(commit=True):
                self.assertEqual(self.h.promote_schedule(NOW), 3)
                self.assertEqual(other.promote_schedule(NOW), 2)
                self.assertEqual(len(rows(other_path, 'task')), 2)
                self.assertEqual(seen, [('main', t.id) for t in independent])
                raise AbortScope('only the first database is cancelled')
        self.assertEqual(snapshot(self.path), before)
        self.assertEqual(other.storage.queue_size(), 2)

    def test_alias_commit_failure_rolls_back_all_queues_then_recovers(self):
        signal.alarm(10)
        peer, task = self.peer()
        first = add_batch(self.h, self.task, 2)
        second = add_batch(peer, task, 3)
        before = snapshot(self.path)
        seen = self.watch_queues(self.h, peer)
        rejected = []

        def authorizer(action, first, second, db, source):
            if action == sqlite3.SQLITE_TRANSACTION and first == 'COMMIT':
                rejected.append(first)
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        with configure_connections(self.h, lambda c: c.set_authorizer(authorizer)):
            with self.assertRaises(Exception):
                with self.h.storage.db(commit=True):
                    self.assertEqual(peer.promote_schedule(NOW), 3)
                    self.assertEqual(self.h.promote_schedule(NOW), 2)
        self.assertTrue(rejected, 'must reach the final SQLite commit')
        self.assertEqual(snapshot(self.path), before)
        self.assertEqual(seen, [])
        with peer.storage.db(commit=True):
            self.assertEqual(self.h.promote_schedule(NOW), 2)
            self.assertEqual(peer.promote_schedule(NOW), 3)
        self.assertEqual(seen, [('main', t.id) for t in first] + [('peer', t.id) for t in second])

    def test_reentrant_promotion_queues_signals_after_all_already_committed_batches(self):
        signal.alarm(10)
        peer, task = self.peer()
        first = add_batch(self.h, self.task, 2)
        second = add_batch(peer, task, 2, prefix='peer')
        seen, inside, workers, observations = [], [], [], []
        later = []

        def observe(queue, item):
            seen.append((queue.name, item.id))
            values = []
            thread = threading.Thread(target=lambda: values.append(peer.storage.queue_size()), daemon=True)
            workers.append(thread)
            thread.start()
            thread.join(0.5)
            observations.append(not thread.is_alive() and len(values) == 1)
            if item.id == first[0].id:
                later.extend(add_batch(peer, task, 2, prefix='receiver'))
                inside.append(peer.promote_schedule(NOW))
                inside.append(list(seen))

        self.h.signal(SIGNAL_ENQUEUED)(lambda signal_name, item: observe(self.h, item))
        peer.signal(SIGNAL_ENQUEUED)(lambda signal_name, item: observe(peer, item))
        with self.h.storage.db(commit=True):
            self.assertEqual(self.h.promote_schedule(NOW), 2)
            self.assertEqual(peer.promote_schedule(NOW), 2)
        for thread in workers:
            thread.join(1)
        self.assertEqual(inside, [2, [('main', first[0].id)]])
        self.assertEqual(seen, [('main', t.id) for t in first] +
                         [('peer', t.id) for t in second + later])
        self.assertEqual(observations, [True] * 6)

    def test_aliases_in_competing_threads_do_not_share_a_write_scope(self):
        signal.alarm(10)
        alias, _ = self.peer(name='main')
        selected = add_batch(self.h, self.task, 7)
        seen = self.watch_queues(self.h, alias)
        entered, release, attempting, second_entered = [threading.Event() for _ in range(4)]
        outcomes = []

        def first_owner():
            try:
                with self.h.storage.db(commit=True):
                    self.assertEqual(self.h.promote_schedule(NOW), 7)
                    entered.set()
                    if not release.wait(3):
                        raise RuntimeError('coordination timed out')
                    raise AbortScope()
            except AbortScope:
                outcomes.append(('rollback', True))
            except BaseException as exc:
                outcomes.append(('error', repr(exc)))
                entered.set()

        def second_owner():
            attempting.set()
            try:
                with alias.storage.db(commit=True):
                    second_entered.set()
                    count = alias.promote_schedule(NOW)
                outcomes.append(('commit', count))
            except BaseException as exc:
                outcomes.append(('error', repr(exc)))

        a = threading.Thread(target=first_owner, daemon=True)
        b = threading.Thread(target=second_owner, daemon=True)
        a.start()
        try:
            self.assertTrue(entered.wait(3))
            b.start()
            self.assertTrue(attempting.wait(1))
            self.assertFalse(second_entered.wait(0.15))
        finally:
            release.set()
            a.join(3)
            if b.ident is not None:
                b.join(3)
        self.assertFalse(a.is_alive())
        self.assertFalse(b.is_alive())
        self.assertCountEqual(outcomes, [('rollback', True), ('commit', 7)])
        self.assertEqual(seen, [('main', t.id) for t in selected])



    def install_abort_trigger(self, action='ROLLBACK'):
        with sqlite3.connect(str(self.path)) as conn:
            conn.execute("CREATE TRIGGER transaction_fault BEFORE INSERT ON kv "
                         "WHEN NEW.key='abort-key' BEGIN SELECT RAISE(%s, 'injected transaction fault'); END" % action)

    def remove_abort_trigger(self):
        with sqlite3.connect(str(self.path)) as conn:
            conn.execute('DROP TRIGGER transaction_fault')

    def test_automatic_rollback_invalidates_all_enclosing_alias_scopes(self):
        peer, task = self.peer()
        first = add_batch(self.h, self.task, 2)
        second = add_batch(peer, task, 3, prefix='peer')
        before = snapshot(self.path)
        seen = self.watch_queues(self.h, peer)
        other_path = Path(self.temp.name) / 'independent-abort.sqlite'
        new_database(other_path)
        other, other_task = self.peer(name='main', path=other_path)
        independent = add_batch(other, other_task, 1, prefix='independent')
        self.install_abort_trigger()
        reached = []
        with self.assertRaises(Exception):
            with self.h.storage.db(commit=True):
                self.assertEqual(self.h.promote_schedule(NOW), 2)
                self.h.storage.incr('parent-count', 7)
                with peer.storage.db(commit=True):
                    self.assertEqual(peer.promote_schedule(NOW), 3)
                    with self.assertRaises(Exception):
                        with self.h.storage.db(commit=True):
                            peer.storage.put_data('abort-key', b'trigger')
                    reached.append('aborted')
                    for operation in (self.h.storage.queue_size,
                                      lambda: peer.storage.peek_data('missing'),
                                      lambda: self.h.storage.incr('late-counter', 1),
                                      lambda: peer.storage.put_data('late-write', b'no'),
                                      lambda: self.h.promote_schedule(NOW)):
                        with self.assertRaises(Exception):
                            operation()
                    self.assertEqual(other.promote_schedule(NOW), 1)
        self.assertEqual(reached, ['aborted'])
        self.assertEqual(snapshot(self.path), before)
        self.assertEqual(seen, [])
        self.assertEqual(task_ids(other, other_path), [independent[0].id])
        self.remove_abort_trigger()
        with peer.storage.db(commit=True):
            self.assertEqual(self.h.promote_schedule(NOW), 2)
            self.assertEqual(peer.promote_schedule(NOW), 3)
        self.assertEqual(seen, [('main', t.id) for t in first] +
                         [('peer', t.id) for t in second])

    def test_caught_raw_cursor_rollback_still_rejects_outer_commit(self):
        selected = add_batch(self.h, self.task, 3)
        before = snapshot(self.path)
        seen = self.events()
        self.install_abort_trigger()
        reached = []
        with self.assertRaises(Exception):
            with self.h.storage.db(commit=True) as cursor:
                self.assertEqual(self.h.promote_schedule(NOW), 3)
                self.h.storage.put_data('discard', b'no')
                with self.assertRaises(sqlite3.IntegrityError):
                    cursor.execute('INSERT INTO kv VALUES (?, ?, ?)',
                                   ('main', 'abort-key', b'fault'))
                reached.append('caught raw SQLite abort')
                # No subsequent storage call is needed to notice the abort.
        self.assertEqual(reached, ['caught raw SQLite abort'])
        self.assertEqual(snapshot(self.path), before)
        self.assertEqual(seen, [])
        self.remove_abort_trigger()
        self.assertEqual(self.h.promote_schedule(NOW), 3)
        self.assertEqual(seen, [t.id for t in selected])

    def test_aborted_write_recovers_inside_read_scope_without_losing_prior_signals(self):
        peer, task = self.peer()
        first = add_batch(self.h, self.task, 1, prefix='committed')
        second = add_batch(peer, task, 2, prefix='peer')
        seen = self.watch_queues(self.h, peer)
        self.install_abort_trigger()
        with self.h.storage.db(commit=False, close=True) as reader:
            connection = reader.connection
            self.assertEqual(self.h.promote_schedule(NOW), 1)
            later = add_batch(self.h, self.task, 2, prefix='later')
            with self.assertRaises(Exception):
                with peer.storage.db(commit=True):
                    self.assertEqual(peer.promote_schedule(NOW), 2)
                    self.assertEqual(self.h.promote_schedule(NOW), 2)
                    with self.assertRaises(Exception):
                        self.h.storage.put_data('abort-key', b'fault')
            self.assertEqual(self.h.storage.queue_size(), 1)
            self.assertEqual(peer.storage.queue_size(), 0)
            self.assertEqual(self.h.storage.schedule_size(), 2)
            self.assertEqual(peer.storage.schedule_size(), 2)
            with peer.storage.db(commit=True):
                self.assertEqual(self.h.promote_schedule(NOW), 2)
                self.assertEqual(peer.promote_schedule(NOW), 2)
            expected = [('main', t.id) for t in first + later] + [('peer', t.id) for t in second]
            self.assertEqual(seen, expected[:len(seen)])
            reader.execute('SELECT count(*) FROM task')
            self.assertEqual(reader.fetchone()[0], 5)
        self.assertEqual(seen, [('main', t.id) for t in first + later] +
                         [('peer', t.id) for t in second])
        with self.assertRaises(sqlite3.ProgrammingError):
            connection.execute('SELECT 1')
        self.assertEqual(self.h.storage.queue_size(), 3)
        self.assertEqual(peer.storage.queue_size(), 2)

    def test_statement_abort_does_not_poison_a_surviving_parent_transaction(self):
        peer, task = self.peer()
        first = add_batch(self.h, self.task, 2)
        second = add_batch(peer, task, 3, prefix='peer')
        seen = self.watch_queues(self.h, peer)
        self.install_abort_trigger('ABORT')
        with self.h.storage.db(commit=True) as outer:
            self.assertEqual(self.h.promote_schedule(NOW), 2)
            self.h.storage.incr('survive', 9)
            with self.assertRaises(Exception):
                with peer.storage.db(commit=True):
                    self.assertEqual(peer.promote_schedule(NOW), 3)
                    peer.storage.put_data('abort-key', b'fault')
            self.assertEqual(self.h.storage.queue_size(), 2)
            self.assertEqual(peer.storage.schedule_size(), 3)
            self.assertEqual(self.h.storage.incr('survive', 0), 9)
            outer.execute('DROP TRIGGER transaction_fault')
            self.assertEqual(peer.promote_schedule(NOW), 3)
        self.assertEqual(seen, [('main', t.id) for t in first] +
                         [('peer', t.id) for t in second])

    def start_fork(self, hueys, close_first=False, extra=None, target=fork_promote_worker):
        ctx = mp.get_context('fork')
        gate = ctx.Event()
        reader, writer = ctx.Pipe(duplex=False)
        args = (hueys[0], self.task, gate, writer) if target is fork_generation_worker else (
            hueys, gate, writer, close_first,
            (self.task, extra) if extra is not None else None)
        child = ctx.Process(target=target, args=args)
        self.children.append(child)
        child.start()
        writer.close()
        return child, gate, reader

    def finish_fork(self, handle):
        child, gate, reader = handle
        gate.set()
        self.assertTrue(reader.poll(8), 'forked worker must finish after parent releases its scope')
        result = reader.recv()
        reader.close()
        child.join(2)
        self.assertFalse(child.is_alive(), 'forked worker did not exit')
        self.assertEqual(child.exitcode, 0)
        self.assertNotIn('error', result)
        return result

    def test_forked_aliases_do_not_inherit_parent_read_scope_or_notifications(self):
        peer, task = self.peer()
        first = add_batch(self.h, self.task, 2)
        second = add_batch(peer, task, 3, prefix='peer')
        seen = self.watch_queues(self.h, peer)
        with self.h.storage.db(commit=False) as cursor:
            cursor.execute('CREATE TEMP TABLE parent_private_state (value TEXT)')
            cursor.execute("INSERT INTO parent_private_state VALUES ('parent')")
        with self.h.storage.db(commit=False):
            with self.h.storage.db(commit=True):
                self.assertEqual(self.h.promote_schedule(NOW), 2)
                self.assertEqual(peer.promote_schedule(NOW), 3)
            later = add_batch(peer, task, 2, prefix='child-peer')
            handle = self.start_fork([self.h, peer], extra='child-main')
            parent_events = [('main', t.id) for t in first] + [('peer', t.id) for t in second]
            self.assertEqual(seen, parent_events[:len(seen)])
        parent_events = [('main', t.id) for t in first] + [('peer', t.id) for t in second]
        self.assertEqual(seen, parent_events)
        result = self.finish_fork(handle)
        self.assertEqual(result['counts'], [1, 2])
        self.assertEqual(result['scheduled'], [0, 0])
        self.assertEqual(result['ready'], [3, 5])
        for session_tables in result['session_tables']:
            self.assertNotIn('parent_private_state', session_tables)
        self.assertEqual(result['seen'], [('main', 'child-main')] +
                         [('peer', t.id) for t in later])
        self.assertEqual(seen, parent_events)
        self.assertEqual(self.h.storage.queue_size(), 3)
        self.assertEqual(peer.storage.queue_size(), 5)
        with self.h.storage.db(commit=False) as cursor:
            cursor.execute('SELECT value FROM parent_private_state')
            self.assertEqual(cursor.fetchall(), [('parent',)])

    def test_fork_while_another_thread_owns_scope_and_child_close_is_safe(self):
        self.h, self.task = self.peer(name='main', factory=ForkCheckedConnection)
        ForkCheckedConnection.inherited_closes = []
        selected = add_batch(self.h, self.task, 3)
        seen = self.events()
        entered, release = threading.Event(), threading.Event()
        errors = []

        def owner():
            try:
                with self.h.storage.db(commit=False):
                    self.assertEqual(self.h.promote_schedule(NOW), 3)
                    entered.set()
                    if not release.wait(4):
                        raise RuntimeError('parent not released')
            except BaseException as exc:
                errors.append(repr(exc))
                entered.set()

        thread = threading.Thread(target=owner, daemon=True)
        thread.start()
        try:
            self.assertTrue(entered.wait(3))
            self.assertEqual(errors, [])
            handle = self.start_fork([self.h], close_first=True, extra='child-after-commit')
        finally:
            release.set()
            thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        result = self.finish_fork(handle)
        self.assertEqual(result['counts'], [1])
        self.assertEqual(result['seen'], [('main', 'child-after-commit')])
        self.assertEqual(result['ready'], [4])
        self.assertEqual(result['inherited_closes'], [])
        self.assertEqual(seen, [t.id for t in selected])
        self.assertEqual(self.h.storage.queue_size(), 4)
        self.h.storage.put_data('parent-still-works', b'yes')
        self.assertEqual(self.h.storage.peek_data('parent-still-works'), b'yes')

    def test_fork_during_notification_dispatch_gets_an_independent_dispatch_queue(self):
        first = add_batch(self.h, self.task, 2)
        parent_pid = os.getpid()
        handles, seen = [], []

        def on_enqueue(signal_name, item):
            seen.append(item.id)
            if os.getpid() == parent_pid and item.id == first[0].id:
                handles.append(self.start_fork([self.h], extra='child-during-dispatch'))

        self.h.signal(SIGNAL_ENQUEUED)(on_enqueue)
        self.assertEqual(self.h.promote_schedule(NOW), 2)
        self.assertEqual(len(handles), 1)
        self.assertEqual(seen, [t.id for t in first])
        result = self.finish_fork(handles[0])
        self.assertEqual(result['counts'], [1])
        self.assertEqual(result['seen'], [('main', 'child-during-dispatch')])
        self.assertEqual(result['ready'], [3])
        self.assertEqual(seen, [t.id for t in first])
        self.assertEqual(self.h.storage.queue_size(), 3)

    def test_descendant_fork_resets_process_state_again(self):
        selected = add_batch(self.h, self.task, 2)
        seen = self.events()
        handle = self.start_fork([self.h], target=fork_generation_worker)
        result = self.finish_fork(handle)
        self.assertEqual(result['committed'], 2)
        self.assertEqual(result['own_signals'], [t.id for t in selected])
        child = result['descendant']
        self.assertNotIn('error', child)
        self.assertEqual(child['counts'], [1])
        self.assertEqual(child['ready'], [3])
        self.assertEqual(child['scheduled'], [0])
        self.assertEqual(child['seen'], [('main', 'grandchild-job')])
        self.assertEqual(seen, [])
        self.assertEqual(self.h.storage.queue_size(), 3)


if __name__ == '__main__':
    unittest.main(verbosity=2)
