""" Testcases borrowed from gevent.lock_tests """
from __future__ import with_statement
import unittest
#from gevent.monkey import patch_all; patch_all()
from gevent.lock import RLock
import redis
from gredislock import GRedisLock

import cProfile
import pstats
from thread import start_new_thread, get_ident
import threading
from test import test_support as support
import gevent
import time

conn = redis.StrictRedis(host='localhost', port=6379, db=9)

def create_lock():
    return GRedisLock(conn, 'testlock')

def _wait():
    # A crude wait/yield function not relying on synchronization primitives.
    time.sleep(0.01)

class Bunch(object):
    """
    A bunch of threads.
    """
    def __init__(self, f, n, wait_before_exit=False):
        """
        Construct a bunch of `n` threads running the same function `f`.
        If `wait_before_exit` is True, the threads won't terminate until
        do_finish() is called.
        """
        self.f = f
        self.n = n
        self.started = []
        self.finished = []
        self._can_exit = not wait_before_exit
        def task():
            tid = get_ident()
            self.started.append(tid)
            try:
                f()
            finally:
                self.finished.append(tid)
                while not self._can_exit:
                   _wait()
        for i in range(n):
            start_new_thread(task, ())

    def wait_for_started(self):
        while len(self.started) < self.n:
            _wait()

    def wait_for_finished(self):
        while len(self.finished) < self.n:
            _wait()

    def do_finish(self):
        self._can_exit = True


class BaseTestCase(unittest.TestCase):
    def locktype(self):
        return create_lock() 

    def setUp(self):
        conn.execute_command("flushdb")
        self._threads = support.threading_setup()

    def tearDown(self):
        support.threading_cleanup(*self._threads)
        support.reap_children()


class BaseLockTests(BaseTestCase):
    """
    Tests for both recursive and non-recursive locks.
    """

    def test_constructor(self):
        lock = self.locktype()
        del lock

    def test_acquire_destroy(self):
        lock = self.locktype()
        lock.acquire()
        #self.assertTrue(lock.counter <= 0)
        del lock

    def test_acquire_release(self):
        lock = self.locktype()
        lock.acquire()
        lock.release()
        del lock

    def test_try_acquire(self):
        lock = self.locktype()
        self.assertTrue(lock.acquire(False))
        lock.release()

    def test_try_acquire_contended(self):
        lock = self.locktype()
        lock.acquire()
        result = []
        def f():
            result.append(lock.acquire(False))
        Bunch(f, 1).wait_for_finished()
        self.assertFalse(result[0])
        lock.release()

    def test_acquire_contended(self):
        lock = self.locktype()
        lock.acquire()
        N = 5
        def f():
            lock.acquire()
            lock.release()

        b = Bunch(f, N)
        b.wait_for_started()
        _wait()
        self.assertEqual(len(b.finished), 0)
        lock.release()
        b.wait_for_finished()
        self.assertEqual(len(b.finished), N)

    def test_with(self):
        lock = self.locktype()
        def f():
            lock.acquire()
            lock.release()
        def _with(err=None):
            with lock:
                if err is not None:
                    raise err
        _with()
        # Check the lock is unacquired
        Bunch(f, 1).wait_for_finished()
        self.assertRaises(TypeError, _with, TypeError)
        # Check the lock is unacquired
        Bunch(f, 1).wait_for_finished()

    def test_thread_leak(self):
        # The lock shouldn't leak a Thread instance when used from a foreign
        # (non-threading) thread.
        lock = self.locktype()
        def f():
            lock.acquire()
            lock.release()
        n = len(threading.enumerate())
        # We run many threads in the hope that existing threads ids won't
        # be recycled.
        Bunch(f, 15).wait_for_finished()
        self.assertEqual(n, len(threading.enumerate()))


class LockTests(BaseLockTests):
    """
    Tests for non-recursive, weak locks
    (which can be acquired and released from different threads).
    """
    def test_reacquire(self):
        # Lock needs to be released before re-acquiring.
        lock = self.locktype()
        phase = []
        def f():
            lock.acquire()
            phase.append(None)
            lock.acquire()
            phase.append(None)
        start_new_thread(f, ())
        while len(phase) == 0:
            _wait()
        _wait()
        self.assertEqual(len(phase), 1)
        lock.release()
        while len(phase) == 1:
            _wait()
        self.assertEqual(len(phase), 2)

    def test_different_thread(self):
        # Lock can be released from a different thread.
        lock = self.locktype()
        lock.acquire()
        def f():
            lock.release()
        b = Bunch(f, 1)
        b.wait_for_finished()
        lock.acquire()
        lock.release()


def test_massive(N, rounds):
    conn.execute_command("flushdb")
    print "======================= N: %d Reps: %d" % (N, rounds)
    p = cProfile.Profile()

    p.enable()

    for i in xrange(0, rounds):
        lock = create_lock()
        lock.acquire()
        def f():
            lock = create_lock()
            lock.acquire()
            lock.release()
            del lock

        b = Bunch(f, N)
        b.wait_for_started()
        _wait()
        lock.release()
        b.wait_for_finished()

    p.disable()
    
    p.create_stats()
    stats = pstats.Stats(p)
    stats.sort_stats(2)
    #stats.reverse_order()
    stats.print_stats(0.1)



if __name__ == '__main__':
    test_massive(2, 50)
    test_massive(2, 200)
    test_massive(2, 300)

    unittest.main()


