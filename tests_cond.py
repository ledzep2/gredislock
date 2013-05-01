""" Testcases borrowed from git://github.com/foxx/python-redislock.git """

import redis
import logging
import multiprocessing
import time
import sys
import traceback
from Queue import Empty, Full
from pprint import pprint as p
from pprint import pformat as pf

from gredislock import GRedisLock

###############################################################
# CONFIGURATION
###############################################################

# Worker config
WORKERS = 5
WORKERS_DUPLICATE = 5

# Logging config
DEBUG_LEVEL = logging.INFO

# Others
THREAD_SYNC_WAIT = 2
LOCK_TIMEOUT = 5
RESULT_TIMEOUT = 20
MQUEUE_TIMEOUT = 1
COLLECT_FPS = 0.01

#### DO NOT EDIT BELOW THIS LINE #####

def configure_logging():
    FORMAT = "[%(asctime)-15s] [%(levelname)s] [%(process)d/%(processName)s] %(message)s"
    logging.basicConfig(format=FORMAT, level=DEBUG_LEVEL)

def display_traceback(msg=""):
    msg = "%s; %s" % ( msg, get_traceback())
    logging.error(msg)

def get_traceback():
    exc_info = sys.exc_info()
    msg = '\n'.join(traceback.format_exception(*exc_info))
    return msg

configure_logging()


class LockTest(object):
    """Simulate workers trying to acquire a lock at the same time.

    Locks are given based on the worker_id.

    This example will attempt to create 5 sets of 5 workers, which
    means each worker_id will be spawned 5 times.

    The end result is that only 5 of the workers should acquire the lock."""

    def __init__(self):
        # temp assign
        expected_total = ( WORKERS * WORKERS_DUPLICATE )

        self.redis = redis.StrictRedis(host='localhost', port=6379, db=9)
        self.mqueue = multiprocessing.Queue(expected_total)
        self.threads = []
        self.lock = None

    def run_worker(self, worker_id, mqueue, instance_id, cqueue):
        """Entry point for worker threads"""

        logging.debug("started thread")

        try:
            # create lock instance
            w = self.lock(self.redis, str(worker_id))

            while True:
                # wait for command
                #logging.debug("waiting for cmd")
                cmd, start_ts, args, kwargs = cqueue.get()
                logging.debug("received cmd: %s" % ( (cmd, start_ts, args, kwargs), ))

                # re-produce race condition (ensures they all start at the same time)
                while time.time() < start_ts:
                    time.sleep(0.01)
                logging.debug("start_ts reached")

                # execute command
                if cmd == 'acquire':
                    result = w.acquire(*args, **kwargs)

                elif cmd == 'release':
                    result = w.release(*args, **kwargs)

                else:
                    assert False, "no such cmd %s" % ( cmd, )

                mqueue.put({
                    'worker_id' : worker_id,
                    'result' : result,
                    #'release' : r_result,
                    'instance_id' : instance_id
                })

        except:
            display_traceback()

    def start_threads(self):
        """Starts up the threads"""

        for worker_id in range(WORKERS):
            for instance_id in range(WORKERS_DUPLICATE):
                worker_name = "worker-%s-%s" % ( worker_id, instance_id, )
                cqueue = multiprocessing.Queue(1)
                t = multiprocessing.Process(target=self.run_worker, name=worker_name, args=(worker_id, self.mqueue, instance_id, cqueue))
                self.threads.append((t, cqueue))
                t.start()

    def stop_threads(self):
        """Hard stop all threads"""

        # ensure all threads are hard killed
        for t, cqueue in self.threads:
            logging.debug("terminating %s" % ( t.name, ))
            t.terminate()

    def thread_action(self, action, *args, **kwargs):
        # sanity checks
        assert action in ['acquire', 'release']

        # push to threads
        start_ts = time.time() + THREAD_SYNC_WAIT
        for t, cqueue in self.threads:
            cqueue.put((action, start_ts, args, kwargs))

        expected_total = ( WORKERS * WORKERS_DUPLICATE )
        results = []
        start_ts = time.time()

        while True:
            # sanity checks
            assert len(results) <= expected_total, "wtf"

            # temp assign
            elapsed = ( time.time() - start_ts )

            if elapsed > RESULT_TIMEOUT:
                raise Exception, "collect_results() timed out"

            # looks like we got all our results
            if len(results) == expected_total:
                break

            try:
                res = self.mqueue.get(block=True, timeout=MQUEUE_TIMEOUT)
                results.append(res)
            except Empty:
                pass

            time.sleep(COLLECT_FPS)
        
        return results

    def test_release(self):
        """Attempt to release the lock on each thread.

        Collect the lock results back from each thread, and check 
        if the locking semantics worked as expected"""

        # fetch results of release from all threads
        result = self.thread_action(action='release')

        logging.debug("Lock release() result:")
        logging.debug(pf(result))

        instance_ids = []
        success = True

        # find duplicate instances
        for worker_id in range(WORKERS):
            worker_results = filter(lambda x: x.get('worker_id') == worker_id, result)
            locks = filter(lambda x: x.get('result') == True, worker_results)
            lock_instance_ids = map(lambda x: str(x.get('instance_id')), locks)
            instance_ids.append(lock_instance_ids)
            lock_instance_ids = ",".join(lock_instance_ids)

            if len(locks) == 1:
                logging.info("worker_id[%s] result: PASSED (lock released by instance %s)" % ( worker_id, lock_instance_ids, ))
            else:
                logging.error("worker_id[%s] result: FAILED (lock released by instances %s)" % ( worker_id, lock_instance_ids, ))
                success = False

        return instance_ids, success

    def test_acquire(self, timeout):
        """Attempt to acquire the lock on each thread.

        Collect the lock results back from each thread, and check 
        if the locking semantics worked as expected"""

        # fetch results of acquire from all threads
        result = self.thread_action(action='acquire', blocking=False, timeout=timeout)

        logging.debug("Lock acquire() result:")
        logging.debug(pf(result))

        instance_ids = []
        success = True

        # find duplicate instances
        for worker_id in range(WORKERS):
            worker_results = filter(lambda x: x.get('worker_id') == worker_id, result)
            locks = filter(lambda x: x.get('result') == True, worker_results)
            lock_instance_ids = map(lambda x: str(x.get('instance_id')), locks)
            instance_ids.append(lock_instance_ids)
            lock_instance_ids = ",".join(lock_instance_ids)

            if len(locks) == 1:
                logging.info("worker_id[%s] result: PASSED (lock acquired by instance %s)" % ( worker_id, lock_instance_ids, ))
            else:
                logging.error("worker_id[%s] result: FAILED (lock acquired by instances %s)" % ( worker_id, lock_instance_ids, ))
                success = False

        return instance_ids, success

    def run(self, lock):
        """Start the test"""

        # clear redis
        self.redis.flushall()
        logging.info("Redis flushall() executed")

        # determine lock type
        self.lock = lock

        logging.info("Testing lock type: %s" % ( lock, )) 

        # listen for results
        try:
            # start threads
            logging.info("Starting threads")
            self.start_threads()

            # Attempt to acquire the first set of locks
            logging.info("------------------------------------------------------")
            logging.info("TEST 1: Initial lock acquire() on 5s timeout")
            ids_acquire, result = self.test_acquire(timeout=5)
            if result:
                logging.info("PASSED")
            else:
                logging.debug("FAILED")

            # Attempt to release the locks immediately
            logging.info("------------------------------------------------------")
            logging.info("TEST 2: Initial lock release() on 2s timeout")
            ids_release, result = self.test_release()
            if result:
                logging.info("PASSED")
            else:
                logging.debug("FAILED")

            logging.info("------------------------------------------------------")
            logging.info("TEST 3: Compare instance IDs between acquire() and release()")
            print ids_acquire, ids_release
            if ids_acquire == ids_release:
                logging.info("PASSED")
            else:
                logging.error("FAILED")

            # clear redis
            self.redis.flushall()
            logging.info("Redis flushall() executed")

            # Attempt to acquire the first set of locks
            logging.info("------------------------------------------------------")
            logging.info("TEST 4: Initial lock acquire() on 5s timeout")
            ids_acquire, result = self.test_acquire(timeout=5)
            if result:
                logging.info("PASSED")
            else:
                logging.debug("FAILED")

            time.sleep(5)

            # attempt to acquire based on timeout
            logging.info("------------------------------------------------------")
            logging.info("TEST 5: Attempt lock acquire() after timeout")
            ids_acquire, result = self.test_acquire(timeout=5)
            if result:
                logging.info("PASSED")
            else:
                logging.debug("FAILED")

            return

            # wait until the timeout expired
            logging.info("waiting for lock timeout to expire")
            time.sleep(LOCK_TIMEOUT + 2)

            # second
            logging.info("running acquire after expire test")
            self.test_acquire()

        except:
            # soft handle traceback
            display_traceback()

        finally:
            self.stop_threads()

def run_tests():
    t = LockTest()
    t.run(GRedisLock)

run_tests()
