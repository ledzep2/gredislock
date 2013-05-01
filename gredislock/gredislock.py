import gevent
import string
import random
import logging

logger = logging.getLogger("GRedisLock")

__all__ = ['GRedisLock', 'LockMismatch']

class LockMismatch(Exception):
    pass

class GRedisLock(object):
    def __init__(self, connection, key, lock_value = None):
        self.conn = connection
        self.key = "RLOCK:%s" % key
        self.value = lock_value and lock_value or "".join([random.choice(string.letters) for i in xrange(0,10)])

    def acquire(self, blocking = True, timeout = 0, acquire_timeout = None):
        with gevent.Timeout(acquire_timeout, False):
            while not self.conn.setnx(self.key, self.value):
                if timeout > 0 and self.conn.ttl(self.key) == None:
                    self.conn.expire(self.key, timeout)

                if not blocking:
                    return False

                gevent.sleep(0.005)

        if timeout > 0:
            self.conn.expire(self.key, timeout)
        return True

    def is_locked(self, check_owner = False):
        if not check_owner:
            return self.conn.get(self.key) != None
        else:
            return self.conn.get(self.key) == self.value 

    def release(self, force_owner = False):
        if force_owner:
            if self.conn.get(self.key) != self.value:
                raise LockMismatch()

        return self.conn.delete(self.key) == 1

    def __enter__(self):
        self.acquire()

    def __exit__(self, *args):
        self.release()


        
