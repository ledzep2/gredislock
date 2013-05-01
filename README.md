GRedisLock
==========

Gevent Frieldly Redis Lock with extensive tests

How it works?
-------------

    from gredislock import GRedisLock

    lock = GRedisLock(redis_instance, "lockname")

    lock.acquire()
    lock.release()

or

    with GRedisLock(redis_instance, "lockname"):
        # do something




    




