try:
    from setuptools import setup
except:
    from disutils.core import setup

setup(
    name='GRedisLock',
    version='1.0.0',
    description='Gevent friendly Redis Lock',
    author='yufeiwu',
    author_email='yufeiwu@gmail.com',
    license='MIT',
    packages=['gredislock']
)
