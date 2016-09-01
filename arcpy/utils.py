import gzip
import os
from glob import glob
from itertools import izip, islice, tee

def nwise(iterable, n = 2):
    return izip(*[islice(it, idx, None) for idx, it in enumerate(tee(iterable, n))])

def first(iterable, default = None):
    if iterable:
        for elem in iterable:
            return elem
    return default

class MinMax(object):
    def __init__(self, min, max):
        self.min = min
        self.max = max

    def __str__(self):
        return '[{},{}]'.format(str(self.min), str(self.max))

    def __repr__(self):
        return str(self)

def min_max(iterable, key=None, min_key=None, max_key=None):
    if key is None: key = lambda x: x
    if min_key is None: min_key = key
    if max_key is None: max_key = key

    min_value, max_value = (None, None)
    for value in iterable:
        if min_value is None or min_key(value) < min_key(min_value):
        	min_value = value
        if max_value is None or max_key(value) > max_key(max_value):
        	max_value = value
    return MinMax(min_value, max_value)

class SQL(object):
    @staticmethod
    def is_between_(name, value):
        return '(%s BETWEEN %s AND %s)' % (name, value[0], value[1])

    @staticmethod
    def is_null_(name):
        return '%s IS NULL' % name

    @staticmethod
    def is_not_null_(name):
        return '%s IS NOT NULL' % name

    @staticmethod
    def eq_(name, value):
        return '%s = %s' % (name, str(value))

    @staticmethod
    def neq_(name, value):
        return '%s <> %s' % (name, str(value))

    @staticmethod
    def quote_(value):
        return '\'%s\'' % value

    @staticmethod
    def and_(iterable):
        return ' AND '.join('(%s)' % str(x) for x in iterable)

    @staticmethod
    def or_(iterable):
        return ' OR '.join('(%s)' % str(x) for x in iterable)

def gzip_files(files):
    return [gzip_file(file) for file in files]

def gzip_file(file):
    target = file + '.gz'
    with open(file) as src:
        with gzip.open(target, 'wb') as dst:
            dst.writelines(src)
    os.remove(file)
    return target
