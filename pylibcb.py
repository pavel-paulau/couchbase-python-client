import _pylibcb


class Client(object):

    """Couchbase client"""

    def __init__(self, host='localhost', user='', password='', bucket='',
                 timeout=0):
        self.instance = _pylibcb.open(host, user, password, bucket)
        self.timeout = int(timeout * 1000)

    def get(self, key, timeout=None):
        timeout = int(timeout * 1000) or self.timeout
        return _pylibcb.get(self.instance, key, timeout)

    def gat(self, key, new_expiry, timeout=None):
        timeout = int(timeout * 1000) or self.timeout
        return _pylibcb.get(self.instance, key, timeout, new_expiry)

    def get_cas(self, key, timeout=None):
        timeout = int(timeout * 1000) or self.timeout
        return _pylibcb.get(self.instance, key, timeout, 0, 1)

    def gat_cas(self, key, new_expiry, timeout=None):
        timeout = int(timeout * 1000) or self.timeout
        return _pylibcb.get(self.instance, key, timeout, new_expiry, 1)

    def set(self, key, value, expiry=0, cas=0):
        return _pylibcb.set(self.instance, key, value, expiry, cas)

    def remove(self, key, cas=0):
        return _pylibcb.remove(self.instance, key)

    def get_async_limit(self):
        return _pylibcb.get_async_limit(self.instance)

    def set_async_limit(self, limit):
        return _pylibcb.set_async_limit(self.instance, limit)

    def get_async_count(self):
        return _pylibcb.get_async_count(self.instance)

    def enable_async(self):
        return _pylibcb.enable_async(self.instance)

    def disable_async(self):
        return _pylibcb.disable_async(self.instance)

    def async_wait(self, timeout=None):
        timeout = int(timeout * 1000) or self.timeout
        return _pylibcb.async_wait(self.instance, timeout)
