import _pylibcb

# aliasing these exceptions for convenience
ConnectionFailure = _pylibcb.ConnectionFailure
Failure = _pylibcb.Failure
KeyExists = _pylibcb.KeyExists
OutOfMemory = _pylibcb.OutOfMemory
Timeout = _pylibcb.Timeout

class Client(object):
    def __init__(self, host='localhost', user=None, passwd=None, bucket=None, timeout=0):
        if not user:
            user = ''
        if not passwd:
            passwd = ''
        if not bucket:
            bucket = ''

        self.instance = _pylibcb.open(host, user, passwd, bucket)
        self.timeout = int(timeout * 1000)
        
    def get(self, key, timeout=None):
        if timeout:
            return _pylibcb.get(self.instance, key, int(timeout * 1000))
        return _pylibcb.get(self.instance, key, self.timeout)

    def gat(self, key, new_expiry, timeout=None):
        if timeout:
            return _pylibcb.get(self.instance, key, int(timeout * 1000), new_expiry)
        return _pylibcb.get(self.instance, key, self.timeout, new_expiry)

    def get_cas(self, key, timeout=None):
        if timeout:
            return _pylibcb.get(self.instance, key, int(timeout * 1000), 0, 1)
        return _pylibcb.get(self.instance, key, self.timeout, 0, 1)

    def gat_cas(self, key, new_expiry, timeout=None):
        if timeout:
            return _pylibcb.get(self.instance, key, int(timeout * 1000), new_expiry, 1)
        return _pylibcb.get(self.instance, key, self.timeout, new_expiry, 1)

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
        if timeout:
            return _pylibcb.async_wait(self.instance, int(timeout * 1000))
        return _pylibcb.async_wait(self.instance, self.timeout)
