import _pylibcb

from decorators import get_as_json, set_as_json


class Client(object):

    """Couchbase client"""

    def __init__(self, host='localhost', user='', password='',
                 bucket='default', timeout=0):
        """Open connection to Couchbase server.

        :param host: hostname of IP address
        :param user: administrative username (is SASL bucked is used)
        :param password: administrative password (is SASL bucked is used)
        :param bucket: bucket name
        :param timeout: optional timeout in milliseconds"""
        self.instance = _pylibcb.open(host, user, password, bucket)
        self.timeout = int(timeout * 1000)

    @get_as_json
    def get(self, key, timeout=0):
        """Get a value by key.

        :param key: key to search for
        :param timeout: optional timeout in milliseconds"""
        timeout = int(timeout * 1000) or self.timeout
        return _pylibcb.get(self.instance, key, timeout)

    @get_as_json
    def gat(self, key, expiry, timeout=0):
        """Get and touch.

        :param key: key to search for
        :param expiry: new expiration time
        :param timeout: optional timeout in milliseconds"""
        timeout = int(timeout * 1000) or self.timeout
        return _pylibcb.get(self.instance, key, timeout, expiry)

    @get_as_json
    def get_cas(self, key, timeout=0):
        """GET with CAS.

        :param key: key to search for
        :param timeout: optional timeout in milliseconds"""
        timeout = int(timeout * 1000) or self.timeout
        return _pylibcb.get(self.instance, key, timeout, 0, 1)

    @get_as_json
    def gat_cas(self, key, expiry, timeout=0):
        """GAT with CAS.

        :param key: key to search for
        :param expiry: new expiration time
        :param timeout: optional timeout in milliseconds"""
        timeout = int(timeout * 1000) or self.timeout
        return _pylibcb.get(self.instance, key, timeout, expiry, 1)

    @set_as_json
    def set(self, key, value, expiry=0, cas=0):
        """Set a value by key.

        :param key: document key
        :param value: document value
        :param expiry: expiration time
        :param cas: CAS (Compare And Swap) value"""
        return _pylibcb.set(self.instance, key, value, expiry, cas)

    def remove(self, key):
        """Remove a value by key

        :param key: key of document to be removed"""
        return _pylibcb.remove(self.instance, key)

    def get_async_limit(self):
        """Get the limit for the number of requests allowed before one is
        required to complete"""
        return _pylibcb.get_async_limit(self.instance)

    def set_async_limit(self, limit):
        """Set the limit for the number of requests allowed before one is
        required to complete"""
        return _pylibcb.set_async_limit(self.instance, limit)

    def get_async_count(self):
        """Get the number of incomplete asynchronous requests waiting"""
        return _pylibcb.get_async_count(self.instance)

    def enable_async(self):
        """Enable asynchronous behavior"""
        return _pylibcb.enable_async(self.instance)

    def disable_async(self):
        """Disable asynchronous behavior"""
        return _pylibcb.disable_async(self.instance)

    def async_wait(self, timeout=0):
        """Execute eventloop for a given number of milliseconds"""
        timeout = int(timeout * 1000) or self.timeout
        return _pylibcb.async_wait(self.instance, timeout)
