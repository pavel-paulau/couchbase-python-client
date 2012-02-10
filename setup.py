from distutils.core import setup, Extension

#gcc pylibcb.c -I/opt/couchbase/include -Wl,-rpath -Wl,/opt/couchbase/lib -levent -lcouchbase

pylibcb = Extension('_pylibcb',
                    #include_dirs = ['/opt/couchbase/include'],
                    libraries = ['event', 'couchbase'],
                    library_dirs = ['/opt/couchbase/lib'],
                    sources = ['pylibcb.c'])

setup (name = 'Pylibcb',
       version = '0.1',
       description = 'pylibcb',
       ext_modules = [pylibcb],
       py_modules = ['pylibcb'])
