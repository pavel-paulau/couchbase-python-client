from distutils.core import setup, Extension

#gcc pylibcb.c -I/opt/couchbase/include -Wl,-rpath -Wl,/opt/couchbase/lib -levent -lcouchbase

pylibcb = Extension('_pylibcb',
                    #include_dirs = ['/opt/couchbase/include'],
                    libraries = ['event', 'couchbase'],
                    library_dirs = ['/opt/couchbase/lib'],
                    sources = ['pylibcb.c'])

setup (name = 'pylibcb',
       version = '0.1.1',
       description = 'Python Couchbase Client',
       author='Sebastian Hubbard',
       author_email='sebastian@chango.com',
       url='http://www.bitbucket.org/chango/pylibcb',
       ext_modules = [pylibcb],
       py_modules = ['pylibcb'])
