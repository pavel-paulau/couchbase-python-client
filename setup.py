from distutils.core import setup, Extension

pylibcb = Extension('_pylibcb',
                    libraries=['event', 'couchbase'],
                    sources=['couchbase/pylibcb.c'])

setup(
    name='couchbase',
    version='0.1.0',
    description='Couchbase Python client',
    ext_modules=[pylibcb],
    packages=['couchbase'],
    license="LICENSE.txt",
    keywords=["encoding", "i18n", "xml"],
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",
        "Development Status :: 4 - Beta",
        "Environment :: Other Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
