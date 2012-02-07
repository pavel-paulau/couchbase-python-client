from distutils.core import setup, Extension

opaque = Extension('opaque',
                   sources = ['opaque.c'])

setup (name = 'Opaque',
       version = '1.0',
       description = 'Opaque objects test',
       ext_modules = [opaque])
