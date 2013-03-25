#!/usr/bin/env python

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages

VERSION = '0.0.1'
LONG_DESC = """\
"""

setup(name='django-datatap',
      version=VERSION,
      description="",
      long_description=LONG_DESC,
      classifiers=[
          'Programming Language :: Python',
          'Operating System :: OS Independent',
          'Natural Language :: English',
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
      ],
      keywords='django hyperadmin dockit',
      maintainer = 'Jason Kraus',
      maintainer_email = 'zbyte64@gmail.com',
      url='http://github.com/zbyte64/django-hyperadmin-eventsocket',
      license='New BSD License',
      packages=find_packages(exclude=['tests']),
      test_suite='tests.runtests.runtests',
      tests_require=(
        'pep8==1.3.1',
        'coverage',
        'django',
        'Mock',
        'nose',
        'django-nose',
      ),
      include_package_data = True,
  )
