from os.path import realpath, dirname, join
from setuptools import find_packages
from distutils.core import setup


PROJECT_ROOT = dirname(realpath(__file__))

with open(join(PROJECT_ROOT, 'requirements.txt'), 'r') as f:
    install_reqs = f.read().splitlines()

with open(join(PROJECT_ROOT, 'cycif_db', '__init__.py'), 'r') as f:
    for line in f:
        if line.startswith('__version__'):
            exec(line)
            VERSION = __version__
            break

long_description = """

This library provides simple database APIs for
managing cycIF quantification data.


This library is hosted at
https://github.com/ohsu-comp-bio/cycIF-DB.

"""

setup(name='cycIF-DB',
      version=VERSION,
      description='A library of database utils for managing cycIF '
                  'quantification data',
      long_description=long_description,
      long_description_content_type="text/markdown",
      url='https://github.com/ohsu-comp-bio/cycIF-DB',
      packages=find_packages(
          exclude=['tests*', 'test-data*', 'examples']),
      entry_points={
          'console_scripts': [
              'cycif_db = cycif_db.__main__:main',
          ],
      },
      package_data={
          '': ['README.md',
               'requirements.txt',
               'alembic.ini',
               'alembic/script.py.mako',
               'cycif_db/config.yml',
               'cycif_db/markers.json']},
      include_package_data=True,
      install_requires=install_reqs,
      platforms='any',
      classifiers=[
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'License :: OSI Approved :: MIT License',
          'Operating System :: Unix',
          'Operating System :: MacOS',
          'Topic :: Scientific/Engineering',
          'Topic :: Scientific/Engineering :: Bio-Informatics',
          'Topic :: Scientific/Engineering :: Database',
          'Topic :: Scientific/Engineering :: Microscopy imaging',
          'Topic :: Scientific/Engineering :: Machine learning',
      ])
