""" Search sample based on query criteria.

python scripts/search_sample --help
"""
import argparse
import logging

from cycif_db import CycSession


parser = argparse.ArgumentParser()

parser.add_argument(
    'q', help="Query, info from a sample. Support name and tag. "
              "Case insensitive.")
parser.add_argument(
    '-v', '--verbose', default=False, action='store_true',
    help="Show detailed log.")

args = parser.parse_args()

if args.verbose:
    logging.basicConfig(level=logging.DEBUG)

with CycSession() as csess:
    rval = csess.search_sample(args.q)

for rv in rval:
    print(rv)
