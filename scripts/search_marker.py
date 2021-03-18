""" Search marker based on query criteria.

python scripts/search_marker --help
"""
import argparse
import logging

from cycif_db import CycSession


parser = argparse.ArgumentParser()

parser.add_argument(
    'q', help="Query, marker info. Supports name, fluor, anti. "
              "Case insensity.")
parser.add_argument(
    '-v', '--verbose', default=False, action='store_true',
    help="Show detailed log.")

args = parser.parse_args()

if args.verbose:
    logging.basicConfig(level=logging.DEBUG)

with CycSession() as csess:
    rval = csess.search_marker(args.q)

for rv in rval:
    print(rv)
