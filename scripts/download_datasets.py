""" Download quantification result datasets from a Galaxy server

python scripts/download_datasets.py --help
"""
import argparse
import logging

from cycif_db.galaxy_download.core import download_datasets

parser = argparse.ArgumentParser()

parser.add_argument(
    '--server', '-s', type=str, dest='server', required=False,
    help="Galaxy server URL address. Can be set in `config.yml`.")
parser.add_argument(
    '--key', '-k', type=str, dest='api_key', required=False,
    help="API key to the Galaxy server. Can be set in `config.yml`.")
parser.add_argument(
    'destination', type=str,
    help="The folder to save the downloaded files.")
parser.add_argument(
    'datasets', type=str, nargs='+',
    help="Dataset IDs in Galaxy.")
parser.add_argument(
    '-v', '--verbose', default=False, action='store_true',
    help="Show detailed log.")

args = parser.parse_args()

if args.verbose:
    logging.basicConfig(level=logging.DEBUG)

download_datasets(args.destination, *args.datasets,
                  server=args.server, api_key=args.api_key)
