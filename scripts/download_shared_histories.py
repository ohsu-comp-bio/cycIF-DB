""" Download datasets from histories list on /list_shared webpage.

python scripts/download_sandana_datasets.py --help
"""
import argparse
import logging

from cycif_db.galaxy_download import SharedGalaxy

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
    '-v', '--verbose', default=False, action='store_true',
    help="Show detailed log.")
parser.add_argument(
    '-vv', '--debug', default=False, action='store_true',
    help="Show detailed log.")

args = parser.parse_args()

if args.verbose:
    logging.basicConfig(level=logging.WARNING)
if args.debug:
    logging.basicConfig(level=logging.DEBUG)

shared = SharedGalaxy(browser='Chrome', headless=True, cutoff_time='2021-01-13')
shared.download(args.destination, server=args.server, api_key=args.api_key)
shared.quit()
