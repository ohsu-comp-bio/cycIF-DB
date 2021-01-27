""" Download quantification result datasets from a Galaxy server

python scripts/download_datasets.py --help
"""
import argparse
import json
import logging
import pathlib
import sys
from bioblend import galaxy

work_dir = pathlib.Path(__file__).absolute().parent.parent
sys.path.insert(1, str(work_dir))
from cycif_db.utils import get_configs


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

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

args = parser.parse_args()
configs = get_configs()

server = args.server
if not server:
    server = configs.get('galaxy_server')
if not server:
    raise Exception("Argument `server` was not provided! Use `--help` for "
                    "help. The parameter can be set in `config.yml` as well.")

api_key = args.api_key
if not api_key:
    api_key = configs.get('api_key')
if not api_key:
    raise Exception("Argument `api` was not privided! Use `--help` for help."
                    "The parameter can be set in `config.yml` as well.")

folder = pathlib.Path(args.destination)
if folder.exists() and folder.is_dir():
    raise Exception("The target folder `{folder}` has already existed!")
log.info(f"Create folder `{folder}`.")
folder.mkdir(parents=True, exist_ok=False)

gi = galaxy.GalaxyInstance(url=server, key=api_key)
dataset_cli = galaxy.datasets.DatasetClient(gi)
his_cli = galaxy.histories.HistoryClient(gi)

datasets = args.datasets
for dataset_id in datasets:
    log.info("Connect to server `%s`. Downloading dataset `%s`"
             % (server, dataset_id))
    dataset_cli.download_dataset(dataset_id, folder, use_default_filename=True)

annotation = {"server": server}
history_id = dataset_cli.show_dataset(dataset_id)['history_id']
history_username_and_slug = \
    his_cli.show_history(history_id)['username_and_slug']
annotation['history_username_and_slug'] = history_username_and_slug
annotation['datasets'] = datasets

with open(folder.joinpath('annotation.txt'), 'w') as fp:
    json.dump(annotation, fp)
