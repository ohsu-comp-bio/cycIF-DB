import json
import logging
import pathlib

from bioblend import galaxy
from ..utils import get_configs


log = logging.getLogger(__name__)


def download_datasets(destination, *datasets, server=None, api_key=None):
    """ download datasets from galaxy server
    """
    configs = get_configs()

    if not server:
        server = configs.get('galaxy_server')
    if not server:
        raise Exception("Argument `server` was not provided! Use `--help` for "
                        "help. The parameter can be set in `config.yml` as well.")

    if not api_key:
        api_key = configs.get('api_key')
    if not api_key:
        raise Exception("Argument `api` was not privided! Use `--help` for help."
                        "The parameter can be set in `config.yml` as well.")

    folder = pathlib.Path(destination)
    if folder.exists() and folder.is_dir():
        raise Exception("The target folder `{folder}` has already existed!")
    log.info(f"Create folder `{folder}`.")
    folder.mkdir(parents=True, exist_ok=False)

    gi = galaxy.GalaxyInstance(url=server, key=api_key)
    dataset_cli = galaxy.datasets.DatasetClient(gi)
    his_cli = galaxy.histories.HistoryClient(gi)

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
