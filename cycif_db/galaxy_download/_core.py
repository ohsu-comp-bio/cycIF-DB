import json
import logging
import pathlib

from bioblend import galaxy
from ..utils import get_configs


log = logging.getLogger(__name__)


def galaxy_client(server=None, api_key=None):
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
    gi = galaxy.GalaxyInstance(url=server, key=api_key)
    return gi


def download_datasets(destination, *datasets, server=None, api_key=None,
                      galaxy_client=None):
    """ download datasets from galaxy server
    """
    gi = galaxy_client
    if not gi:
        gi = galaxy_client(server=server, api_key=api_key)
    dataset_cli = galaxy.datasets.DatasetClient(gi)
    his_cli = galaxy.histories.HistoryClient(gi)

    if isinstance(destination, str):
        destination = pathlib.Path(destination)
    if destination.exists() and destination.is_dir():
        raise Exception("The target folder `%s` has already existed!"
                        % str(destination))
    log.info("Create folder `%s`." % str(destination))
    destination.mkdir(parents=True, exist_ok=False)

    for dataset_id in datasets:
        log.info("Connect to server `%s`. Downloading dataset `%s`"
                 % (gi.url, dataset_id))
        dataset_cli.download_dataset(dataset_id, str(destination),
                                     use_default_filename=True)

    annotation = {"server": server}
    history_id = dataset_cli.show_dataset(dataset_id)['history_id']
    history_username_and_slug = \
        his_cli.show_history(history_id)['username_and_slug']
    annotation['history_username_and_slug'] = history_username_and_slug
    annotation['datasets'] = datasets

    with open(destination.joinpath('annotation.txt'), 'w') as fp:
        json.dump(annotation, fp)
