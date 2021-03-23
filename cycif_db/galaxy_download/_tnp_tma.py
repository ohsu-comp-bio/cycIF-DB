import logging
import pathlib
import re
import requests

from bioblend import galaxy
from ._core import (
    galaxy_client,
    download_datasets,
    find_markers_csv_and_quantification_v2
)


log = logging.getLogger(__name__)

url = ('https://galaxy.ohsu.edu/galaxy/history/list_published?'
       'async=false&sort=update_time&page=all&show_item_checkboxes=false'
       '&advanced_search=false&f-username=All&f-tags=All')


def is_tnp_tma_history(name):
    """ whether a history runs sandana sample

    name: str
        Name of a galaxy history.
    """
    return 'tnp-tma' in name.lower()


def get_sample_name(history_name):
    """ generate sample name for a galaxy history running cycif workflow.

    Parameters
    ----------
    history_name: str.
        The name of a history.

    Returns
    --------
    str
    """
    match = re.match('(?P<tag>\S+)\s+(?P<name>TNP-TMA\S+)\s',
                     history_name, flags=re.I)
    name = match.group('name')
    tag = match.group('tag')

    rval = name + '__' + tag

    log.info(f"Generate sample name `{rval}`.")
    return rval


def download_tnp_tma(destination, server=None, api_key=None):
    """ download markers.csv and quantification datasets from a history
    running TNP-TMA samples.

    Parameters
    ----------
    destination: str
        The folder path to save the datasets.
    server: str
        Galaxy server. Optional.
    api_key: str
        The galalxy user API key to the galaxy server.
    """
    res = requests.get(url)
    assert res.status_code == 200

    histories = res.json()['items']
    histories = [{'name': his['column_config']['Name']['value'],
                  'encode_id': his['encode_id']} for his in histories]
    histories = [his for his in histories if is_tnp_tma_history(his['name'])]
    sample_names = [get_sample_name(his['name'])
                    for his in histories]

    gi = galaxy_client(server=server, api_key=api_key)
    his_cli = galaxy.histories.HistoryClient(gi)

    markers_and_quants = [
        find_markers_csv_and_quantification_v2(his_cli, his['encode_id'])
        for his in histories]

    folder = pathlib.Path(destination)
    for name, datasets in zip(sample_names, markers_and_quants):
        if datasets:
            dataset_ids = [dataset['id'] for dataset in datasets]
            cp_dataset_ids = [dataset_ids[0], dataset_ids[2]]
            destination = folder.joinpath(name + '_' + 'cellpose').absolute()
            try:
                download_datasets(destination, *cp_dataset_ids, galaxy_client=gi)
            except Exception as e:
                log.warn(e)
            s3_dataset_ids = [dataset_ids[1], dataset_ids[2]]
            destination = folder.joinpath(name + '_' + 's3').absolute()
            try:
                download_datasets(destination, *s3_dataset_ids, galaxy_client=gi)
            except Exception as e:
                log.warn(e)
