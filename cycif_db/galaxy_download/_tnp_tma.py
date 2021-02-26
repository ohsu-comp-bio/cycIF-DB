import logging
import pathlib
import re
import requests

from bioblend import galaxy
from ._core import galaxy_client, download_datasets


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


def find_markers_csv_and_quantification(his_client, history_id,
                                        check_naive_state=6):
    """ Find two datasets martching `markers.csv` and quantifiation.

    Parameters
    -----------
    his_client: HistoryClient object.
        From `bioblend.galaxy.histories.HistoryClient`.
    history_id: str
        Galaxy history id.
    check_naive_state: int, default=1.
        check whether naive state dataset exists in last number of datasets
        in the history.
    Returns
    --------
    None or tuple of dataset_ids ({quantification}, {markers_csv}).
    """
    contents = his_client.show_history(history_id, contents=True,
                                       deleted=False, types=['dataset'])
    contents = [dataset for dataset in contents if dataset['state'] == 'ok']

    def is_naivestate(dataset_meta) -> bool:
        """ check whether a dataset name in galaxy history is states
        result from cycif.
        """
        name = dataset_meta['name'].lower()
        return 'states' in name and dataset_meta['extension'] == 'png'

    def is_cp_quant(dataset_meta) -> bool:
        """ check whether a dataset name in galaxy history is quantification
        result from cycif.
        """
        name = dataset_meta['name'].lower()
        return 'cp_quant.csv' in name and dataset_meta['extension'] == 'csv'

    def is_s3_quant(dataset_meta) -> bool:
        """ check whether a dataset name in galaxy history is quantification
        result from cycif.
        """
        name = dataset_meta['name'].lower()
        return 's3_quant.csv' in name and dataset_meta['extension'] == 'csv'

    def is_marker_csv(dataset_meta) -> bool:
        """ whether a dataset name in galaxy history is `markers.csv`
        for cycif.

        name: str
            Name of a galaxy dataset.
        """
        name = dataset_meta['name'].lower()
        return 'markers.csv' in name and 'type' not in name \
            and dataset_meta['extension'] == 'csv'

    # naive_state in last 5 datasets
    ns_dataset = [dataset for dataset in contents if dataset['hid']]
    ns_dataset = [dataset for dataset in ns_dataset[-check_naive_state:]
                  if is_naivestate(dataset)]

    if not ns_dataset:
        log.warn("Error: make sure the history is completed successfully! %s."
                 % history_id)
        return

    # find marker.csv
    markers_dataset = [dataset for dataset in contents
                       if is_marker_csv(dataset)]
    if len(markers_dataset) != 1:
        log.warn("Expected one and only one `markers.csv` dataset in the input "
                 "history, but got %d datasets. %s."
                 % (len(markers_dataset), history_id))
        return

    # find cellpose quantification dataset
    cp_quant_dataset = [dataset for dataset in contents
                        if is_cp_quant(dataset)]
    if len(cp_quant_dataset) != 1:
        log.warn("Expected one and only one cellpose quantification dataset in "
                 "the input history, but got %d datasets. %s."
                 % (len(cp_quant_dataset), history_id))
        return

    # find s3 quantification dataset
    s3_quant_dataset = [dataset for dataset in contents
                        if is_s3_quant(dataset)]
    if len(s3_quant_dataset) != 1:
        log.warn("Expected one and only one s3 quantification dataset in the input "
                 "history, but got %d datasets. %s."
                 % (len(s3_quant_dataset), history_id))
        return

    return (cp_quant_dataset[0], s3_quant_dataset[0], markers_dataset[0])


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
        find_markers_csv_and_quantification(his_cli, his['encode_id'])
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
