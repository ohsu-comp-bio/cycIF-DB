import getpass
import json
import logging
import pathlib

from bioblend import galaxy
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
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


def find_markers_csv_and_quantification(his_client, history_id,
                                        check_naive_state=1):
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

    def is_quantification(dataset_meta) -> bool:
        """ check whether a dataset name in galaxy history is quantification
        result from cycif.
        """
        name = dataset_meta['name'].lower()
        return 'quantification' in name and dataset_meta['extension'] == 'csv'

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

    # find quantification dataset
    quant_dataset = [dataset for dataset in contents
                     if is_quantification(dataset)]
    if len(quant_dataset) != 1:
        log.warn("Expected one and only one quantification dataset in the input "
                 "history, but got %d datasets. %s."
                 % (len(quant_dataset), history_id))
        return
    return (quant_dataset[0], markers_dataset[0])


class GalaxyDriver(object):
    """ Access galaxy content with selenium webdriver
    """
    def __init__(self, browser='Chrome', headless=True, server=None,
                 username=None, password=None, **kwargs) -> None:
        if browser not in ['Chrome', 'Firefox']:
            raise ValueError("The `browser` mush be one of ['Chrom', 'Firefox']!")
        self.browser = browser
        self.headless = headless
        if not server:
            server = get_configs()['galaxy_server']
        if not server.endswith('/'):
            server += '/'
        self.server = server
        self.username = username
        self.kwargs = kwargs

        options = getattr(webdriver, browser+'Options')()
        options.headless = self.headless
        self.driver = getattr(webdriver, browser)(options=options, **kwargs)

        self._login(self.username, password)

    def _login(self, username=None, password=None):
        url = self.server + 'user/login'
        self.driver.get(url)

        if not username:
            username = input("Username / Email Address:")
        if not password:
            password = getpass.getpass()

        self.driver.find_element_by_xpath('//*[@id="login"]/div[1]/input[1]')\
            .send_keys(username)
        self.driver.find_element_by_name('password').send_keys(password + Keys.RETURN)
        log.info(f"Logging for user: {username}.")
