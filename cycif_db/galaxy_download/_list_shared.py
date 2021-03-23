import logging
import pathlib
import re

from bioblend import galaxy
from datetime import datetime, timedelta
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from ._core import (
    GalaxyDriver,
    download_datasets,
    find_markers_csv_and_quantification,
    find_markers_csv_and_quantification_v2,
    galaxy_client)


log = logging.getLogger(__name__)


class SharedGalaxy(GalaxyDriver):
    """ Operate on `list_shared` histories.
    """
    def __init__(self, browser='Chrome', headless=True, server=None,
                 username=None, password=None, wait_time=10,
                 cutoff_time='2021-02-06',
                 **kwargs) -> None:
        super().__init__(browser=browser, headless=headless, server=server,
                         username=username, password=password, **kwargs)

        self.wait_time = wait_time
        self.cutoff_time = cutoff_time
        self._get_history_rows()

    def _get_history_rows(self, cutoff_time=None):
        if not cutoff_time:
            cutoff_time = self.cutoff_time
        url = self.server + 'histories/list_shared'
        self.driver.get(url)
        log.info("Go to page `histories/list_shared.`")
        htable_element = WebDriverWait(self.driver, self.wait_time).until(
            EC.presence_of_element_located((By.ID, 'grid-table-body')))
        history_rows = htable_element.find_elements(By.XPATH, './/tr')

        history_rows = [row.find_elements(By.XPATH, './/td') for row in history_rows]

        self.history_rows = [row for row in history_rows if
                             SharedGalaxy.is_valid_row(row, cutoff_time=cutoff_time)]
        log.info("Found %d valid history rows." % (len(self.history_rows)))

    @classmethod
    def is_valid_row(cls, tds, min_OKs=5, cutoff_time='2021-01-01'):
        """ check whether a shared history row is valid.

        Parameter
        ---------
        tds: list of WebElement objects ('.//td').
            From selenium.
        cutoff_time: str
            Only time after the cutoff is valid.
        """
        assert len(tds) == 5, ("Expect 5 cols for the shared history row, "
                               "but got %d instead!" % len(tds))

        try:
            n_ok = int(tds[1].find_element(By.CLASS_NAME,
                                           'state-color-ok').text)
        except Exception:
            log.info("This row of history has no state-OK counts: %s" % tds[0].text)
            return False

        if n_ok < min_OKs:
            return False
        try:
            time_str = tds[3].text
            update_time = datetime.strptime(time_str, '%b %d, %Y')
        except ValueError:
            now = datetime.now()
            match = re.match('(?P<days>\d+)\s+day(s)?\s+ago$', time_str, flags=re.I)
            if match:
                days = match.group('days')
                update_time = now - timedelta(float(days))
            else:
                match = re.match(
                    '(?P<hours>\d+)\s+hour(s)?\s+ago$', time_str, flags=re.I)
                if match:
                    hours = match.group('hours')
                    update_time = now - timedelta(hours=float(hours))
                else:
                    log.error("The update time was either invalid or just now!")
                    return False

        cutoff_time = datetime.strptime(cutoff_time, '%Y-%m-%d')
        if update_time < cutoff_time:
            log.info("This row of history is older than the cutoff time `%s`: %s"
                     % (cutoff_time, tds[0].text))
            return False

        return True

    def get_history_names_and_ids(self):
        """ get history ids for valid history rows.

        Returns
        --------
        A list of tuple (history_name, history_id).
        """
        rval = []
        for tds in self.history_rows:
            self.driver.execute_script("arguments[0].scrollIntoView()", tds[0])
            tds[0].click()
            log.info("Click history row %s" % tds[0].text)
            popmenu = WebDriverWait(self.driver, self.wait_time).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'popmenu-wrapper')))
            view_e = popmenu.find_element(By.LINK_TEXT, 'View')
            link = view_e.get_attribute('href')
            his_id = link.split('id=')[1]
            his_name = tds[0].text
            rval.append((his_name, his_id))
            # remove the popmenu
            tds[2].click()

        return rval

    @classmethod
    def get_sample_name(cls, history_name):
        """ Generate sample name from galaxy history name

        Parameters
        ----------
        name: str

        Returns
        --------
        str
        """
        match = re.match('(?P<tag1>\S+)\s+(?P<name>\S+)\s+(?P<tag2>\w+)$',
                         history_name)
        if match:
            name = match.group('name')
            tag1 = match.group('tag1')
            tag2 = match.group('tag2')
            rval = name + '__' + tag1 + '_' + tag2
        else:
            match = re.match('(?P<name>\w+)_(?P<tag>mcmicro_v.+)$',
                             history_name, flags=re.I)
            if match:
                name = match.group('name')
                tag = match.group('tag')
                rval = name + '__' + tag
            else:
                raise Exception("Failed to extract sample name from the history "
                                "name: %s" % history_name)

        log.info(f"Generate sample name `{rval}`.")
        return rval

    def download(self, destinatin, server=None, api_key=None, version='2'):
        gi = galaxy_client(server=server, api_key=api_key)
        his_cli = galaxy.histories.HistoryClient(gi)
        folder = pathlib.Path(destinatin)
        if version == '2':
            _func = find_markers_csv_and_quantification_v2
        else:
            _func = find_markers_csv_and_quantification
        for his_name, his_id in self.get_history_names_and_ids():
            print(_func)
            markers_and_quants = _func(his_cli, his_id, check_naive_state=6)
            if not markers_and_quants:
                continue
            dataset_ids = [dataset['id'] for dataset in markers_and_quants]
            sample_name = SharedGalaxy.get_sample_name(his_name)
            if len(dataset_ids) == 2:
                destination = folder.joinpath(sample_name).absolute()
                try:
                    download_datasets(destination, *dataset_ids, galaxy_client=gi)
                except Exception as e:
                    log.warn(e)
            else:
                cp_dataset_ids = [dataset_ids[0], dataset_ids[2]]
                destination = folder.joinpath(sample_name + '_' + 'cellpose').absolute()
                try:
                    download_datasets(destination, *cp_dataset_ids, galaxy_client=gi)
                except Exception as e:
                    log.warn(e)
                s3_dataset_ids = [dataset_ids[1], dataset_ids[2]]
                destination = folder.joinpath(sample_name + '_' + 's3').absolute()
                try:
                    download_datasets(destination, *s3_dataset_ids, galaxy_client=gi)
                except Exception as e:
                    log.warn(e)
