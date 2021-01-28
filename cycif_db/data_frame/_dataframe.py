""" Utils for linking dataframe to database
"""
import logging
import pandas as pd
import re
from pandas import DataFrame, Index, Series

from ..markers import Markers


log = logging.getLogger(__name__)

HEADER_MARKER_NAME = 'marker_name'
HEADER_SUFFIX_MAPPING = {
    '_+nuclei[\s_-]*masks$': '__nuclei_masks',
    '_+cell[\s_-]*masks$': '__cell_masks',
    '_+cellpose[\s_-]*masks[\s_-]*on[\s_-]*data[\s_-]*\d*$': '__nuclei_masks',
}


class CycDataFrame(object):
    """ Utils relating to cycif quantification data in `pandas.DataFrame`.
    """
    def __init__(self):
        self.stock_markers = Markers()

    def header_to_dbcolumn(self, st):
        """ Map DataFram header to column name in database.
        """
        for k, v in HEADER_SUFFIX_MAPPING.items():
            marker, count = re.subn(k, '', st, flags=re.I)
            if count:
                return self.stock_markers.get_dbname(marker).lower() + v

        return self.stock_markers.get_dbname(st).lower()

    def check_feature_compatibility(self, cells_data, markers_data, **kwargs):
        """ Check whether markers in cells table matches marker names
        listed in markers.csv.

        Parameters
        -----------
        cells_data: str, DataFrame or pandas Index/Series.
            The file path, DataFrame or header of cells quantification data.
        markers_data: str, DataFrame or pandas Index/Series.
            The file path, DataFrame or column data of the markers.csv.

        Returns
        --------
        None or raise ValueError if failed.
        """
        if isinstance(cells_data, str):   # file path to the tabu
            df = pd.read_csv(cells_data, **kwargs)
            cells_data = df.columns
        elif isinstance(cells_data, DataFrame):
            cells_data = cells_data.columns
        elif not isinstance(cells_data, (Index, Series)):
            raise ValueError("Unsupported datatype for `cells`!")

        if isinstance(markers_data, str):   # file path to the tabu
            df = pd.read_csv(markers_data, **kwargs)
            markers_data = df[HEADER_MARKER_NAME]
        elif isinstance(markers_data, DataFrame):
            markers_data = markers_data[HEADER_MARKER_NAME]
        elif not isinstance(markers_data, (Index, Series)):
            raise ValueError("Unsupported datatype for `markers`!")

        markers_in_cells, others = get_headers_categorized(cells_data)
        markers_in_cells = [header_to_marker(mkr) for mkr in markers_in_cells]

        unknown_markers = [mkr for mkr in markers_in_cells
                           if self.stock_markers.get_dbname(mkr) is None]
        unknown_markers = set(unknown_markers)

        unknown_others = [mkr for mkr in others
                          if self.stock_markers.get_dbname(mkr) is None]
        unknown_others = set(unknown_others)

        m_markers = markers_data.map(self.stock_markers.get_dbname)
        unknown_m_markers = []
        for i, mkr in enumerate(markers_data):
            if m_markers[i] is None:
                unknown_m_markers.append(mkr)
        unknown_m_markers = set(unknown_m_markers)

        if unknown_markers or unknown_others or unknown_m_markers:
            message = "Found %d unknown marker(s): %s." \
                % (len(unknown_markers), ', '.join(unknown_markers)) \
                if unknown_markers else ""
            if unknown_others:
                message = message + \
                    "\nFound %d unknown non-marker feature(s): %s."\
                    % (len(unknown_others), ', '.join(unknown_others))
            if unknown_m_markers:
                message = message + \
                    "\nFound %d unknown marker(s) in `markers.csv`: %s."\
                    % (len(unknown_m_markers), ', '.join(unknown_m_markers))
            raise ValueError("The sample data are not compatible with "
                             "database schema! %s" % message)

        m_markers_set = set(m_markers)
        markers_set_in_cells = set(markers_in_cells)
        diff1 = [mkr for mkr in markers_set_in_cells
                 if self.stock_markers.get_dbname(mkr) not in m_markers_set]

        if diff1:
            log.warn(
                "The following markers found in cells headers didn't "
                "match any marker name listed in the `markers.csv`: %s"
                % (', '.join(diff1)))

        log.info("Check DB schema compatibility: Succeed!")


def header_to_marker(header):
    """ Map DataFrame header to conventional name of marker

    Parameters
    ----------
    header: str
    """
    for k in HEADER_SUFFIX_MAPPING:
        rval, count = re.subn(k, '', header, flags=re.I)
        if count:
            return rval

    return header


def is_marker(header):
    """ Check whether a header is marker plus a suffix.
    """
    for k in HEADER_SUFFIX_MAPPING:
        if re.search(k, header, flags=re.I):
            return True
    return False


def get_headers_categorized(data, **kwargs):
    """ Split DataFrame headers into to two list, markers and other features

    Arguments
    ---------
    data: str, DataFrame or pandas Index/Series.
    kwargs: keywords parameters.
        Used `pd.read_csv`. Only relevent when data is str.
    """
    if isinstance(data, str):   # file path to the tabu
        df = pd.read_csv(data, **kwargs)
        headers = df.columns
    elif isinstance(data, DataFrame):
        headers = data.columns
    elif isinstance(data, (Index, Series)):
        headers = data
    else:
        raise ValueError("Unrecognized type for data!")

    markers = [x for x in headers if is_marker(x)]
    others = [x for x in headers if x not in markers]

    return markers, others
