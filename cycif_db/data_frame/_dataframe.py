""" Utils for linking dataframe to database
"""
import logging
import pandas as pd
from pandas import DataFrame, Index, Series

from ..markers import Markers


log = logging.getLogger(__name__)

HEADER_MARKER_NAME = 'marker_name'


class CycDataFrame(object):
    """ Utils relating to cycif quantification data in `pandas.DataFrame`.
    """
    def __init__(self):
        self.stock_markers = Markers()

    def header_to_dbcolumn(self, st):
        """ Map DataFram header to column name in database.
        """
        st = st.lower()

        if st.endswith('_nuclei masks'):
            marker, suffix = st[:-13], '__nuclei_masks'
        elif st.endswith('_nucleimasks'):
            marker, suffix = st[:-12], '__nuclei_masks'
        elif st.endswith('_cell masks'):
            marker, suffix = st[:-11], '__cell_masks'
        elif st.endswith('_cellmasks'):
            marker, suffix = st[:-10], '__cell_masks'
        else:
            marker, suffix = st, ''

        return self.stock_markers.get_dbname(marker) + suffix

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

        unknown_others = [mkr for mkr in others
                          if self.stock_markers.get_dbname(mkr) is None]

        if not unknown_markers and not unknown_others:
            log.info("Check DB schema compatibility: Succeed!")
        else:
            message = "Found %d unknown markers: %s." \
                % (len(unknown_markers), ', '.join(unknown_markers)) \
                if unknown_markers else ""
            if unknown_others:
                message = message + \
                    " Found %d unknown non-marker features: %s."\
                    % (len(unknown_others), ', '.join(unknown_others))
            raise ValueError("The cells data are not compatible with database "
                             "schema! %s" % message)

        m_markers = set(markers_data.map(self.stock_markers.get_dbname))

        markers_set_in_cells = set(markers_in_cells)
        diff1 = [mkr for mkr in markers_set_in_cells
                 if self.stock_markers.get_dbname(mkr) not in m_markers]

        if diff1:
            log.warn(
                "The following markers found in cells headers didn't "
                "match any marker name listed in the `markers.csv`: %s"
                % (', '.join(diff1)))


def header_to_marker(st):
    """ Map DataFrame header to conventional name of marker
    """
    lower = st.lower()
    if lower.endswith(('_nuclei masks')):
        return st[:-13]
    if lower.endswith(('_nucleimasks')):
        return st[:-12]
    if lower.endswith(('_cell masks')):
        return st[:-11]
    if lower.endswith(('_cellmasks')):
        return st[:-10]
    return st


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

    markers = [x for x in headers
               if x.lower().endswith((
                   '_nuclei masks', '_nuclei_masks',
                   '_cell masks', '_cell_masks',
                   '_cellmasks', '_cellmasks'))]
    others = [x for x in headers if x not in markers]

    return markers, others
