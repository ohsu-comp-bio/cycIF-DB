""" Utils for linking dataframe to database
"""
import json
import logging
import pathlib
import pandas as pd
from pandas import DataFrame, Index, Series


log = logging.getLogger(__name__)

module = pathlib.Path(__file__).absolute().parent.parent
PATH_TO_MARKERS = str(pathlib.Path.joinpath(module, 'markers.json'))


def header_to_dbcolumn(st):
    """ Map DataFram header to column name in cells table in database
    """
    st = st.lower()
    st = st.replace('.', '_').replace('-', '_')
    if st.endswith('_nuclei masks'):
        st = st[:-13] + '__nuclei_masks'
    elif st.endswith('_cell masks'):
        st = st[:-11] + '__cell_masks'
    elif st == 'cellid':
        st = 'sample_cell_id'

    return st


def header_to_marker(st):
    """ Map DataFrame header to conventional name of marker
    """
    if st.lower().endswith(('_nuclei masks')):
        return st[:-13]
    if st.lower().endswith(('_cell masks')):
        return st[:-11]
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
                   '_cell masks', '_cell_masks'))]
    others = [x for x in headers if x not in markers]

    return markers, others


def check_feature_compatiblity(data, update=False, toplace=None, **kwargs):
    """ Check whether a cycIF quantification result is compatible
    with database.

    Arguments
    ---------
    data: str, DataFrame or pandas Index/Series.
    update: bool, default=False.
        Whether to include incompatible markers/features into the marker json
        file, if found.
    toplace: None or str, Default=None.
        The path to save the updated marker/feature list. When toplace
        is None, it's the original path + '.new'.
    kwargs: keywords parameters.
        Used `pd.read_csv`. Only relevent when data is `str`.

    Returns
    -------
    Boolean if update is False, otherwise `None`.
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

    markers, others = get_headers_categorized(headers)
    markers = [header_to_marker(x) for x in markers]

    with open(PATH_TO_MARKERS, 'r') as fp:
        features_json = json.load(fp)

    cur_markers = features_json['markers']
    cur_markers = [header_to_dbcolumn(x) for x in cur_markers]
    cur_others = features_json['other_features']
    cur_others = [header_to_dbcolumn(x) for x in cur_others]

    new_markers = [x for x in markers
                   if header_to_dbcolumn(x) not in cur_markers]
    new_others = [x for x in others
                  if header_to_dbcolumn(x) not in cur_others]

    if not new_markers and not new_others:
        log.info("The sample complex is compatible with database schema!")
        return True

    log.info("Found {} new markers. They are {}.".format(
        len(new_markers), ', '.join(new_markers)))
    log.info("Found {} new non-marker features. They are {}.".format(
        len(new_others), ', '.join(new_others)))

    if not update:
        raise ValueError("The sample cells are not supported by database "
                         "schema!")

    if not toplace:
        toplace = PATH_TO_MARKERS + '.new'

    if new_markers:
        new_marker_list = features_json['markers']
        new_marker_list.extend(new_markers)
        features_json['markers'] = sorted(new_marker_list, key=str.casefold)
    if new_others:
        new_other_list = features_json['other_features']
        new_other_list.extend(new_others)
        features_json['other_features'] = sorted(new_other_list,
                                                 key=str.casefold)

    with open(toplace, 'w') as fp:
        json.dump(features_json, fp)
    log.info("Marker/Feature list is updated!")
