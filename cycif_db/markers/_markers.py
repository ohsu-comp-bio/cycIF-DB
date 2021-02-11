""" Utilies to uniform marker names
"""
import logging
import numpy as np
import pandas as pd
import pathlib

from ..model.mapping import OTHER_FEATHERS


log = logging.getLogger(__name__)

module = pathlib.Path(__file__).absolute().parent
PATH_TO_MARKERS = str(pathlib.Path.joinpath(module, 'markers.tsv'))


class Markers(object):
    """ cycif markers

    Parameters
    -----------
    path_to_markers: str or None.
        The file path to the `markers.json`.
    """
    def __init__(self, path_to_markers=None):
        self.path_to_markers = path_to_markers
        if self.path_to_markers:
            self._path = self.path_to_markers
        else:
            self._path = PATH_TO_MARKERS

        markers_df = pd.read_csv(self._path, sep='\t', dtype=str).fillna('')

        self.unique_keys = ['name', 'fluor', 'anti', 'replicate']

        self._check_duplicate(markers_df)
        self._load_stock_markers()

    def _check_duplicate(self, df):
        """ check marker duplicate.
        """
        # check uniqueness of all stock markers
        duplicate_markers = df.duplicated(subset=self.unique_keys, keep=False)
        if duplicate_markers.any():
            raise Exception("Duplicate markers found in `markers.tsv`: %s"
                            % df[duplicate_markers])
        log.info("Loaded %d unique stock markers." % df.shape[0])
        self.markers_df = df

    def _load_stock_markers(self):
        """ Load `markers.json` into python dictinary and convert to
        alias: db_name format.
        """
        self.markers = {alias.lower().strip(): i for i, v in enumerate(
                        self.markers_df['aliases']) for alias in v.split(',')}
        log.info("Converted to %d pairs of `marker: db_marker`."
                 % len(self.markers))

        other_features = OTHER_FEATHERS
        log.info("Loaded %d unique DB column names for non-marker features."
                 % len(other_features))
        self.other_features = \
            {name.lower(): k for k, v in other_features.items()
             for name in v}

    def get_marker_db_entry(self, marker):
        """ Get database ingestion entry for a marker, support various
        alias names.

        Parameters
        ----------
        marker: str
            The name of a marker, which can be common name or alias.

        Returns
        ----------
        Tuple, or None if the name doesn't exist in the `markers.tsv` file.
        """
        marker = format_marker(marker)

        id = self.markers.get(marker, None)
        if id is None:
            log.warn(f"The marker name `{marker}` was not recognized!")
            return

        rval = tuple(self.markers_df.loc[id, self.unique_keys])
        return rval

    def get_other_feature_db_name(self, name):
        """ Get formatted database name to a non-marker features.

        Parameters
        ----------
        name: str
            The name of a non-marker feature.

        Returns
        ----------
        str, or None if the name doesn't exist self.other_features.
        """
        name = format_marker(name)

        rval = self.other_features.get(name, None)
        if not rval:
            log.warn(f"The feature name `{name}` was not recognized!")
        return rval

    def update_stock_markers(self, new_markers, toplace=None, reload=False):
        """ Update `markers.tsv`.

        Arguments
        ---------
        new_markers: tuple, list or list of lists.
            In (name, fluor, anti, replicate, aliases) format.
        toplace: None or str, default is None.
            The path to save the updated marker dataframe. When toplace
            is None, it's the original path + '.new'.
        reload: boolean, default is False.
            Whether to reload the updated markers/features.
        """
        if not isinstance(new_markers, (list, tuple)):
            raise ValueError("`new_markers` must be list, tuple or list of "
                             "lists datatype!")
        if not isinstance(new_markers[0], (list, tuple)):
            new_markers = [new_markers]

        df = self.markers_df.copy()
        start = df.shape[0]
        for i, mkr in enumerate(new_markers):
            marker_mask = [(df.loc[:, x] == (y or ''))
                           for x, y in zip(self.unique_keys, mkr)]
            marker_mask = np.logical_and.reduce(marker_mask)
            if marker_mask.any():
                for alias in mkr[-1].split(','):
                    if format_marker(alias) not in self.markers:
                        df.loc[marker_mask, 'aliases'] += ', ' + alias
            else:
                df.loc[start+i] = mkr

        self._check_duplicate(df)

        if not toplace:
            toplace = self._path + '.new'

        df.to_csv(toplace, sep='\t', index=False)

        if reload:
            self._path = toplace
            self._load_stock_markers()

        log.info("Marker/Feature list is updated!")


def format_marker(name):
    """ Turn to lowercaes and remove all whitespaces
    """
    rval = name.lower()
    rval = ''.join(rval.split())
    rval = rval.replace('-', '_')
    return rval
