""" Utilies to uniform marker names
"""
import json
import logging
import pathlib


log = logging.getLogger(__name__)

module = pathlib.Path(__file__).absolute().parent
PATH_TO_MARKERS = str(pathlib.Path.joinpath(module, 'markers.json'))


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

        self._load_stock_markers()

    def _load_stock_markers(self):
        """ Load `markers.json` into python dictinary and convert to
        alias: db_name format.
        """
        with open(self._path, 'r') as fp:
            markers_n_features = json.load(fp)

        markers = markers_n_features['markers']
        log.info("Loaded %d unique DB column names for markers."
                 % len(markers))
        self.markers = {name.lower(): k for k, v in markers.items()
                        for name in v}
        log.info("Converted to %d pairs of `db_keyword: marker_name`."
                 % len(self.markers))

        other_features = markers_n_features['other_features']
        log.info("Loaded %d unique DB column names for non-marker features."
                 % len(other_features))
        self.other_features = \
            {name.lower(): k for k, v in other_features.items()
             for name in v}

    def get_dbname(self, name):
        """ Get the name used in database for a marker, support various
        alias names.

        Parameters
        ----------
        name: str
            The name of a marker, which can be common name or alias.

        Returns
        ----------
        str, or None if the name doesn't exist in the `markers.json` file.
        """
        lower_name = name.lower()
        # remove all white spaces
        lower_name = ''.join(lower_name.split())
        try:
            return self.markers[lower_name]
        except KeyError:
            try:
                return self.other_features[lower_name]
            except KeyError:
                log.warn(f"The marker name `{name}` was not recognized!")
                return None

    def update_stock_markers(self, new_markers, new_others=None,
                             toplace=None, reload=False):
        """ Check whether a cycIF quantification result is compatible
        with database.

        Arguments
        ---------
        new_markers: dict.
            In {db_keywords: marker, ...} format.
        new_others: None or dict, default is None.
            In {db_keywords: marker, ...} format.
        toplace: None or str, default is None.
            The path to save the updated marker/feature list. When toplace
            is None, it's the original path + '.new'.
        reload: boolean, default is False.
            Whether to reload the updated markers/features.
        """
        if not isinstance(new_markers, dict):
            raise ValueError("`new_markers` must be dict datatype instead!")
        if not new_others:
            new_others = {}
        if not isinstance(new_others, dict):
            raise ValueError("The datatype of `new_others` is not supported!")

        with open(self._path, 'r') as fp:
            markers_n_features = json.load(fp)

        for k, v in new_markers.items():
            if k in markers_n_features['markers']:
                if isinstance(v, list):
                    markers_n_features['markers'][k].extend(v)
                else:
                    markers_n_features['markers'][k].append(v)
            else:
                if isinstance(v, list):
                    markers_n_features['markers'][k] = v
                else:
                    markers_n_features['markers'][k] = [v]

        for k, v in new_others.items():
            if k in markers_n_features['other_features']:
                if isinstance(v, list):
                    markers_n_features['other_features'][k].extend(v)
                else:
                    markers_n_features['other_features'][k].append(v)
            else:
                if isinstance(v, list):
                    markers_n_features['other_features'][k] = v
                else:
                    markers_n_features['other_features'][k] = [v]

        if not toplace:
            toplace = self._path + '.new'

        with open(toplace, 'w') as fp:
            json.dump(markers_n_features, fp, indent=4, sort_keys=True)

        if reload:
            self._path = toplace
            self._load_stock_markers()

        log.info("Marker/Feature list is updated!")
