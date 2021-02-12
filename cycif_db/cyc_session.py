""" Main wrapper class that interacts with cycIF_DB
"""
import logging
import pandas as pd
import re

from collections.abc import Iterable
from pandas import DataFrame
from sqlalchemy import func
from sqlalchemy.orm import Session
from .data_frame import CycDataFrame, get_headers_categorized
from .markers import format_marker, Marker_Comparator
from .model import (Cell, Marker, Marker_Alias, Sample,
                    Sample_Marker_Association)
from .utils import engine_maker


log = logging.getLogger(__name__)

HEADER_SUFFIX_MAPPING = {
    '_+nuclei[\s_-]*masks$': '_nu',
    '_+cell[\s_-]*masks$': '_cl',
    '_+cellpose[\s_-]*masks[\s_-]*on[\s_-]*data[\s_-]*\d*$': '_nu',
}


class CycSession(Session):
    """ A sqlalchemy Session subclass

    Parameters
    ----------
    bind: `sqlalchemy.engine.Engine` object or other supported
        object, default=None.
    kwargs: other keywords parameter for Session
    """
    def __init__(self, bind=None, **kwargs):
        if not bind:
            engine = engine_maker()
            bind = engine
        super(CycSession, self).__init__(bind=bind, **kwargs)
        # float precision
        self.decimals = 4

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()

    def load_dataframe_util(self):
        self.data_frame = CycDataFrame()

    def other_feature_to_dbcolumn(self, header):
        """ map none marker header to db column.
        """
        if not hasattr(self, 'data_frame'):
            self.load_dataframe_util()

        rval = self.data_frame.stock_markers.get_other_feature_db_name(header)
        if rval:
            return rval.lower()

        raise ValueError(f'Unrecognized header: `{header}`!')

    def marker_header_to_dbkey(self, header):
        """ map marker header to db json key.
        Suppose the header has valid suffix.
        """
        for k, v in HEADER_SUFFIX_MAPPING.items():
            marker, count = re.subn(k, '', header, flags=re.I)
            if count:
                marker_id = self.get_alias_marker_id(marker)
                assert marker_id
                rval = str(marker_id) + v
                log.info(f"Mapped header `{header}` to `{rval}`!")
                return rval

        raise Exception(f"Unregnized suffix for header: `{header}`!")

    ######################################################
    #              Data Ingestion
    ######################################################
    def add_sample(self, sample):
        """ Add a sample object to samples table.

        Parameters
        -----------
        sample: dict or Sample object.
            Dict to build a Sample object.
        """
        if isinstance(sample, dict):
            sample = Sample(**sample)
        assert isinstance(sample, Sample), \
            "Unsupported datatype for sample!"

        self.add(sample)
        self.flush()
        log.info("Added sample {}.".format(repr(sample)))
        return sample

    def insert_cells_mappings(self, sample_id, cells, **kwargs):
        """ Insert cell quantification data into cells table.

        Parameters
        ----------
        sample_id: int.
            Index of sample object in database.
        cells: str or pandas.DataFrame object.
            If str, it's path string to a csv file.
        kwargs: keywords parameter.
            Addtional parameters used `pd.read_csv`.
        """
        if isinstance(cells, str):
            cells = pd.read_csv(cells, **kwargs)
        elif not isinstance(cells, DataFrame):
            raise ValueError("Unsupported datatype for cells!")

        markers, others = get_headers_categorized(cells.columns)

        cells_marker = cells.loc[:, markers]
        cells_marker.columns = cells_marker.columns.map(
            self.marker_header_to_dbkey)
        cells_marker = cells_marker.round(decimals=self.decimals)
        marker_obs = cells_marker.to_dict('records')

        cells_other = cells.loc[:, others]
        cells_other.columns = cells_other.columns.map(
            self.other_feature_to_dbcolumn)
        cells_other['sample_id'] = sample_id
        cell_obs = cells_other.to_dict('records')
        for idx, ob in enumerate(cell_obs):
            ob['features'] = marker_obs[idx]

        self.bulk_insert_mappings(Cell, cell_obs)
        if not self.autoflush:
            self.flush()
        log.info("Added %d cell records." % len(cell_obs))

    def add_marker(self, marker):
        """ Add marker object

        Parameter
        --------
        marker: dict or Marker object

        Return
        ------
        Marker object or None
        """
        if isinstance(marker, dict):
            marker = Marker(**marker)
        if not isinstance(marker, Marker):
            raise ValueError("%s was not a supported datatype for marker!"
                             % type(marker))
        self.add(marker)
        self.flush()
        assert marker.id
        log.info("Added marker {}.".format(repr(marker)))
        return marker

    def insert_or_sync_markers(self):
        """ Sync stock markers in `markers.tsv` with database.
        """
        if not hasattr(self, 'data_frame'):
            self.load_dataframe_util()

        markers_df = self.data_frame.stock_markers.markers_df
        try:
            for marker in markers_df.to_dict('records'):
                aliases = marker.pop('aliases')
                marker_id = self.get_or_create_marker(marker).id
                for alias in aliases.split(','):
                    obj = self.query(Marker_Alias).filter(
                        func.lower(Marker_Alias.name) == format_marker(alias))\
                        .first()
                    if not obj:
                        alias = Marker_Alias(name=alias.strip(),
                                             marker_id=marker_id)
                        self.add(alias)
                        log.info("Added marker alias %s" % repr(alias))
                    elif obj.marker_id != marker_id:
                        obj.marker_id = marker_id
                        log.info("Updated marker_id to %s for %s."
                                 % (marker_id, repr(obj)))
                    self.flush()
            self.commit()
        except Exception:
            self.rollback()
            raise
        log.info("Insert or Sync stock markers completed!")

    def insert_sample_markers(self, sample_id, markers, **kwargs):
        """ Insert sample marker association into database.

        Parameters
        ----------
        sample_id: int.
            Index of sample object in database.
        markers: str or pandas.DataFrame object.
            If str, it's path string to a csv file.
        kwargs: keywords parameter.
            Addtional parameters used `pd.read_csv`.

        Returns
        ----------
        None.
        """
        if isinstance(markers, str):
            markers = pd.read_csv(markers, **kwargs)
        elif not isinstance(markers, DataFrame):
            raise ValueError("Unsupported datatype for markers!")

        associates = []
        for i, row in markers.iterrows():
            marker_id = self.get_alias_marker_id(row['marker_name'])
            asso = {
                'sample_id': sample_id,
                'marker_id': marker_id,
                'channel_number': row['channel_number'],
                'cycle_number': row['cycle_number']
            }
            associates.append(asso)

        self.bulk_insert_mappings(Sample_Marker_Association, associates)
        self.flush()
        log.info("Added %d entries of sample marker association!"
                 % len(associates))

    def add_sample_complex(self, sample, cells, markers,
                           dry_run=False, **kwargs):
        """ Insert the quantification result from a single sample
        into database, including cell quantification table and
        marker list table.

        Parameters
        ----------
        sample: dict or Sample object.
            Dict to build a Sample object.
        cells: str or pandas.DataFrame object.
            If str, it's path string to a csv file.
        markers: str or pandas.DataFrame object.
            If str, it's path string to a csv file.
        dry_run: bool, default is False.
            Whether to run the sample adding without commit.
        kwargs: keywords parameter.
            Addtional parameters used `pd.read_csv`.
        """
        # check schema compatibility and marker consistency
        if not hasattr(self, 'data_frame'):
            self.load_dataframe_util()
        self.data_frame.check_feature_compatibility(cells, markers)

        assert self.get_sample_id(sample) is None,\
            ("This sample couldn't be added to database because it's "
             "against the unique constraint or it has invalid `id`!")
        try:
            sample = self.add_sample(sample)
            self.insert_cells_mappings(sample.id, cells, **kwargs)
            self.insert_sample_markers(sample.id, markers, **kwargs)
            if not dry_run:
                self.commit()
            else:
                self.rollback()
            log.info("Adding sample complex completed!")
        except Exception:
            self.rollback()
            raise

    ###################################################
    #              Data Removal
    ###################################################
    def delete_sample(self, id=None, name=None, tag=None):
        """ Remove a sample and its related records from database

        Parameters
        ----------
        id: int, default is None.
            The index id in `samples` table.
        name: str, default is None.
            The name of the sample.
        tag: str, default is None.
            The tag of the sample.
        """
        if id is not None:
            self.query(Sample).filter_by(id=id).delete()
        else:
            assert name and isinstance(name, str), \
                "Argument `name` must be a valid string!"
            self.query(Sample)\
                .filter(func.lower(Sample.name) == name.lower())\
                .filter((Sample.tag == tag)
                        | (func.lower(Sample.tag) == str(tag).lower()))\
                .delete()

        self.commit()

    def delete_marker(self, id=None, name=None):
        """ Remove a marker and its related records from database

        Parameters
        ----------
        id: int, default is None.
            The index id in `markers` table.
        name: str, default is None.
            The unique name of an marker.
        """
        if id is not None:
            if not isinstance(id, int):
                raise ValueError("Invalid datatype for `id`")
            self.query(Marker).filter_by(id=id).delete()
        else:
            if not isinstance(name, str):
                raise ValueError("Invalid argument datatype!")
            self.query(Marker).filter_by(name=name).delete()

        self.commit()

    def delete_all(self):
        """ Remove all records in all tables in the database.
        """
        self.query(Sample).delete()
        self.query(Marker).delete()
        self.query(Cell).delete()
        self.query(Sample_Marker_Association).delete()

        self.commit()

    ###################################################
    #              Data update
    ###################################################
    def update_sample_feature_list(self, sample, cells, **kwargs):
        """ Standalone util to update feature list for a sample.

        Parameters
        ----------
        sample: dict or Sample object.
        cells: str or pandas.DataFrame object.
            If str, it's path string to a csv file.
        kwargs: keywords parameter.
            Addtional parameters used `pd.read_csv`.
        """
        if isinstance(cells, str):
            cells = pd.read_csv(cells, **kwargs)
        elif not isinstance(cells, DataFrame):
            raise ValueError("Unsupported datatype for cells!")

        if isinstance(sample, Sample):
            name = sample.name
            tag = sample.tag
        elif isinstance(sample, dict):
            name = sample['name']
            tag = sample.get('tag', None)
        else:
            raise ValueError("Unsupported data type for `sample`!")

        if not hasattr(self, 'data_frame'):
            self.load_dataframe_util()
        cells.columns = cells.columns.map(self.data_frame.header_to_dbcolumn)
        feature_list = ','.join(list(cells.columns))

        query = self.query(Sample)\
            .filter(func.lower(Sample.name) == name.lower())\
            .filter((Sample.tag == tag)
                    | (func.lower(Sample.tag) == str(tag).lower()))\

        if not query.first():
            raise Exception("Update database failed. No matching sample "
                            "was found!")

        try:
            query.update(dict(feature_list=feature_list),
                         synchronize_session=False)
            self.commit()
        except Exception:
            self.rollback()
            raise

        log.info("Update feature list for sample `%s`: %s"
                 % (sample, feature_list))

    ###################################################
    #              Data Query
    ###################################################
    def get_alias_marker_id(self, alias):
        """ get marker_id for a marker alias.

        Parameters
        ----------
        alias: str.

        Returns
        --------
        Int or None.
        """
        marker_id = self.query(Marker_Alias.marker_id) \
            .filter(func.lower(Marker_Alias.name) == format_marker(alias)) \
            .scalar()

        return marker_id

    def get_or_create_marker(self, marker):
        """ Fetch a Marker object from markers table.
        if fails, create one instead.

        Parameters
        ----------
        marker_name: dict.
            The marker to query.

        Returns
        ----------
        An object of Marker.
        """
        marker_obj = self.get_marker_by_name(marker)
        if marker_obj:
            log.info("Successfully fetched a martching marker %s."
                     % repr(marker))
        else:
            log.info("No matching marker found for %s. Create one instead..."
                     % marker)
            marker_obj = self.add_marker(marker)

        return marker_obj

    def get_samples_by_name(self, sample_name):
        """ Query samples by name.

        Parameters
        ----------
         sample_name: str.
            Sample name.

        Returns
        -------
        A list of `Sample` objects or None.
        """
        samples = self.query(Sample).filter(
            func.lower(Sample.name) == sample_name.lower())
        return samples

    def get_sample_id(self, sample):
        """ Get unique sample ID in database.

        Parameters
        ----------
        sample: dict or Sample object.

        """
        if isinstance(sample, Sample):
            sample_id = sample.id
            if sample_id is not None:
                return sample_id
            else:
                name = sample.name
                tag = sample.tag or ''
        elif isinstance(sample, dict):
            name = sample.get('name')
            tag = sample.get('tag', None)
            assert name and isinstance(name, str), \
                "The sample name must be a valid string!"
        else:
            raise ValueError("Unsupported data type for `sample`.")

        query = self.query(Sample.id)\
            .filter(func.lower(Sample.name) == name.lower())\
            .filter((Sample.tag == tag)
                    | (func.lower(Sample.tag) == str(tag).lower()))\
            .first()

        if query:
            return query[0]

        log.info("This database has no matching record for sammple=`{}`,"
                 " name=`{}` and tag=`{}`!".format(sample, name, tag))

    def get_marker_by_name(self, marker):
        """ Query marker.

        Parameters
        ----------
        marker_name: dict
            Marker query info.

        Returns
        -------
        Marker object or None.
        """
        name = marker.get('name')
        fluor = marker.get('fluor', '') or ''
        anti = marker.get('anti', '') or ''
        duplicate = marker.get('duplicate', '') or ''

        marker = self.query(Marker) \
            .filter(func.lower(Marker.name) == name.lower()) \
            .filter((Marker.fluor == fluor)
                    | (func.lower(Marker.fluor) == fluor.lower())) \
            .filter((Marker.anti == anti)
                    | (func.lower(Marker.anti) == anti.lower())) \
            .filter((Marker.duplicate == duplicate)
                    | (func.lower(Marker.duplicate) == duplicate.lower())) \
            .first()
        return marker

    def list_samples(self, detailed=False):
        """ List all the samples stored in database.

        Returns
        -------
        List of Sample objects or None.
        """
        sample_list = self.query(Sample).all()
        if detailed:
            sample_list = [item.__dict__ for item in sample_list]
        return sample_list

    def list_markers(self, detailed=False):
        """ List all the markers stored in database.

        Parameters
        ----------
        detailed: bool, default is False.
            If True, return a dict for each marker.

        Returns
        -------
        List of Marker objects or None.
        """
        marker_list = self.query(Marker).all()
        if detailed:
            marker_list = [item.__dict__ for item in marker_list]
        return marker_list

    def get_sample(self, id=None, name=None, tag=None):
        """ get a Sample object

        Parameters
        ----------
        id: int or None.
            Index of sample in database.
            Ignoring `name` and `tag` if this one is provided.
        name: str or None.
            Name of sample, ignoring cases. One of `id` and `name` must
            be provided.
        tag: str or None.
            Tag of the sample, ignoring cases.

        Returns
        -------
        Sample object.
        """
        if not isinstance(id, (int, None)):
            raise ValueError("Invalid `id` was provided. The argument "
                             "must be int or None!")
        if isinstance(id, int):
            sample = self.query(Sample).filter_by(id=id).first()
        elif name:
            sample = self.query(Sample) \
                .filter(func.lower(Sample.name) == name.lower()) \
                .filter((Sample.tag == tag)
                        | (func.lower(Sample.tag) == str(tag).lower())) \
                .first()
        else:
            raise ValueError("Neither `id` nor `name` was provided!")

        log.info(f"Retrived sample: {sample}!")

    def get_cells_for_sample(self, sample=None, name=None, tag=None,
                             to_path=None, **kwargs):
        """ Retrieve all cells for a sample and convert to pandas DataFrame.

        Parameters
        ----------
        sample: `Sample` object or int.
            If int, it's the index of sample in database.
            Ignoring `name` and `tag` if this one is provided.
        name: str or None.
            Name of the sample, ignoring cases. One of `sample` and `name` must
            be provided.
        tag: str or None.
            Tag of the sample, ignoring cases.
        to_path: str, default is None.
            If provided, this is the path to save the cells data.
        kwargs: Key words arguments
            Used in pandas dataframe `to_csv`.

        Returns
        -------
        pandas DataFrame object.
        """
        if not isinstance(sample, (int, Sample)):
            raise ValueError("The argument `sample` was provided, but it "
                             "was not a valid Sample object!")
        if not isinstance(sample, Sample):
            sample = self.get_sample(id=sample, name=name, tag=tag)

        assert sample, ("No matching record found for the sample!")

        feature_list = sample.feature_list.split(',')
        feature_list = sorted(feature_list, key=column_sort_key)
        feature_columns = [getattr(Cell, col) for col in feature_list]

        data = self.query(Sample.name, Sample.tag, *feature_columns)\
            .join(Sample) \
            .filter(Cell.sample_id == sample.id).all()

        df = pd.DataFrame(
            data,
            columns=['sample_name', 'sample_tag']+feature_list)

        if to_path:
            df.to_csv(to_path, **kwargs)

        return df

    def get_cells_from_samples(self, samples=None, names=None, tags=None,
                               column_filter='intersection', to_path=None,
                               **kwargs):
        """ Retrieve all cells data for a list of samples and convert to
            pandas DataFrame.

        Parameters
        ----------
        samples: iterable of `Sample` objects or ints.
            If int, these are the indices of samples in database.
            Ignoring `name` and `tag` if this one is provided.
        names: list/tuple of str or None.
            Name of the sample, ignoring cases. One of `sample` and `names`
            must be provided.
        tags: list/tuple of str or None.
            Tag of the sample, ignoring cases.
        column_filter: str
            One of ['intersection', 'union'].
        to_path: str, default is None.
            If provided, this is the path to save the cells data.
        kwargs: Key words arguments
            Used in pandas dataframe `to_csv`.

        Returns
        -------
        pandas DataFrame object.
        """
        if not isinstance(samples, (Iterable, None)):
            raise ValueError("The samples provided, `{samples}`, are not "
                             "iterable or None.")

        if column_filter not in ('intersection', 'union'):
            raise ValueError("Argument `column_filter` must be one of "
                             "['intersection', 'union'], but got "
                             "`{}`!".format(column_filter))

        if samples:
            if isinstance(samples[0], int):
                samples = [self.get_sample(id) for id in samples]
            elif not isinstance(samples[0], Sample):
                raise ValueError(
                    "The element of `samples` must be either int or Sample "
                    "object, but got `{samples[0]}`!")
        elif names:
            if not isinstance(names, (list, tuple)):
                raise ValueError("The argument `names` requires list or tuple "
                                 "data type! `{names}` was not valid!")
            if not tags:
                tags = [None]
            if len(tags) < len(names):
                tags.extend([None] * (len(names) - len(tags)))
            samples = [self.get_sample(name=name, tag=tag)
                       for name, tag in zip(names, tags)]
        else:
            raise ValueError("One of the `samples` and `names` must be "
                             "provided!")

        feature_lists = [sample.feature_list.split(',') for sample in samples]
        feature_lists = [set(x) for x in feature_lists]
        if column_filter == 'intersection':
            feature_list = set.intersection(*feature_lists)
        else:
            feature_list = set.union(*feature_lists)

        feature_list = sorted(list(feature_list), key=column_sort_key)

        feature_columns = [getattr(Cell, col) for col in feature_list]
        sample_ids = [sample.id for sample in samples]

        data = self.query(Sample.name, Sample.tag, *feature_columns)\
            .join(Sample) \
            .filter(Cell.sample_id.in_(sample_ids)).all()

        df = pd.DataFrame(
            data,
            columns=['sample_name', 'sample_tag']+feature_list)

        if to_path:
            df.to_csv(to_path, **kwargs)

        return df


def column_sort_key(column):
    """ util for sort columns.
    """
    if column.endswith('_id'):
        return '0' + column
    if column.endswith('_masks'):
        return '1' + column
    return column


class DB_Key(object):
    def __init__(self, session, key, fluor_sensitive=True,
                 anti_sensitive=False, keep_duplicates='keep') -> None:
        self.session = session
        self.key = key
        marker_id, mask_type = self.key.split('_')
        self.marker_id = int(marker_id)
        if mask_type == 'cl':
            self.mask_type = 'cell_masks'
        elif mask_type == 'nu':
            self.mask_type = 'nuclei_masks'
        else:
            raise ValueError(f"Unrecognized dabase json key: {key}!")
        marker = self.session.query(Marker).get(self.marker_id)
        self.marker_comparator = Marker_Comparator(
            marker, fluor_sensitive=fluor_sensitive,
            anti_sensitive=anti_sensitive,
            keep_duplicates=keep_duplicates)

    def __repr__(self) -> str:
        return f"<DB_Key('{self.key}')>"

    def to_header(self) -> str:
        return repr(self.marker_comparator) + '__' + self.mask_type

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DB_Key):
            return False

        return self.marker_comparator == other.marker_comparator \
            and self.mask_type == other.mask_type

    def __hash__(self) -> int:
        return self.marker_comparator.__hash__() + hash(self.mask_type)
