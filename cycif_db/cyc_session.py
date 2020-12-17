""" Main wrapper class to to interact with cycIF_DB
"""
import logging
import pandas as pd

from pandas import DataFrame
from sqlalchemy import func
from sqlalchemy.orm import Session
from .data_frame import header_to_dbcolumn, check_feature_compatiblity
from .model import Cell, Marker, Sample, Sample_Marker_Association
from .utils import engine_maker


log = logging.getLogger(__name__)


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

    @property
    def url(self):
        return self.session.bind.url

    ######################################################
    #              Data Entry
    ######################################################
    def add_sample(self, sample):
        """ Add a sample object to samples table.

        Parameters
        -----------
        sample: str, dict or Sample object.
            Sample name or dict to build a Sample object.

        Returns
        ----------
        An object of Sample.
        """
        if isinstance(sample, str):
            sample = Sample(name=sample)
        elif isinstance(sample, dict):
            sample = Sample(**sample)
        elif not isinstance(sample, Sample):
            raise ValueError("Unsupported datatype for sample!")

        self.add(sample)
        if not self.autoflush:
            self.flush()
        sample = self.get_sample_by_name(sample.name)
        assert sample
        log.info("Added sample {}.".format(repr(sample)))
        return sample

    def add_cells(self, sample, cells, **kwargs):
        """ Insert cell quantification data into cells table.

        Parameters
        ----------
        sample: Sample object.
            The parent of cells.
        cells: str or pandas.DataFrame object.
            If str, it's path string to a csv file.
        kwargs: keywords parameter.
            Addtional parameters used `pd.read_csv`.

        Returns
        ----------
        None.
        """
        if isinstance(cells, str):
            cells = pd.read_csv(cells)
        elif not isinstance(cells, DataFrame):
            raise ValueError("Unsupported datatype for cells!")

        cells.columns = cells.columns.map(header_to_dbcolumn)
        cell_obs = cells.to_dict('records')
        for ob in cell_obs:
            ob['sample_id'] = sample.id
        self.bulk_insert_mappings(Cell, cell_obs)
        if not self.autoflush:
            self.flush()
        log.info("Added %d cell records." % len(cell_obs))

    def add_marker(self, marker):
        """ Insert one marker object into markers table.

        Parameters
        -----------
        marker: str, dict or Marker object.
            Marker name (string) or dict to build a Marker object.

        Returns
        ----------
        An object of Marker.
        """
        if isinstance(marker, str):
            marker = Marker(name=marker)
        elif isinstance(marker, dict):
            marker = Marker(**marker)
        elif not isinstance(marker, Marker):
            raise ValueError("Unsupported datatype for sample!")

        self.add(marker)
        if not self.autoflush:
            self.flush()
        marker = self.get_marker_by_name(marker.name)
        assert marker
        log.info("Added marker {}.".format(repr(marker)))
        return marker

    def get_or_create_marker_by_name(self, marker_name):
        """ Fetch a Marker object by name from markers table.
        if fails, create one instead.

        Parameters
        ----------
        marker_name: str.
            The name of marker to query.

        Returns
        ----------
        An object of Marker.
        """
        marker = self.get_marker_by_name(marker_name)
        if marker:
            log.info("Successfully fetch a martching marker %s."
                     % repr(marker))
        else:
            log.info(f"No matching marker found for `{marker_name}`. Create "
                     "one instead...")
            marker = self.add_marker(marker_name)

        return marker

    def add_sample_markers(self, sample, markers):
        """ Insert sample marker association into database.

        Parameters
        ----------
        sample: Sample object.
            The parent of cells.
        markers: str or pandas.DataFrame object.
            If str, it's path string to a csv file.
        kwargs: keywords parameter.
            Addtional parameters used `pd.read_csv`.

        Returns
        ----------
        None.
        """
        if isinstance(markers, str):
            markers = pd.read_csv(markers)
        elif not isinstance(markers, DataFrame):
            raise ValueError("Unsupported datatype for markers!")

        associates = []
        for i, row in markers.iterrows():
            marker = self.get_or_create_marker_by_name(row['marker_name'])
            asso = {
                'sample_id': sample.id,
                'marker_id': marker.id,
                'channel_number': row['channel_number'],
                'cycle_number': row['cycle_number']
            }
            associates.append(asso)

        self.bulk_insert_mappings(Sample_Marker_Association, associates)
        if not self.autoflush:
            self.flush()
        log.info("Added %d records of sample marker association!"
                 % len(associates))

    def add_sample_complex(self, sample, cells, markers, **kwargs):
        """ Insert the quantification result from a single sample
        into database, including cell quantification table and
        marker list table.

        Parameters
        ----------
        sample: str, dict or Sample object.
            Sample name or dict to build a Sample object.
        cells: str or pandas.DataFrame object.
            If str, it's path string to a csv file.
        markers: str or pandas.DataFrame object.
            If str, it's path string to a csv file.
        kwargs: keywords parameter.
            Addtional parameters used `pd.read_csv`.
        """
        # check schema compatibility
        check_feature_compatiblity(cells)

        try:
            sample = self.add_sample(sample)
            self.add_cells(sample, cells, **kwargs)
            self.add_sample_markers(sample, markers, **kwargs)
            self.commit()
            log.info("Adding sample complex completed!")
        except Exception:
            self.rollback()
            raise

    ###################################################
    #              Data Qquery
    ###################################################
    def get_sample_by_name(self, sample_name):
        """ Query  samples by name.

        Parameters
        ----------
         sample_name: str.
            Sample name.

        Returns
        -------
        None or an object of Sample.
        """
        sample = self.query(Sample).filter_by(name=sample_name).first()
        return sample

    def get_marker_by_name(self, marker_name):
        """ Query markers by name.

        Parameters
        ----------
        marker_name: str.
            Marker name.

        Returns
        -------
        None or an object of Marker.
        """
        # TODO Make header_to_dbcolumn query.
        marker = self.query(Marker).filter(
            func.lower(Marker.name) == marker_name.lower()).first()
        return marker
