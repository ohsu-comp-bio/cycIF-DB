"""
Data model classes

"""
import json
import logging
import os

from migrate.versioning import repository, schema
from sqlalchemy import Column, create_engine, ForeignKey, Index, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import (
    Boolean,
    DateTime,
    Integer,
    Numeric,
    String,
)


log = logging.getLogger(__name__)

MARKERS_PATH = os.path.join(os.path.dirname(__file__), 'markers.json')
with open(MARKERS_PATH, 'r') as fp:
    KNOWN_MARKERS = json.load(fp)

Base = declarative_base()


class Sample(Base):
    __tablename__ = 'samples'

    id = Column(Integer, primary_key=True)
    name = Column(String)

    def __repr__(self):
        return "<Sample(name='{}')>".format(self.name)


class Marker(Base):
    __tablename__ = 'markers'

    id = Column(Integer, primary_key=True)
    name = Column(String)

    def __repr__(self):
        return "<Marker(name='{}')>".format(self.name)


class Sample_Marker_Association(Base):
    __tablename__ = 'sample_marker_association'

    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey("samples.id"))
    marker_id = Column(Integer, ForeignKey("markers.id"))
    channel_num = Column(Integer)
    cycle_num = Column(Integer)

    sample = relationship("samples", back_populates="markers")
    marker = relationship("markers", back_populates="samples")

    def __repr__(self):
        return "<Sample_Marker_Association(sample={}, marker={})>"\
            .format(self.sample, self.marker)


class Cells(Base):
    __tablename__ = 'cells'

    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey("samples.id"))

    sample_cell_id = Column(Integer)     # local experiment ID
    area = Column(Integer)
    eccentricity = Column(Numeric(15, 0))
    extent = Column(Numeric(15, 0))
    majoraxislength = Column(Numeric(15, 0))
    minoraxislength = Column(Numeric(15, 0))
    orientation = Column(Numeric(15, 0))
    solidity = Column(Numeric(15, 0))
    x_centroid = Column(Numeric(15, 0))
    y_centroid = Column(Numeric(15, 0))
    row_centroid  = Column(Numeric(15, 0))
    column_centroid = Column(Numeric(15, 0))

    sample = relationship("samples", back_populates="cells")

    def __repr__(self):
        return "<Cell(sample={}, cellID={})>"\
            .format(self.sample, self.sample_cell_id)


class Cell_Cell_Masks(Base):
    __tablename__ = 'cell_cell_masks'

    id = Column(Integer, primary_key=True)
    cell_id = Column(Integer, ForeignKey("cells.id"))

    cell = relationship("cells", back_populates="cell_masks")

    def __repr__(self):
        return "<Cell_Cell_Masks(Cell={}>".format(self.cell)


class Cell_Nuclei_Masks(Base):
    __tablename__ = 'cell_nuclei_masks'

    id = Column(Integer, primary_key=True)
    cell_id = Column(Integer, ForeignKey("cells.id"))

    cell = relationship("cells", back_populates="nuclei_masks")

    def __repr__(self):
        return "<Cell_Nuclei_Masks(Cell={}>".format(self.cell)


for mkr in KNOWN_MARKERS['markers']:
    mkr.replace('-', '_')
    mkr = mkr.upper()
    setattr(Cell_Cell_Masks, mkr, Column(Numeric(15, 0)))     # Cautious of case sensitivity
    setattr(Cell_Nuclei_Masks, mkr, Column(Numeric(15, 0)))


def init(engine):
    Base.metadata.create_all(engine)
