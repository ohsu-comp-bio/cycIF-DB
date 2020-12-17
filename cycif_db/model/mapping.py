"""
Data model classes

"""
import json
import logging
import os

from sqlalchemy import Column, create_engine, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import (
    Boolean,
    DateTime,
    Integer,
    Numeric,
    String,
)
from ..data_frame import header_to_dbcolumn


log = logging.getLogger(__name__)

MARKERS_PATH = os.path.join(os.path.dirname(__file__),
                            os.pardir, 'markers.json')
with open(MARKERS_PATH, 'r') as fp:
    KNOWN_MARKERS = json.load(fp)

Base = declarative_base()


class Sample(Base):
    __tablename__ = 'samples'

    id = Column(Integer, primary_key=True)
    name = Column(String)

    cells = relationship('Cell', back_populates='sample')
    markers = relationship('Sample_Marker_Association',
                           back_populates='sample')

    def __repr__(self):
        return "<Sample(name='{}')>".format(self.name)


Index('ix_sample_name', Sample.name, unique=True)


class Marker(Base):
    __tablename__ = 'markers'

    id = Column(Integer, primary_key=True)
    name = Column(String)

    samples = relationship('Sample_Marker_Association',
                           back_populates='marker')

    def __repr__(self):
        return "<Marker(name='{}')>".format(self.name)


Index('ix_marker_name', Marker.name, unique=True)


class Sample_Marker_Association(Base):
    __tablename__ = 'sample_marker_association'

    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey("samples.id", ondelete="CASCADE"))
    marker_id = Column(Integer, ForeignKey("markers.id", ondelete="CASCADE"))
    channel_number = Column(Integer)
    cycle_number = Column(Integer)

    sample = relationship("Sample", back_populates="markers")
    marker = relationship("Marker", back_populates="samples")

    def __repr__(self):
        return "<Sample_Marker_Association(sample={}, marker={})>"\
            .format(self.sample, self.marker)


Index('ix_sample_marker_associate',
      Sample_Marker_Association.sample_id,
      Sample_Marker_Association.marker_id,
      Sample_Marker_Association.channel_number, unique=True)


class Cell(Base):
    __tablename__ = 'cells'

    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey("samples.id", ondelete="CASCADE"),
                       nullable=False)

    sample_cell_id = Column(Integer)     # local experiment ID
    area = Column(Integer)

    sample = relationship("Sample", back_populates="cells")

    def __repr__(self):
        return "<Cell(sample={}, sample_cell_id={})>"\
            .format(self.sample, self.sample_cell_id)


for ftr in KNOWN_MARKERS['other_features']:
    ftr = header_to_dbcolumn(ftr)
    if ftr in ['sample_cell_id', 'area']:
        continue
    setattr(Cell, ftr, Column(Numeric(15, 0)))


for mkr in KNOWN_MARKERS['markers']:
    mkr = header_to_dbcolumn(mkr)
    setattr(Cell, mkr+'__cell_masks', Column(Numeric(15, 0)))
    setattr(Cell, mkr+'__nuclei_masks', Column(Numeric(15, 0)))


def init(engine):
    Base.metadata.create_all(engine)
