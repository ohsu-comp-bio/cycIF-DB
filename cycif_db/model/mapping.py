"""
Data model classes

"""
import json
import logging
import os

from sqlalchemy import Column, ForeignKey, func, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import (
    DateTime,
    Integer,
    Numeric,
    String,
)


log = logging.getLogger(__name__)

MARKERS_PATH = os.path.join(os.path.dirname(__file__),
                            os.pardir, 'markers', 'markers.json')
with open(MARKERS_PATH, 'r') as fp:
    KNOWN_MARKERS = json.load(fp)

Base = declarative_base()


class Sample(Base):
    __tablename__ = 'sample'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    tag = Column(String)
    annotation = Column(String)
    entry_at = Column(DateTime(timezone=True), server_default=func.now())

    cells = relationship('Cell', back_populates='sample')
    markers = relationship('Marker',
                           secondary='sample_marker_association',
                           back_populates='samples',
                           cascade="all, delete-orphan")
    marker_associates = relationship('Sample_Marker_Association',
                                     back_populates='sample')

    def __repr__(self):
        return "<Sample({}: '{}', '{}')>".format(
            self.id, self.name, self.tag)


Index('ix_sample_name', Sample.name, Sample.tag, unique=True)


class Marker(Base):
    __tablename__ = 'marker'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    fluor = Column(Integer)
    anti = Column(String)
    replicate = Column(Integer)
    entry_at = Column(DateTime(timezone=True), server_default=func.now())

    sample_associates = relationship('Sample_Marker_Association',
                                     back_populates='marker')
    samples = relationship('Sample',
                           secondary='sample_marker_association',
                           back_populates='markers',
                           passive_deletes=True)

    def __repr__(self):
        return "<Marker({}, '{}', '{}', '{}', '{}')>".format(
            self.id, self.name, self.fluor, self.anti, self.replicate)


Index('ix_marker_name', Marker.name, Marker.fluor, Marker.anti,
      Marker.replicate, unique=True)


class Sample_Marker_Association(Base):
    __tablename__ = 'sample_marker_association'

    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey("samples.id", ondelete="CASCADE",
                                           onupdate="CASCADE"))
    marker_id = Column(Integer, ForeignKey("markers.id", onupdate="CASCADE"))
    channel_number = Column(Integer)
    cycle_number = Column(Integer)
    entry_at = Column(DateTime(timezone=True), server_default=func.now())

    sample = relationship("Sample", back_populates="marker_associates")
    marker = relationship("Marker", back_populates="sample_associates")

    def __repr__(self):
        return "<Sample_Marker_Association(sample={}, marker={})>"\
            .format(self.sample, self.marker)


Index('ix_sample_marker_associate',
      Sample_Marker_Association.sample_id,
      Sample_Marker_Association.marker_id,
      Sample_Marker_Association.channel_number, unique=True)


class Cell(Base):
    __tablename__ = 'cell'

    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey("samples.id", ondelete="CASCADE"),
                       onupdate='CASCADE', nullable=False)
    sample_cell_id = Column(Integer)     # local experiment ID
    entry_at = Column(DateTime(timezone=True), server_default=func.now())
    features = Column(JSONB)

    sample = relationship("Sample", back_populates="cells")

    def __repr__(self):
        return "<Cell(sample={}, sample_cell_id={})>"\
            .format(self.sample, self.sample_cell_id)


for ftr in KNOWN_MARKERS['other_features']:
    ftr = ftr.lower()
    if ftr == 'sample_cell_id':
        continue
    setattr(Cell, ftr, Column(Numeric(15, 4)))


def init(engine):
    Base.metadata.create_all(engine)
