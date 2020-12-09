"""
Data model classes

"""
import logging
import os

from migrate.versioning import repository, schema
from sqlalchemy import Column, create_engine, ForeignKey, Index, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import (
    Boolean,
    DateTime,
    Integer,
    Numeric,
    String,
)


log = logging.getLogger(__name__)

Base = declarative_base()


class Samples(Base):
    __tablename__ = 'samples'

    id = Column(Integer, primary_key=True)


class Markers(Base):
    __tablename__ = 'markers'

    id = Column(Integer, primary_key=True)


class Cells(Base):
    __tablename__ = 'cells'

    id = Column(Integer, primary_key=True)
    sample_id = Column(ForeignKey('samples.id'))
    marker_id = Column(ForeignKey('markers.id'))



def init(engine):
    Base.metadata.create_all(engine)
