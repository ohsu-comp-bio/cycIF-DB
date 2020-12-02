"""
Data model classes

"""
import logging
import os

from migrate.versioning import repository, schema
from sqlalchemy import Column, create_engine, ForeignKey, Index, text
from sqlalchemy.orm import declarative_base
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



def init(url):
    engine = create_engine(url)
    Base.metadata.create_all(engine)

    # version control
    migrate_repo_dir = os.path.join(os.path.dirname(__file__), 'migrate')
    migrate_repository = repository.Repository(migrate_repo_dir)
    current_version = migrate_repository.version().version
    schema.ControlledSchema.create(engine, migrate_repository, version=current_version)
    db_schema = schema.ControlledSchema(engine, migrate_repository)
    assert db_schema.version == current_version
