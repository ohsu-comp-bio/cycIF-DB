import datetime
import logging

from sqlalchemy import (
    Column, create_engine, ForeignKey, Index, MetaData, Table, text)
from sqlalchemy.types import (
    Boolean,
    DateTime,
    Integer,
    Numeric,
    String,
)


log = logging.getLogger(__name__)
now = datetime.datetime.utcnow
metadata = MetaData()


Samples_table = Table(
    'samples', metadata,
    Column('id', Integer, primary_key=True),

)

Markers_table = Table(
    'markers', metadata,
    Column('id', Integer, primary_key=True),

)


Cells_table = Table(
    'cells', metadata,
    Column('id', Integer, primary_key=True),
    Column('sample_id', ForeignKey('samples.id')),
    Column('marker_id', ForeignKey('markers.id')),

)


def upgrade(engine):
    print(__doc__)
    metadata.bind = engine
    metadata.create_all()

