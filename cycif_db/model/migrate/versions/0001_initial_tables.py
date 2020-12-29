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

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

log = logging.getLogger(__name__)

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
    area = Column(Numeric(precision=15, scale=0))
    eccentricity = Column(Numeric(precision=15, scale=0))
    extent = Column(Numeric(precision=15, scale=0))
    majoraxislength = Column(Numeric(precision=15, scale=0))
    minoraxislength = Column(Numeric(precision=15, scale=0))
    orientation = Column(Numeric(precision=15, scale=0))
    solidity = Column(Numeric(precision=15, scale=0))
    x_centroid = Column(Numeric(precision=15, scale=0))
    y_centroid = Column(Numeric(precision=15, scale=0))
    column_centroid = Column(Numeric(precision=15, scale=0))
    row_centroid = Column(Numeric(precision=15, scale=0))
    alpha_sma__cell_masks = Column(Numeric(precision=15, scale=0))
    alpha_sma__nuclei_masks = Column(Numeric(precision=15, scale=0))
    ar__cell_masks = Column(Numeric(precision=15, scale=0))
    ar__nuclei_masks = Column(Numeric(precision=15, scale=0))
    cd11b__cell_masks = Column(Numeric(precision=15, scale=0))
    cd11b__nuclei_masks = Column(Numeric(precision=15, scale=0))
    cd14__cell_masks = Column(Numeric(precision=15, scale=0))
    cd14__nuclei_masks = Column(Numeric(precision=15, scale=0))
    cd163__cell_masks = Column(Numeric(precision=15, scale=0))
    cd163__nuclei_masks = Column(Numeric(precision=15, scale=0))
    cd20__cell_masks = Column(Numeric(precision=15, scale=0))
    cd20__nuclei_masks = Column(Numeric(precision=15, scale=0))
    cd3__cell_masks = Column(Numeric(precision=15, scale=0))
    cd3__nuclei_masks = Column(Numeric(precision=15, scale=0))
    cd45__cell_masks = Column(Numeric(precision=15, scale=0))
    cd45__nuclei_masks = Column(Numeric(precision=15, scale=0))
    cd45_1__cell_masks = Column(Numeric(precision=15, scale=0))
    cd45_1__nuclei_masks = Column(Numeric(precision=15, scale=0))
    cd45_2__cell_masks = Column(Numeric(precision=15, scale=0))
    cd45_2__nuclei_masks = Column(Numeric(precision=15, scale=0))
    cd45r_1__cell_masks = Column(Numeric(precision=15, scale=0))
    cd45r_1__nuclei_masks = Column(Numeric(precision=15, scale=0))
    cd45r_2__cell_masks = Column(Numeric(precision=15, scale=0))
    cd45r_2__nuclei_masks = Column(Numeric(precision=15, scale=0))
    cd45ro__cell_masks = Column(Numeric(precision=15, scale=0))
    cd45ro__nuclei_masks = Column(Numeric(precision=15, scale=0))
    cd4_1__cell_masks = Column(Numeric(precision=15, scale=0))
    cd4_1__nuclei_masks = Column(Numeric(precision=15, scale=0))
    cd4_2__cell_masks = Column(Numeric(precision=15, scale=0))
    cd4_2__nuclei_masks = Column(Numeric(precision=15, scale=0))
    cd68__cell_masks = Column(Numeric(precision=15, scale=0))
    cd68__nuclei_masks = Column(Numeric(precision=15, scale=0))
    cd8a__cell_masks = Column(Numeric(precision=15, scale=0))
    cd8a__nuclei_masks = Column(Numeric(precision=15, scale=0))
    ck__cell_masks = Column(Numeric(precision=15, scale=0))
    ck__nuclei_masks = Column(Numeric(precision=15, scale=0))
    ck_14_1__cell_masks = Column(Numeric(precision=15, scale=0))
    ck_14_1__nuclei_masks = Column(Numeric(precision=15, scale=0))
    ck_14_2__cell_masks = Column(Numeric(precision=15, scale=0))
    ck_14_2__nuclei_masks = Column(Numeric(precision=15, scale=0))
    ck_17__cell_masks = Column(Numeric(precision=15, scale=0))
    ck_17__nuclei_masks = Column(Numeric(precision=15, scale=0))
    ck_19__cell_masks = Column(Numeric(precision=15, scale=0))
    ck_19__nuclei_masks = Column(Numeric(precision=15, scale=0))
    ck_7__cell_masks = Column(Numeric(precision=15, scale=0))
    ck_7__nuclei_masks = Column(Numeric(precision=15, scale=0))
    cyclind1__cell_masks = Column(Numeric(precision=15, scale=0))
    cyclind1__nuclei_masks = Column(Numeric(precision=15, scale=0))
    dapi_1__cell_masks = Column(Numeric(precision=15, scale=0))
    dapi_1__nuclei_masks = Column(Numeric(precision=15, scale=0))
    dapi_2__cell_masks = Column(Numeric(precision=15, scale=0))
    dapi_2__nuclei_masks = Column(Numeric(precision=15, scale=0))
    dapi_3__cell_masks = Column(Numeric(precision=15, scale=0))
    dapi_3__nuclei_masks = Column(Numeric(precision=15, scale=0))
    dapi_4__cell_masks = Column(Numeric(precision=15, scale=0))
    dapi_4__nuclei_masks = Column(Numeric(precision=15, scale=0))
    dapi_5__cell_masks = Column(Numeric(precision=15, scale=0))
    dapi_5__nuclei_masks = Column(Numeric(precision=15, scale=0))
    dapi_6__cell_masks = Column(Numeric(precision=15, scale=0))
    dapi_6__nuclei_masks = Column(Numeric(precision=15, scale=0))
    dapi_7__cell_masks = Column(Numeric(precision=15, scale=0))
    dapi_7__nuclei_masks = Column(Numeric(precision=15, scale=0))
    dapi_8__cell_masks = Column(Numeric(precision=15, scale=0))
    dapi_8__nuclei_masks = Column(Numeric(precision=15, scale=0))
    e_cadherin__cell_masks = Column(Numeric(precision=15, scale=0))
    e_cadherin__nuclei_masks = Column(Numeric(precision=15, scale=0))
    egfr__cell_masks = Column(Numeric(precision=15, scale=0))
    egfr__nuclei_masks = Column(Numeric(precision=15, scale=0))
    er_alpha__cell_masks = Column(Numeric(precision=15, scale=0))
    er_alpha__nuclei_masks = Column(Numeric(precision=15, scale=0))
    erk_1__cell_masks = Column(Numeric(precision=15, scale=0))
    erk_1__nuclei_masks = Column(Numeric(precision=15, scale=0))
    foxp3__cell_masks = Column(Numeric(precision=15, scale=0))
    foxp3__nuclei_masks = Column(Numeric(precision=15, scale=0))
    goat_igg_af488__cell_masks = Column(Numeric(precision=15, scale=0))
    goat_igg_af488__nuclei_masks = Column(Numeric(precision=15, scale=0))
    goat_igg_af555__cell_masks = Column(Numeric(precision=15, scale=0))
    goat_igg_af555__nuclei_masks = Column(Numeric(precision=15, scale=0))
    goat_igg_af647__cell_masks = Column(Numeric(precision=15, scale=0))
    goat_igg_af647__nuclei_masks = Column(Numeric(precision=15, scale=0))
    granzymeb__cell_masks = Column(Numeric(precision=15, scale=0))
    granzymeb__nuclei_masks = Column(Numeric(precision=15, scale=0))
    h2a_x__cell_masks = Column(Numeric(precision=15, scale=0))
    h2a_x__nuclei_masks = Column(Numeric(precision=15, scale=0))
    her2__cell_masks = Column(Numeric(precision=15, scale=0))
    her2__nuclei_masks = Column(Numeric(precision=15, scale=0))
    hes_1__cell_masks = Column(Numeric(precision=15, scale=0))
    hes_1__nuclei_masks = Column(Numeric(precision=15, scale=0))
    histone__cell_masks = Column(Numeric(precision=15, scale=0))
    histone__nuclei_masks = Column(Numeric(precision=15, scale=0))
    hla_a__cell_masks = Column(Numeric(precision=15, scale=0))
    hla_a__nuclei_masks = Column(Numeric(precision=15, scale=0))
    ki_67__cell_masks = Column(Numeric(precision=15, scale=0))
    ki_67__nuclei_masks = Column(Numeric(precision=15, scale=0))
    lag_3__cell_masks = Column(Numeric(precision=15, scale=0))
    lag_3__nuclei_masks = Column(Numeric(precision=15, scale=0))
    p21__cell_masks = Column(Numeric(precision=15, scale=0))
    p21__nuclei_masks = Column(Numeric(precision=15, scale=0))
    parp__cell_masks = Column(Numeric(precision=15, scale=0))
    parp__nuclei_masks = Column(Numeric(precision=15, scale=0))
    pd_1__cell_masks = Column(Numeric(precision=15, scale=0))
    pd_1__nuclei_masks = Column(Numeric(precision=15, scale=0))
    pd_l1__cell_masks = Column(Numeric(precision=15, scale=0))
    pd_l1__nuclei_masks = Column(Numeric(precision=15, scale=0))
    pr__cell_masks = Column(Numeric(precision=15, scale=0))
    pr__nuclei_masks = Column(Numeric(precision=15, scale=0))
    rad51__cell_masks = Column(Numeric(precision=15, scale=0))
    rad51__nuclei_masks = Column(Numeric(precision=15, scale=0))
    rb__cell_masks = Column(Numeric(precision=15, scale=0))
    rb__nuclei_masks = Column(Numeric(precision=15, scale=0))
    vimentin__cell_masks = Column(Numeric(precision=15, scale=0))
    vimentin__nuclei_masks = Column(Numeric(precision=15, scale=0))

    sample = relationship("Sample", back_populates="cells")

    def __repr__(self):
        return "<Cell(sample={}, sample_cell_id={})>"\
            .format(self.sample, self.sample_cell_id)


def upgrade(engine):
    print(__doc__)
    Base.metadata.create_all(engine)
