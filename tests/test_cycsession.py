import pandas as pd
import pathlib
import random
import string

from nose.tools import assert_raises
from sqlalchemy_utils import drop_database, database_exists
from cycif_db import CycSession
from cycif_db.cyc_session import DB_Key, fuse_db_keys
from cycif_db.model import create_db, Sample, Cell, Marker
from cycif_db.utils import engine_maker


data = {
    "cellID": [1],
    "Area": [120],
    "CD45_1_Cell Masks": [15809.175000],
    "DAPI_1_Nuclei Masks": [17131.137500]
}

df = pd.DataFrame(data)
headers = df.columns

letters = string.ascii_lowercase
random.seed(42)
db_name = ''.join(random.choice(letters) for i in range(30))
url = 'postgresql:///' + db_name

engine = engine_maker(url)
csess = CycSession(bind=engine)


_multiprocess_shared_ = True


def setup():
    if database_exists(url):
        raise Exception("Test database exists: %s!" % url)
    create_db(url)
    # ingest markers
    csess.insert_or_sync_markers()


def teardown():
    csess.close()
    drop_database(url)


def test_other_feature_to_dbcolumn():
    rval = csess.other_feature_to_dbcolumn('cellID')
    assert rval == 'sample_cell_id', rval
    rval = csess.other_feature_to_dbcolumn('Area')
    assert rval == 'area', rval

    assert_raises(ValueError,
                  csess.other_feature_to_dbcolumn,
                  'Something_New')


def test_marker_header_to_dbkey():
    rval = csess.marker_header_to_dbkey('CD45_1_Cell Masks')
    assert rval == '56_cl', rval

    rval = csess.marker_header_to_dbkey('CD45_1_Nuclei Masks')
    assert rval == '56_nu', rval

    rval = csess.marker_header_to_dbkey('DAPI_1_Nuclei Masks')
    assert rval == '105_nu', rval

    assert_raises(AssertionError,
                  csess.marker_header_to_dbkey,
                  'DAPI_100_Nuclei Masks')

    assert_raises(Exception,
                  csess.marker_header_to_dbkey,
                  'DAPI_100_Nuclei Masks__')


def test_add_sample_complex():
    module = pathlib.Path(__file__).absolute().parent.parent

    path76 = str(pathlib.Path.joinpath(
        module, 'examples',
        'Galaxy76-[quantification_on_data_1,_data_74,_and_data_71].csv')
    )

    path_markers_76 = str(pathlib.Path.joinpath(
        module, 'examples',
        'Galaxy76-markers.csv')
    )

    csess.add_sample_complex({'name': 'Galaxy76', 'tag': 'v0.1'},
                             path76, path_markers_76)
    n_samples = csess.query(Sample.id).count()
    n_cells = csess.query(Cell.id).count()
    assert n_samples == 1, n_samples
    assert n_cells == 11032, n_cells

    path84 = str(pathlib.Path.joinpath(
        module, 'examples',
        'Galaxy84-[quantification_on_data_1,_data_83,_and_data_71].csv')
    )

    path_markers_84 = str(pathlib.Path.joinpath(
        module, 'examples',
        'Galaxy84-markers.csv')
    )

    csess.add_sample_complex({'name': 'Galaxy84', 'tag': 'v0.2'},
                             path84, path_markers_84)
    n_samples = csess.query(Sample.id).count()
    n_cells = csess.query(Cell.id).count()
    assert n_samples == 2, n_samples
    assert n_cells == 44551, n_cells


def test_db_key():
    m1 = Marker(id=10001, name='CD1000')
    m2 = Marker(id=10002, name='CD1000', fluor='ef570')
    m3 = Marker(id=10003, name='CD1000', fluor='ef570', anti='goat')
    m4 = Marker(id=10004, name='CD1000', anti='goat')
    m5 = Marker(id=10005, name='CD1000', duplicate='1')
    m6 = Marker(id=10006, name='CD1000', duplicate='2')
    m7 = Marker(id=10007, name='CD1002')

    csess.add_all([m1, m2, m3, m4, m5, m6, m7])

    assert DB_Key(csess, '10001_cl') != DB_Key(csess, '10007_cl')
    assert DB_Key(csess, '10001_cl') != DB_Key(csess, '10002_cl')
    assert DB_Key(csess, '10001_cl', fluor_sensitive=False) == \
        DB_Key(csess, '10002_cl', fluor_sensitive=False)
    assert DB_Key(csess, '10001_cl') == DB_Key(csess, '10004_cl')
    assert DB_Key(csess, '10001_cl', anti_sensitive=True) != \
        DB_Key(csess, '10004_cl', anti_sensitive=True)
    assert DB_Key(csess, '10002_cl') == DB_Key(csess, '10003_cl')
    assert DB_Key(csess, '10002_cl', anti_sensitive=True) != \
        DB_Key(csess, '10003_cl', anti_sensitive=True)
    assert DB_Key(csess, '10001_cl') == DB_Key(csess, '10005_cl')
    assert DB_Key(csess, '10001_cl') == DB_Key(csess, '10006_cl')
    assert DB_Key(csess, '10001_cl') != DB_Key(csess, '10006_nu')
    assert_raises(ValueError, DB_Key, csess, '10001_xx')

    assert repr(DB_Key(csess, '10001_cl')) == "<DB_Key('10001_cl')>",\
        repr(DB_Key(csess, '10001_cl'))
    assert DB_Key(csess, '10001_cl').to_header() == 'CD1000__cell_masks', \
        DB_Key(csess, '10001_cl').to_header()
    assert DB_Key(csess, '10001_nu').to_header() == 'CD1000__nuclei_masks', \
        DB_Key(csess, '10001_nu').to_header()
    assert DB_Key(csess, '10002_cl').to_header() == \
        'CD1000_ef570__cell_masks', \
        DB_Key(csess, '10002_cl').to_header()
    assert DB_Key(csess, '10002_cl', fluor_sensitive=False).to_header() == \
        'CD1000__cell_masks', \
        DB_Key(csess, '10002_cl', fluor_sensitive=False).to_header()
    assert DB_Key(csess, '10004_cl').to_header() == 'CD1000__cell_masks', \
        DB_Key(csess, '10004_cl').to_header()
    assert DB_Key(csess, '10004_cl', anti_sensitive=True).to_header() == \
        'CD1000_goat__cell_masks', \
        DB_Key(csess, '10004_cl', anti_sensitive=True).to_header()
    assert DB_Key(csess, '10005_cl').to_header() == 'CD1000_1__cell_masks', \
        DB_Key(csess, '10005_cl').to_header()
    assert DB_Key(csess, '10006_cl').to_header() == 'CD1000_2__cell_masks', \
        DB_Key(csess, '10006_cl').to_header()

    key_set = set([DB_Key(csess, '10001_cl'), DB_Key(csess, '10005_cl'),
                   DB_Key(csess, '10006_cl')])
    assert len(key_set) == 1

    assert DB_Key(csess, '10001_cl') in key_set
    assert DB_Key(csess, '10005_cl') in key_set
    assert DB_Key(csess, '10006_cl') in key_set


def test_get_sample():
    s1 = Sample(id=1000001, name='sample_name1', tag='v1')
    s2 = Sample(id=1000002, name='sample_name2', tag='v2')

    csess.add_all([s1, s2])

    sample1 = csess.get_sample(id=1000001)
    assert sample1
    assert sample1.name == 'sample_name1'
    assert sample1.tag == 'v1'
    sample2 = csess.get_sample(name='sample_name2', tag='v2')
    assert sample2
    assert sample2.id == 1000002
    assert sample2.name == 'sample_name2'
    assert sample2.tag == 'v2'

    sample = csess.get_sample(1000003)
    assert sample is None, sample
    sample = csess.get_sample(name='sample_name3')
    assert sample is None, sample


def test_get_sample_db_keys():
    keys = csess.get_sample_db_keys(name='Galaxy76', tag='v0.1')

    expected = ['7_cl', '13_cl', '18_cl', '21_cl', '269_cl', '49_cl',
                '55_cl', '83_cl', '84_cl', '85_cl', '87_cl', '90_cl',
                '103_cl', '105_cl', '108_cl', '109_cl', '110_cl',
                '111_cl', '112_cl', '113_cl', '114_cl', '133_cl',
                '121_cl', '131_cl', '125_cl', '148_cl', '154_cl',
                '157_cl', '162_cl', '180_cl', '209_cl', '217_cl',
                '229_cl', '233_cl', '235_cl', '264_cl']

    assert keys == expected, keys


def test_fuse_db_keys():
    keys_76 = csess.get_sample_db_keys(1)
    keys_84 = csess.get_sample_db_keys(2)

    keys_84 = [k[:-2]+'cl' for k in keys_84]

    fused = fuse_db_keys(csess, [keys_76, keys_84])

    expected = ['7_cl', '13_cl', '18_cl', '49_cl', '55_cl', '56_cl',
                '57_cl', '105_cl', '108_cl', '109_cl', '110_cl',
                '111_cl', '112_cl', '113_cl', '114_cl', '162_cl',
                '180_cl', '235_cl', '269_cl']
    expected = ['7_cl', '13_cl', '18_cl', '269_cl', '49_cl', '55_cl',
                '56_cl', '57_cl', '105_cl', '108_cl', '109_cl', '110_cl',
                '111_cl', '112_cl', '113_cl', '114_cl', '162_cl',
                '180_cl', '235_cl']

    assert fused == expected, fused

    fused = fuse_db_keys(csess, [keys_76, keys_84], marker_filter='union')
    assert len(fused) == 56, fused
