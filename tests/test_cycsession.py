import pandas as pd
import pathlib
import random
import string

from nose.tools import assert_raises
from sqlalchemy_utils import drop_database, database_exists
from cycif_db import CycSession
from cycif_db import model
from cycif_db.model import create_db, Sample, Cell
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

    path84 = str(pathlib.Path.joinpath(
        module, 'examples',
        'Galaxy76-[quantification_on_data_1,_data_74,_and_data_71].csv')
    )

    path_markers_84 = str(pathlib.Path.joinpath(
        module, 'examples',
        'Galaxy76-markers.csv')
    )

    csess.add_sample_complex({'name': 'Galaxy84', 'tag': 'v0.1'},
                             path84, path_markers_84)
    n_samples = csess.query(Sample.id).count()
    n_cells = csess.query(Cell.id).count()
    assert n_samples == 1, n_samples
    assert n_cells == 11032, n_cells
