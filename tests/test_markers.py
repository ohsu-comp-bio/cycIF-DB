import json
import tempfile
from cycif_db.markers import Markers


cyc_markers = Markers()


def test_load_known_markers():
    
    markers, other_features = cyc_markers.markers, cyc_markers.other_features

    assert len(markers) > 80, len(markers)
    assert len(other_features) == 12, len(other_features)

    print(markers)
    assert 'asma' in markers
    assert markers['asma'] == 'alpha_SMA'

    assert 'area' in other_features
    assert other_features['area'] == 'area'


def test_get_dbname():
    assert cyc_markers.get_dbname('aSMA') == 'alpha_SMA'
    assert cyc_markers.get_dbname('alpha-SMA') == 'alpha_SMA'

    assert cyc_markers.get_dbname('Area') == 'area'
    assert cyc_markers.get_dbname('cellID') == 'sample_cell_id'
    assert cyc_markers.get_dbname('X_centroid') == 'x_centroid'


def test_update_stock_markers():
    cyc_markers = Markers()
    o_markers = cyc_markers.markers
    o_other_features = cyc_markers.other_features

    new_markers = {
        'ABC_1': 'ABC-1',
        'ABC_2': ['ABC-2', 'ABC2'],
        'CK14': 'CK14a',
        'CD3': ['CD3a', 'CD3b']
    }
    new_others = {
        'area': 'Area0',
        'test1': 'Test1',
        'test2': ['Test2', 'Test-2']
    }

    with tempfile.NamedTemporaryFile() as tmp:
        to_path = tmp.name
        cyc_markers.update_stock_markers(
            new_markers, new_others, toplace=to_path, reload=True)
        with open(to_path, 'r') as fp:
            markers_n_features = json.load(fp)
        markers = markers_n_features['markers']
        other_features = markers_n_features['other_features']

    assert cyc_markers._path == to_path
    assert len(other_features) - len(o_other_features) == 2, other_features
    assert len(cyc_markers.other_features) - len(o_other_features) == 4, \
        cyc_markers.other_features
    assert other_features['area'] == ['Area', 'Area0'], other_features['area']
    assert other_features['test1'] == ['Test1'], other_features['test1']
    assert other_features['test2'] == ['Test2', 'Test-2'], \
        other_features['test2']

    assert len(cyc_markers.markers) - len(o_markers) == 6
    assert markers['CD3'] == ['CD3', 'CD3a', 'CD3b'], markers['CD3']
    assert markers['ABC_1'] == ['ABC-1'], markers['ABC_1']
    assert markers['ABC_2'] == ['ABC-2', 'ABC2']
    assert markers['CK14'] == ['CK14', 'CK_14', 'CK-14', 'CK14a'], \
        markers['CK14']
