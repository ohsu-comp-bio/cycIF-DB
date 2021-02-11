import numpy as np
import pandas as pd
import tempfile
from cycif_db.markers import Markers


cyc_markers = Markers()


def test_load_known_markers():
    markers, other_features = cyc_markers.markers, cyc_markers.other_features

    assert len(markers) > 80, len(markers)
    assert len(other_features) == 12, len(other_features)

    print(markers)
    assert 'asma' in markers
    assert markers['asma'] == 266

    assert 'area' in other_features
    assert other_features['area'] == 'area'


def test_get_marker_db_entry():
    assert cyc_markers.get_marker_db_entry('aSMA') == \
        ('aSMA', '', '', ''), \
        cyc_markers.get_marker_db_entry('aSMA')
    assert cyc_markers.get_marker_db_entry('alpha-SMA') == \
        ('aSMA', '', '', ''), \
        cyc_markers.get_marker_db_entry('alpha-SMA')


def test_get_other_feature_db_name():
    assert cyc_markers.get_other_feature_db_name('Area') == 'area'
    assert cyc_markers.get_other_feature_db_name('cellID') == 'sample_cell_id'
    assert cyc_markers.get_other_feature_db_name('X_centroid') == \
        'x_centroid'


def test_update_stock_markers():
    cyc_markers = Markers()
    o_markers = cyc_markers.markers
    o_other_features = cyc_markers.other_features

    new_markers = [
        ('ABC1', None, None, 1, 'ABC_1'),
        ('ABC1', 'eF660', None, None, 'ABC1_660'),
        ('ABC_2', None, None, None, 'ABC-2; ABC2'),
        ('CK14', None, None, None, 'CK14a'),
        ('CD3', 'eF450', 'goat', None, 'CD3a; CD3b')
    ]

    with tempfile.NamedTemporaryFile() as tmp:
        to_path = tmp.name
        cyc_markers.update_stock_markers(
            new_markers, toplace=to_path, reload=True)
        df = pd.read_csv(to_path, sep='\t').fillna('')

    other_features = cyc_markers.other_features

    assert cyc_markers._path == to_path
    assert len(other_features) == len(o_other_features), other_features

    assert len(cyc_markers.markers) - len(o_markers) == 7
    entry = tuple(df.iloc[-2])
    assert (entry) == ('ABC_2', '', '', '', 'ABC-2; ABC2'), entry
    entry = tuple(df.iloc[-1])
    assert (entry) == ('CD3', 'eF450', 'goat', '', 'CD3a; CD3b'), entry

    masks = [df[x] == (y or '') for x, y in
             zip(cyc_markers.unique_keys, new_markers[-2])]
    masks = np.logical_and.reduce(masks)
    entry = list(df.loc[masks, 'aliases'])[-1]
    assert (entry) == 'CK14; CK_14; CK-14; CK14a', entry
