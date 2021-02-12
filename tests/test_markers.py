import numpy as np
import pandas as pd
import tempfile
from cycif_db.markers import Markers, Marker_Comparator
from cycif_db.model import Marker


cyc_markers = Markers()


def test_load_known_markers():
    markers, other_features = cyc_markers.markers, cyc_markers.other_features

    assert len(markers) > 80, len(markers)
    assert len(other_features) == 12, len(other_features)

    assert 'asma' in markers
    assert markers['asma'] == 268, markers['asma']

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
        ('ABC_2', None, None, None, 'ABC-2, ABC2'),
        ('CK14', None, None, None, 'CK14a'),
        ('CD3', 'eF450', 'goat', None, 'CD3a, CD3b')
    ]

    with tempfile.NamedTemporaryFile() as tmp:
        to_path = tmp.name
        cyc_markers.update_stock_markers(
            new_markers, toplace=to_path, reload=True)
        df = pd.read_csv(to_path, sep='\t').fillna('')

    other_features = cyc_markers.other_features

    assert cyc_markers._path == to_path
    assert len(other_features) == len(o_other_features), other_features

    assert len(cyc_markers.markers) - len(o_markers) == 7, \
        set(cyc_markers.markers.keys()) - set(o_markers.keys())
    entry = tuple(df.iloc[-2])
    assert (entry) == ('ABC_2', '', '', '', 'ABC-2, ABC2'), entry
    entry = tuple(df.iloc[-1])
    assert (entry) == ('CD3', 'eF450', 'goat', '', 'CD3a, CD3b'), entry

    masks = [df[x] == (y or '') for x, y in
             zip(cyc_markers.unique_keys, new_markers[-2])]
    masks = np.logical_and.reduce(masks)
    entry = list(df.loc[masks, 'aliases'])[-1]
    assert (entry) == 'CK14, CK_14, CK-14, CK14a', entry


def test_marker_comparator():
    m1 = Marker(name='CD4')
    m2 = Marker(name='CD4', fluor='ef570')
    m3 = Marker(name='CD4', fluor='ef570', anti='goat')
    m4 = Marker(name='CD4', anti='goat')
    m5 = Marker(name='CD4', duplicate='1')
    m6 = Marker(name='CD4', duplicate='2')
    m7 = Marker(name='CD45')

    assert Marker_Comparator(m1) != Marker_Comparator(m7)
    assert Marker_Comparator(m1) != Marker_Comparator(m2)
    assert Marker_Comparator(m1, fluor_sensitive=False) == \
        Marker_Comparator(m2, fluor_sensitive=False)
    assert Marker_Comparator(m1) == Marker_Comparator(m4)
    assert Marker_Comparator(m1, anti_sensitive=True) != \
        Marker_Comparator(m4, anti_sensitive=True)
    assert Marker_Comparator(m2) == Marker_Comparator(m3)
    assert Marker_Comparator(m2, anti_sensitive=True) != \
        Marker_Comparator(m3, anti_sensitive=True)

    assert Marker_Comparator(m1) == Marker_Comparator(m5)
    assert Marker_Comparator(m1) == Marker_Comparator(m6)

    assert repr(Marker_Comparator(m1)) == 'CD4', repr(Marker_Comparator(m1))
    assert repr(Marker_Comparator(m2)) == 'CD4_ef570', \
        repr(Marker_Comparator(m2))
    assert repr(Marker_Comparator(m2, fluor_sensitive=False)) == 'CD4', \
        repr(Marker_Comparator(m2, fluor_sensitive=False))
    assert repr(Marker_Comparator(m4)) == 'CD4', \
        repr(Marker_Comparator(m4))
    assert repr(Marker_Comparator(m4, anti_sensitive=True)) == 'CD4_goat', \
        repr(Marker_Comparator(m4, anti_sensitive=True))
    assert repr(Marker_Comparator(m5)) == 'CD4_1', repr(Marker_Comparator(m5))
    assert repr(Marker_Comparator(m6)) == 'CD4_2', repr(Marker_Comparator(m6))

    marker_set = set([Marker_Comparator(m1), Marker_Comparator(m5),
                      Marker_Comparator(m6)])
    assert len(marker_set) == 1

    assert Marker_Comparator(m1) in marker_set
    assert Marker_Comparator(m5) in marker_set
    assert Marker_Comparator(m6) in marker_set
