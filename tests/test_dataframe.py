import json
import pandas as pd
import tempfile

from nose.tools import assert_raises
from cycif_db.data_frame import (
    CycDataFrame,
    get_headers_categorized,
    header_to_marker,
    MarkerIncompatibilityError)


data = {
    "cellID": [1],
    "Area": [120],
    "CD45_1_Cell Masks": [15809.175000],
    "DAPI_1_Nuclei Masks": [17131.137500]
}

df = pd.DataFrame(data)
headers = df.columns


def test_header_to_dbcolumn():
    new = headers.map(CycDataFrame().header_to_dbcolumn)

    assert list(new) == [
        'sample_cell_id', 'area', 'cd45:::1__cell_masks',
        'dapi:::1__nuclei_masks'], new


def test_header_to_marker():
    new = headers.map(header_to_marker)

    assert list(new) == [
        'cellID', 'Area', 'CD45_1', 'DAPI_1'], new


def test_get_headers_categorized():
    markers, others = get_headers_categorized(headers)

    assert markers == ['CD45_1_Cell Masks', 'DAPI_1_Nuclei Masks'], markers
    assert others == ['cellID', 'Area'], others


def test_check_feature_compatibility():
    data_frame = CycDataFrame()
    m_markers = pd.Series(['CD45', 'DAPI'])
    cp = data_frame.check_feature_compatibility(df, m_markers)
    assert cp is None

    mapper = {"DAPI_1_Nuclei Masks": "DAPI_100_Nuclei Masks"}
    new_df = df.rename(columns=mapper, inplace=False)
    assert_raises(MarkerIncompatibilityError,
                  data_frame.check_feature_compatibility,
                  new_df, m_markers)
