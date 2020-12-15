import json
import pandas as pd
import tempfile

from cycif_db.data_frame import (
    get_headers_categorized, header_to_dbcolumn,
    header_to_marker, check_feature_compatiblity)


data = {
    "cellID": [1],
    "Area": [120],
    "CD45_1_Cell Masks": [15809.175000],
    "DAPI_1_Nuclei Masks": [17131.137500]
}

df = pd.DataFrame(data)
headers = df.columns


def test_header_to_dbcolumn():
    new = headers.map(header_to_dbcolumn)

    assert list(new) == [
        'sample_cell_id', 'area', 'cd45_1__cell_masks',
        'dapi_1__nuclei_masks'], new


def test_header_to_marker():
    new = headers.map(header_to_marker)

    assert list(new) == [
        'cellID', 'Area', 'CD45_1', 'DAPI_1'], new


def test_get_headers_categorized():
    markers, others = get_headers_categorized(headers)

    assert markers == ['CD45_1_Cell Masks', 'DAPI_1_Nuclei Masks'], markers
    assert others == ['cellID', 'Area'], others


def test_check_feature_compatiblity():
    cp = check_feature_compatiblity(df)
    assert cp is True, cp

    mapper = {"DAPI_1_Nuclei Masks": "DAPI_10_Nuclei Masks"}
    new_df = df.rename(columns=mapper, inplace=False)
    cp = check_feature_compatiblity(new_df)
    assert cp is False, cp

    with tempfile.NamedTemporaryFile() as tmp:
        cp = check_feature_compatiblity(new_df, update=True, toplace=tmp.name)
        new_json = json.load(tmp)

    assert 'DAPI_10' in new_json['markers']
