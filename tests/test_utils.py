from cycif_db.utils import get_configs


def test_get_configs():
    configs = get_configs()
    assert configs['auto_migrate'] is False
    assert configs['db_url'] == 'sqlite:////tmp/db.sqlite'
