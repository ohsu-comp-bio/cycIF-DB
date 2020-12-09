from model.utils import get_configs

def test_get_configs():
    configs = get_configs()
    assert configs['auto_migrate'] == False
    assert configs['db_url'] == 'sqlite:///db.sqlite'