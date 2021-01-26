""" Generic utilities
"""
import os
import yaml

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def get_configs(conf_file=None):
    """ Get configurations, like db_url
    """
    if not conf_file:
        conf_file = os.path.join(os.path.dirname(__file__),
                                 os.pardir,
                                 'config.yml')
    with open(conf_file, 'r') as fp:
        configs = yaml.safe_load(fp)

    return configs


def engine_maker(url=None, **kw):
    if not url:
        configs = get_configs()
        assert 'db_url' in configs and configs['db_url'], \
            "No database URL is set in `config.yml`!"

        url = get_configs()['db_url']
    engine = create_engine(url, **kw)
    return engine


def session_maker(engine=None, **kwargs):
    """ Provide session object to the wrapped function
    """
    if not engine:
        engine = engine_maker()
    Session = sessionmaker(engine, **kwargs)
    return Session()
