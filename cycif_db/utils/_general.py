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


def session_maker():
    """ Provide session object to the wrapped function
    """
    url = get_configs()['db_url']
    engine = create_engine(url)
    Session = sessionmaker(engine)
    return Session()
