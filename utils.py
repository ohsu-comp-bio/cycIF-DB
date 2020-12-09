import os
import yaml


def get_configs(conf_file=None):
    if not conf_file:
        conf_file = os.path.join(os.path.dirname(__file__), 'config.yml')
    with open(conf_file, 'r') as fp:
        configs = yaml.safe_load(fp)

    return configs