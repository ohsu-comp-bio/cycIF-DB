import pathlib
import yaml


def get_configs(conf_file=None):
    if not conf_file:
        conf_file = str(pathlib.Path.joinpath(pathlib.Path(__file__).parent, 'config.yml'))
    with open(conf_file, 'r') as fp:
        configs = yaml.safe_load(fp)

    return configs