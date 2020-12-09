import logging
import pathlib
import sys
import yaml

from migrate.versioning.shell import main


work_dir = pathlib.Path(__file__).parent.parent

sys.path.insert(1, str(work_dir))

from model.check import create_or_verify_database as create_database
from model.utils import get_configs

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


def invoke_create():
    configs = get_configs()
    db_url = configs['db_url']

    create_database(db_url)


if __name__ == "__main__":
    invoke_create()