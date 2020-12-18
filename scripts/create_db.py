import logging
import pathlib
import sys

module_path = pathlib.Path(__file__).absolute().parent.parent
sys.path.insert(1, str(module_path))

from cycif_db.model import create_db


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


def invoke_create(url):
    create_db(url)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        url = sys.argv[1]
    else:
        url = None
    invoke_create(url)
