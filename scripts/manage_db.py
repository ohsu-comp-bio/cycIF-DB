import logging
import pathlib
import sys

from migrate.versioning.shell import main

work_dir = pathlib.Path(__file__).absolute().parent.parent

sys.path.insert(1, str(work_dir))

from cycif_db.model import check
from cycif_db.utils import get_configs


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


def invoke_migrate_main():
    configs = get_configs()
    db_url = configs['db_url']
    repo = check.migrate_repo_dir

    main(repository=repo, url=db_url)


if __name__ == "__main__":
    invoke_migrate_main()
