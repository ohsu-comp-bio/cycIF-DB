""" Input result data from cycIF, csv file or DataFrame,
into database.

Help:
python scripts/update_sample_feature_list.py --help
"""
import argparse
import logging
import pathlib
import sys

work_dir = pathlib.Path(__file__).absolute().parent.parent
sys.path.insert(1, str(work_dir))
from cycif_db import CycSession

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument(
    'sample', type=str, nargs='?',
    help="Sample name and tag separated by `__`.")
parser.add_argument(
    'cells', type=str, nargs='?',
    help="The path to cells quantification data, in csv.")
parser.add_argument(
    '--dir', '-d', dest='dir', type=str, required=False,
    help=("As an alternative input option, a folder containing bother cells "
          "and markers csv overrides other positional arguments. The folder "
          "name will be used as arguments for sample, separated by `__`.")
)

args = parser.parse_args()

folder = args.dir

if folder:
    folder = pathlib.Path(folder)
    log.info("Use folder: %s", str(folder))

    sample_args = folder.name

    for fl in folder.iterdir():
        fl_name = fl.name.lower()
        if 'quantification' in fl_name:
            cells_path = str(fl.absolute())
            break
    else:
        raise Exception("Couldn't find a file having `quantification` in "
                        "name to serve as cells data!")

else:
    if not args.sample:
        raise Exception("Positional argument `sample` was required or "
                        "use `--dir` option!")
    sample_args = args.sample

    if not args.cells:
        raise Exception("Positional argument `cells` was required!")
    cells_path = str(pathlib.Path(args.cells).absolute())

sample_args = sample_args.split('__', 1)
sample = dict(name=sample_args[0].strip())
if len(sample_args) > 1:
    sample['tag'] = sample_args[1].strip()

log.info("The sample info: {}.".format(sample))
log.info(f"The path to Cells: {cells_path}.")

with CycSession() as csess:
    csess.update_sample_feature_list(
        sample, cells_path)
