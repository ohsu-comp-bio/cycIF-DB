""" Input result data from cycIF, csv file or DataFrame,
into database.

Help:
python scripts/add_sample_complex.py --help
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
    'sample', type=str,
    help="Sample name and annotation separated by `;`.")
parser.add_argument(
    'cells', type=str,
    help="The path to cells quantification data, in csv.")
parser.add_argument(
    'markers', type=str,
    help="The path to markers used for the sample, in csv.")

args = parser.parse_args()

sample_args = args.sample.split(';', 1)
if len(sample_args) == 1:
    sample = sample_args[0]
else:
    sample = dict(name=sample_args[0].strip(),
                  annotation=sample_args[1].strip())
log.info("The sample name: {}.".format(sample))

cells_path = str(pathlib.Path(args.cells).absolute())
log.info(f"The path to Cells: {cells_path}.")

markers_path = str(pathlib.Path(args.markers).absolute())
log.info(f"The path to Markers: {markers_path}.")

with CycSession() as csess:
    csess.add_sample_complex(
        sample, cells_path, markers_path)
