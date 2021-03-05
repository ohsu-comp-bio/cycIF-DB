""" Ingest cycif quantification result datasets into database.

Help:
python scripts/add_sample_complex.py --help
"""
import argparse
import json
import logging
import pathlib
import time

from cycif_db import CycSession


log = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument(
    'sample', type=str, nargs='?',
    help="Sample name and tag separated by `__`.")
parser.add_argument(
    'cells', type=str, nargs='?',
    help="The path to cells quantification data, in csv.")
parser.add_argument(
    'markers', type=str, nargs='?',
    help="The path to markers used for the sample, in csv.")
parser.add_argument(
    '--dir', '-d', dest='dir', type=str, required=False,
    help=("As an alternative input option, a folder containing bother cells "
          "and markers csv overrides other positional arguments. The folder "
          "name will be used as arguments for sample, separated by `__`.")
)
parser.add_argument(
    '--dry_run', default=False, action='store_true',
    help=("If enabled, run the add_sample_complex script without "
          "committing. Database will not be changed, as a result.")
)
parser.add_argument(
    '-v', '--verbose', default=False, action='store_true',
    help="Show detailed log.")

args = parser.parse_args()

if args.verbose:
    logging.basicConfig(level=logging.DEBUG)

folder = args.dir
sample_annotation = ''

if folder:
    folder = pathlib.Path(folder)
    log.info("Use folder: %s", str(folder))

    sample_args = folder.name

    cells_path, markers_path = '', ''
    for fl in folder.iterdir():
        fl_name = fl.name.lower()
        if 'quantification' in fl_name or '_quant.csv' in fl_name:
            cells_path = str(fl.absolute())
        elif 'markers.csv' in fl_name:
            markers_path = str(fl.absolute())
        elif fl.stem == 'annotation':
            with open(fl.absolute(), 'r') as fp:
                sample_annotation = fp.read()

    if not cells_path:
        raise Exception("Couldn't find a file having `quantification` in "
                        "name!")
    if not markers_path:
        raise Exception("Couldn't find a file having `markers.csv` in "
                        "name!")
else:
    if not args.sample:
        raise Exception("Positional argument `sample` was required or "
                        "use `--dir` option!")
    sample_args = args.sample

    if not args.cells:
        raise Exception("Positional argument `cells` was required!")
    cells_path = str(pathlib.Path(args.cells).absolute())

    if not args.markers:
        raise Exception("Positional argument `markers` was required!")
    markers_path = str(pathlib.Path(args.markers).absolute())

sample_args = sample_args.split('__', 1)
sample = dict(name=sample_args[0].strip())
if len(sample_args) > 1:
    sample['tag'] = sample_args[1].strip()
if sample_annotation:
    sample['annotation'] = json.loads(sample_annotation.strip())

log.info("The sample info: {}.".format(sample))
log.info(f"The path to Cells: {cells_path}.")
log.info(f"The path to Markers: {markers_path}.")

start_time = time.time()
with CycSession() as csess:
    csess.add_sample_complex(
        sample, cells_path, markers_path, dry_run=args.dry_run)
end_time = time.time()
log.info("Finished in %.10f s" % (end_time - start_time))
