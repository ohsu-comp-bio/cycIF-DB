""" Insert or Sync stock markers to database.

Help:
python scripts/insert_or_sync_stock_markers.py
"""
import logging
import pathlib
import sys

work_dir = pathlib.Path(__file__).absolute().parent.parent
sys.path.insert(1, str(work_dir))
from cycif_db import CycSession

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

with CycSession() as csess:
    csess.insert_or_sync_markers()
