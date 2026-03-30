import sys
import os

# Ensure the root directory is in the path so we can import from 'src'
# This is necessary because Glue jobs often run with only the entry point's directory in PYTHONPATH
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from src.components.dq.validator import DataQualityValidator
