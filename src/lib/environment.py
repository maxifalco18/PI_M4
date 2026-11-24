import sys
import os
import logging

def setup_environment():
    """
    Centralized Environment Setup for Glue/Spark.
    Removes the need for repetitive sys.path hacks in entry points.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Ascend to the 'src' parent (Root)
    root_dir = os.path.dirname(os.path.dirname(current_dir))
    
    if root_dir not in sys.path:
        sys.path.insert(0, root_dir)
        
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("PIM4_PLATFORM")
    return logger
