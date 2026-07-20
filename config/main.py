import sys
from pathlib import Path

# Automatically find the 'finflow' folder and add it to Python's search path
root_dir = Path(__file__).resolve().parents[1]  #moves to the root parent directory 'finflow'
sys.path.append(str(root_dir)) #adds the 'finflow' folder to the Python search path, allowing you to import modules from it

from config.package.logger import logger
from config.package.settings import PipelineConfig