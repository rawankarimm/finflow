import sys
from pathlib import Path

root_dir = Path(__file__).resolve().parents[1] 
sys.path.append(str(root_dir))

#Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass 
#C:\Users\rawan\.venv\Scripts\Activate.ps1