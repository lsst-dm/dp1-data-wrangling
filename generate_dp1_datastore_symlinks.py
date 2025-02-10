import os
import sys

script_dir = os.path.dirname(os.path.abspath(__file__))
module_path = os.path.join(script_dir, "python")
sys.path.insert(0, module_path)

from lsst.dp1_data_wrangling.generate_dp1_file_tree import main

main()
