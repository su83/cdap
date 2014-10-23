# -*- coding: utf-8 -*-

import sys
import os

# Import the common config file
# Note that paths in the common config are interpreted as if they were 
# in the location of this file

sys.path.insert(0, os.path.abspath('../../_common'))
from common_conf import * 

# Override the common config

html_short_title = u'CDAP Tutorials'
# Remove this guide from the mapping as it will fail as it has been deleted by clean
intersphinx_mapping.pop("tutorials", None)