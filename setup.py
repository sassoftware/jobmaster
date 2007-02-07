#
# Copyright (c) 2007 rPath, Inc.
#
# All rights reserved
#

# dataManifest is a list of relative paths to include in the data tarball
dataManifest = ['config', 'bin', 'jobmaster.init', 'jobmaster.tmpwatch']

import os

from jobmaster import constants
from conary.lib import util

# do standard setuptools inclusion
from setuptools import setup, find_packages
setup(
    name = "jobmaster",
    version = constants.version,
    packages = find_packages(),
)

# include data separately to avoid complicated setuptools rules
os.system('tar -czvO %s > "dist/jobmaster-data-%s.tar.gz"' % \
              ('"' + '" "'.join(dataManifest) + '"', constants.version))

# force deletion of .egg-info dirs
for eggDir in [x for x in \
                   os.listdir(os.path.split(os.path.abspath(__file__))[0]) \
                   if x.endswith('.egg-info')]:
    util.rmtree(os.path.abspath(eggDir))
