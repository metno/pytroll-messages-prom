#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2012 Trygve Aspenenes

# Author(s):

#   Trygve Aspenes <trygveas@met.no>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Setup for pytroll_messages_prom.
"""
from setuptools import setup
import imp

version = imp.load_source(
    'messages_prom.version', 'messages_prom/version.py')

setup(name="pytroll_messages_prom",
      version=version.__version__,
      description='Pytroll messages prom',
      author='Trygve Aspenes',
      author_email='trygveas@met.no',
      classifiers=["Development Status :: 3 - Alpha",
                   "Intended Audience :: Science/Research",
                   "License :: OSI Approved :: GNU General Public License v3 " +
                   "or later (GPLv3+)",
                   "Operating System :: OS Independent",
                   "Programming Language :: Python",
                   "Topic :: Scientific/Engineering"],
      url="",
      packages=['messages_prom',],
      scripts=['bin/pytroll-messages-prom.py',
               ],
      data_files=[],
      zip_safe=False,
      install_requires=['posttroll', 'prometheus_client',],
      tests_require=[],
      test_suite='',
      )
