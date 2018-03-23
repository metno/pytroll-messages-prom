#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2018 Trygve Aspenenes

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

"""Setup for pytroll_handle messages.
"""
from setuptools import setup
import imp

version = imp.load_source(
    'handle_messages.version', 'handle_messages/version.py')

setup(name="pytroll_handle_messages",
      version=version.__version__,
      description='Pytroll handle messages',
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
      packages=['handle_messages',],
      scripts=['bin/pytroll-handle-messages.py',
               ],
      data_files=[],
      zip_safe=False,
      install_requires=['posttroll', 'trollsift', 'netifaces',
                        ],
      tests_require=[],
      test_suite='',
      )
