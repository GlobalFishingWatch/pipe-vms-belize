#!/usr/bin/env python

"""
Setup script for pipe-vms-belize
"""

from pipe_tools.beam.requirements import requirements as DATAFLOW_PINNED_DEPENDENCIES

from setuptools import setup

import codecs

PACKAGE_NAME='pipe-vms-belize'

package = __import__('pipe_vms_belize')

DEPENDENCIES = [
    "google-cloud-storage==1.22.0",
    "jinja2-cli",
    "pipe-tools==3.1.2",
    "requests==2.24.0",
    "xmltodict==0.12.0"
]

with codecs.open('README.md', encoding='utf-8') as f:
    readme = f.read().strip()

with codecs.open('requirements.txt', encoding='utf-8') as f:
    DEPENDENCY_LINKS=[line for line in f]

setup(
    author=package.__author__,
    author_email=package.__email__,
    description=package.__doc__.strip(),
    include_package_data=True,
    install_requires=DEPENDENCIES + DATAFLOW_PINNED_DEPENDENCIES,
    license=package.__license__.strip(),
    long_description=readme,
    name=PACKAGE_NAME,
    url=package.__source__,
    version=package.__version__,
    zip_safe=True,
    dependency_links=DEPENDENCY_LINKS
)
