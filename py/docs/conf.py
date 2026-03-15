# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys
from pathlib import Path

# Set environment variable to indicate we're building docs
os.environ['SPHINX_BUILD'] = 'true'

# Add source directory to path for autodoc
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

# Also add the generated client to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'antfly-client'))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'antfly-sdk'
copyright = '2025, ajroetker@antfly.io'
author = 'ajroetker@antfly.io'
release = '0.1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autodoc.typehints',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx',
]

# Mock imports that might not be available during docs build
autodoc_mock_imports = [
    'httpx',
    'attrs',
    'antfly_client',
    'antfly_client.api',
    'antfly_client.api.api_table',
    'antfly_client.api.api_index',
    'antfly_client.models',
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# Autodoc settings
autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'special-members': '__init__',
    'undoc-members': True,
    'exclude-members': '__weakref__'
}

# Napoleon settings for Google/NumPy style docstrings
napoleon_google_docstring = True
napoleon_numpy_docstring = True

# Intersphinx mapping for linking to other project docs
intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
}

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']
