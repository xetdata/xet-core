import os
import sys

os.environ['SPHINX_BUILD'] = "1"
sys.path.insert(0, os.path.abspath('../python/pyxet'))

import pyxet

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'pyxet'
copyright = '2023, XetHub'
author = '<a href="https://xethub.com/xdssio" target="_blank">Jonathan Alexander</a>, <a href="https://xethub.com/team" target="_blank">XetHub team</a>, and the <a href="https://github.com/xetdata/pyxet" target="_blank">pyxet</a> community'
release = pyxet.__version__
print(f"Generating for pyxet {release}")

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["myst_parser", "sphinx.ext.autodoc", 'sphinx.ext.autosummary', "sphinx.ext.napoleon", "sphinx.ext.viewcode", "sphinx_rtd_theme",
              "sphinx_book_theme"]
autosummary_generate = True
templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', 'docs', '.venv']

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_book_theme"

html_static_path = ['_static']

