# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
import os
import sys
import sphinx_rtd_theme

# -- Path setup --------------------------------------------------------------

# Insert the project root directory into sys.path
sys.path.insert(0, os.path.abspath('../src'))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'mqtt_spb_wrapper'
copyright = '2024, Javier FG'
author = 'Javier FG'
release = "1.0.19"

# -- General configuration ---------------------------------------------------

extensions = [
    'sphinx.ext.autodoc',       # Core autodoc extension
    'sphinx.ext.napoleon',      # Support for NumPy and Google style docstrings
    'sphinx.ext.viewcode',      # Add links to highlighted source code
    'sphinx.ext.autosummary',   # Create summaries of modules/classes/functions
    'sphinx_autodoc_typehints', # Better type hint support
]

# Generate autosummary pages
autosummary_generate = True

# Include __init__ methods in documentation
autoclass_content = 'both'

# Configure Napoleon to parse Google style docstrings
napoleon_google_docstring = True
napoleon_numpy_docstring = False

# Set the master doc
master_doc = 'index'

# List of patterns to ignore
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Options for HTML output -------------------------------------------------

# Use Read the Docs theme
html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom themes here
html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]

# -- Extension configuration -------------------------------------------------

# Add any paths that contain templates here
templates_path = ['_templates']

# Static files (e.g., style sheets)
html_static_path = ['_static']
