# Configuration file for the Sphinx documentation builder.

import os
import sys

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../finance"))
)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

project = "Finance Data Pipeline"
copyright = "2024, Amir Nazary"
author = "Amir Nazary"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx_rtd_theme",
    "myst_parser",
    "sphinx_copybutton",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "furo"

pygments_style = "sphinx"
pygments_dark_style = "monokai"

html_static_path = ["_static"]
html_css_files = ["custom.css"]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}
