import sys

# This package will build documentation without virtualenv nor installing itself.
sys.path.append("../")

import necst  # noqa: E402


# -- Project information -----------------------------------------------------

project = "necst"
copyright = "2022, NECST Developers"
author = "NECST Developers"
release = version = necst.__version__

# -- General configuration ---------------------------------------------------

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

autosummary_generate = True
add_module_names = False
autodoc_member_order = "bysource"
autodoc_typehints_format = "short"
autodoc_typehints = "description"
napoleon_use_admonition_for_notes = True
napoleon_use_ivar = True

# -- Options for HTML output -------------------------------------------------

html_theme = "pydata_sphinx_theme"
html_theme_options = {
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/necst-telescope/necst",
            "icon": "fab fa-github-square",
            "type": "fontawesome",
        },
        {
            "name": "PyPI",
            "url": "https://pypi.org/project/necst/",
            "icon": "fas fa-cubes",
            "type": "fontawesome",
        },
    ],
    "navbar_start": ["navbar-logo"],
}
# html_logo = "_static/logo.svg"
html_favicon = "https://avatars.githubusercontent.com/u/106944387?s=400&u=ddc959411de05d65ed4a64cc8b871d20a05ce395&v=4"  # noqa: E501
html_sidebars = {
    "**": [
        "version",
        "search-field.html",
        "sidebar-nav-bs.html",
    ],
}

html_static_path = ["_static"]
html_css_files = ["css/custom.css"]
