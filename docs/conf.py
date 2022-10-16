import sys
from typing import Dict, List

from sphinx.application import Sphinx

# This documentation will be built without installing current project.
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
myst_heading_anchors = 3
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
    "pygment_light_style": "emacs",
    "pygment_dark_style": "dracula",
}
html_logo = "_static/logo.svg"
html_favicon = "https://avatars.githubusercontent.com/u/106944387?s=400&u=ddc959411de05d65ed4a64cc8b871d20a05ce395&v=4"  # noqa: E501
html_sidebars = {
    "**": [
        "version",
        "search-field.html",
        "sidebar-nav-bs.html",
    ],
}
html_context = {
    "default_mode": "dark",
}

html_static_path = ["_static"]
# html_css_files = ["css/custom.css"]

# -- Custom handler ----------------------------------------------------------


def summarize(
    app: Sphinx,
    what: str,
    name: str,
    obj: object,
    options: Dict[str, str],
    lines: List[str],
):
    import inspect

    def _get_attr(attrname: str):
        return getattr(obj, attrname, None)

    def _is_to_be_documented(attrname: str):
        if attrname.startswith("_"):
            return False
        attr = _get_attr(attrname)
        if attr is None:
            return False
        if inspect.ismodule(attr):
            return False
        module_name = getattr(attr, "__module__", "")
        if not module_name.startswith(project):
            return False
        if name == module_name:
            return False
        return True

    def _create_table(attr_names: List[str]):
        ret = [".. csv-table::", "   :widths: auto", ""]
        for attr in attr_names:
            link = f":doc:`{attr} <{_get_attr(attr).__module__}>`"
            docs = str(getattr(_get_attr(attr), "__doc__", "")).split("\n")[0]
            ret.append(f'   {link}, "{docs}"')
        return ret

    if what == "module":
        alias_names = [attr for attr in dir(obj) if _is_to_be_documented(attr)]
        if len(alias_names) > 0:
            lines.extend(["=======", "Aliases", "=======", ""])
            lines.extend(_create_table(alias_names))


def setup(app: Sphinx) -> None:
    app.connect("autodoc-process-docstring", summarize)
