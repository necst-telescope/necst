from pathlib import Path
from setuptools import setup, find_packages
from typing import List

package_name = "necst"


def get_executor_entrypoints() -> List[str]:
    """Get entrypoint expression for every executor-definition files found.

    This function should never be executed in ``setuptools.setup``, as the function may
    be executed in different directory, causing incorrect ``__file__``.

    """
    root = Path(__file__).parent / package_name
    exec_files = root.glob("**/exec_*.py")

    def _parse(path: Path) -> str:
        cmd = path.stem.strip("exec_")
        path_with_no_extension = path.with_suffix("")
        dot_separated_path = str(path_with_no_extension).replace("/", ".")
        return f"{cmd}={package_name}.{dot_separated_path}:main"

    paths_in_pkg = [f.relative_to(root) for f in exec_files]
    return [_parse(p) for p in paths_in_pkg]


executor_entrypoints = get_executor_entrypoints()


setup(
    name=package_name,
    version="0.2.3",
    packages=find_packages(),
    data_files=[
        ("share/ament_index/resource_index/packages", ["resource/" + package_name]),
        ("share/" + package_name, ["package.xml"]),
    ],
    install_requires=["setuptools"],
    zip_safe=True,
    maintainer="Kaoru Nishikawa",
    maintainer_email="k.nishikawa@a.phys.nagoya-u.ac.jp",
    description="NEw Control System for Telescope",
    license="MIT",
    tests_require=["pytest"],
    entry_points={
        "console_scripts": executor_entrypoints
        + [
            # Custom paths will be added here
        ],
    },
)
