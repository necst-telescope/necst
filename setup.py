from setuptools import setup

package_name = "necst"

setup(
    name=package_name,
    version="0.1.0",
    packages=[package_name],
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
        "console_scripts": [
            f"authorizer={package_name}.core.authorizer:main",
            f"tester={package_name}.tester:main",
            f"cli={package_name}.core.privileged_node:main",
        ],
    },
)
