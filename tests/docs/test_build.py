import sys
import subprocess
from pathlib import Path

import pytest


project_root = Path(__file__).parent.parent.parent
python_version = sys.version_info

PKG_NAME = "necst"


@pytest.fixture
def tmp_project_dir(tmp_path_factory) -> Path:
    project_dir = tmp_path_factory.mktemp(PKG_NAME)
    _ = subprocess.run(["cp", "-rv", ".", str(project_dir)], cwd=project_root)
    return project_dir


class TestBuildDocs:

    COMMAND = {
        "apidoc": lambda rootdir: [
            "sphinx-apidoc",
            "-efTM",
            "-t",
            f"{str(rootdir)}/docs/_templates/apidoc",
            "-o",
            f"{str(rootdir)}/docs/_source",
            PKG_NAME,
            f"{PKG_NAME}/console",
        ],
        "build": lambda rootdir: [
            "sphinx-build",
            "-W",
            "-a",
            f"{str(rootdir)}/docs",
            f"{str(rootdir)}/docs/_build",
        ],
    }

    def test_create_stub(self, tmp_project_dir: Path):
        assert (tmp_project_dir / "docs" / "conf.py").exists()

        result = subprocess.run(
            ["python3", "-m", *self.COMMAND["apidoc"](tmp_project_dir)],
            capture_output=True,
        )
        assert result.returncode == 0

    def test_build(self, tmp_project_dir: Path):
        _ = subprocess.run(["python3", "-m", *self.COMMAND["apidoc"](tmp_project_dir)])

        assert (tmp_project_dir / "docs" / "conf.py").exists()
        result = subprocess.run(
            ["python3", "-m", *self.COMMAND["build"](tmp_project_dir)],
            capture_output=True,
        )
        print(result.stderr, result.stdout)
        assert result.returncode == 0
