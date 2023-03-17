import subprocess
import sys
from pathlib import Path

import pytest

project_root = Path(__file__).parent.parent.parent
python_version = sys.version_info

PKG_NAME = "necst"


@pytest.fixture
def tmp_project_dir(tmp_path_factory) -> Path:
    project_dir = tmp_path_factory.mktemp(PKG_NAME)

    # Copy only tracked files
    # Copy only files tracked by git. Git command won't be available in minimum virtual
    # environments, but since this project is managed by git/GitHub, the test runner
    # should have git installed.
    # https://superuser.com/questions/1219553/is-there-a-git-cp-command-that-can-copy-only-tracked-files-like-svn-cp
    subprocess.run(["git", "clone", ".", str(project_dir)], cwd=project_root)

    return project_dir


class TestBuildDocs:
    COMMAND = {
        "apidoc": lambda rootdir: [
            "sphinx-apidoc",
            "-efTM",
            "-t",
            f"{rootdir!s}/docs/_templates/apidoc",
            "-o",
            f"{rootdir!s}/docs/_source",
            PKG_NAME,
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
            self.COMMAND["apidoc"](tmp_project_dir),
            capture_output=True,
        )
        assert result.returncode == 0

    def test_build(self, tmp_project_dir: Path):
        _ = subprocess.run(self.COMMAND["apidoc"](tmp_project_dir))

        assert (tmp_project_dir / "docs" / "conf.py").exists()
        result = subprocess.run(
            self.COMMAND["build"](tmp_project_dir),
            capture_output=True,
        )
        print(result.stderr, result.stdout)
        assert result.returncode == 0
