import re
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _dockerfile_args() -> dict[str, str]:
    content = (REPO_ROOT / "Dockerfile_dagster").read_text()
    return dict(re.findall(r"^ARG (\w+)=(\S+)$", content, flags=re.MULTILINE))


def _locked_version(package: str) -> str:
    lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text())
    return next(p["version"] for p in lock["package"] if p["name"] == package)


def test_dagster_image_matches_lockfile():
    """The webserver/daemon image runs the same Dagster versions as the code location."""
    args = _dockerfile_args()

    assert args["DAGSTER_VERSION"] == _locked_version("dagster")
    assert args["DAGSTER_LIBS_VERSION"] == _locked_version("dagster-postgres")
