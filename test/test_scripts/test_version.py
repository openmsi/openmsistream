import datetime
import requests
import pytest
from packaging.version import parse, InvalidVersion
import openmsistream


@pytest.fixture
def get_latest_pypi_version_and_date():
    """
    Helper to fetch latest PyPI version + upload timestamp.
    Returned as a callable for readability.
    """
    def _fetch(package_name):
        try:
            resp = requests.get(
                f"https://pypi.org/pypi/{package_name}/json",
                timeout=30
            )
            resp.raise_for_status()
            data = resp.json()

            version = data["info"]["version"]
            release_date = datetime.datetime.fromisoformat(
                data["releases"][version][0]["upload_time"]
            ).replace(tzinfo=datetime.timezone.utc)

            return parse(version), release_date

        except Exception as exc:
            raise RuntimeError(
                f"Failed to fetch PyPI metadata for {package_name}: {exc}"
            ) from exc

    return _fetch


def test_version_incremented(get_latest_pypi_version_and_date):
    """Verify the local __version__ is ahead of or equal to the latest PyPI version
    (equal only if PyPI release is <12 hours old)."""
    pypi_version, release_date = get_latest_pypi_version_and_date("openmsistream")

    try:
        current_version = parse(openmsistream.__version__)
    except InvalidVersion as exc:
        raise ValueError(
            f"Invalid version string: {openmsistream.__version__}"
        ) from exc

    now = datetime.datetime.now(datetime.timezone.utc)
    twelve_hours = 12 * 60  # minutes

    age_minutes = (now - release_date).total_seconds() / 60.0

    if age_minutes < twelve_hours:
        assert pypi_version == current_version, (
            f"PyPI version ({pypi_version}) does not match current version ({current_version}) "
            f"but the release is less than 12 hours old."
        )
    else:
        assert pypi_version < current_version, (
            f"PyPI version ({pypi_version}) is not less than current version ({current_version}). "
            "Did you update it yet?"
        )
