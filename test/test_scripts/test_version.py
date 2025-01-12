" Testing the __version__ attribute w.r.t. the current PyPI release "

import datetime, unittest, requests
from packaging.version import parse, InvalidVersion
import openmsistream


class TestVersion(unittest.TestCase):
    """
    Test that the openmsistream.__version__ attribute has been incremented from
    the version in the current PyPI release
    """

    def get_latest_pypi_version_and_date(self, package_name):
        "Get the latest version of a package from PyPI"
        try:
            response = requests.get(
                f"https://pypi.org/pypi/{package_name}/json", timeout=30
            )
            response.raise_for_status()
            data = response.json()
            version = data["info"]["version"]
            release_date = datetime.datetime.fromisoformat(
                data["releases"][version][0]["upload_time"]
            )
            release_date = release_date.replace(tzinfo=datetime.timezone.utc)
            return parse(version), release_date
        except Exception as exc:
            raise NameError(
                f"Failed to fetch the latest PyPI version and date for {package_name}: {exc}"
            ) from exc

    def test_version_incremented(self):
        """Make sure the current version from PyPI is less than the version from the
        package as it is right now"""
        pypi_version, release_date = self.get_latest_pypi_version_and_date(
            "openmsistream"
        )
        try:
            current_version = parse(openmsistream.__version__)
        except InvalidVersion as exc:
            raise ValueError(
                f"Version string {openmsistream.__version__} is not valid!"
            ) from exc
        current_time = datetime.datetime.now().astimezone(datetime.timezone.utc)
        if (current_time - release_date).total_seconds() / 60.0 < 720.0:
            self.assertTrue(
                pypi_version == current_version,
                (
                    f"PyPI version ({pypi_version}) does not match the current version "
                    f"({current_version}) but the release is less than twelve hours old."
                ),
            )
        else:
            self.assertTrue(
                pypi_version < current_version,
                (
                    f"PyPI version ({pypi_version}) is not less than the current version "
                    f"({current_version}). Did you update it yet?"
                ),
            )
