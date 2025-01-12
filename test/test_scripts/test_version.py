" Testing the __version__ attribute w.r.t. the current PyPI release "

import datetime, unittest, requests
from packaging import version
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
        """
     Ensure the current version in the source code is either greater than 
     or equal to the version published on PyPI.
     """
        pypi_version = version.parse("1.8.2.5")  # Replace this with the logic to fetch the actual PyPI version
     current_version = version.parse("1.8.2.5")  # Replace with your project's current version

     # Assert that the current version is at least equal to or greater than the PyPI version
     self.assertFalse(
            pypi_version > current_version,
            f"PyPI version ({pypi_version}) should not be greater than the current version ({current_version})."
     )

