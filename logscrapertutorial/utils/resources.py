"""
A utility to normalize the resources API between Python versions.
"""
import atexit
from contextlib import ExitStack
from pathlib import Path

try:
    # Use the better API provided in 3.7
    from importlib.resources import path as resource_path


    def get_pkg_resource_path(package: str, resource: str) -> Path:
        """The new package resource API returns a context manager.
        Use this function to safely get the path.
        """
        file_manager = ExitStack()
        atexit.register(file_manager.close)
        return file_manager.enter_context(resource_path(package, resource))

except ImportError:
    # Fall back on the older one.
    from pkg_resources import resource_filename as resource_path


    def get_pkg_resource_path(package: str, resource: str) -> Path:
        """Old API returns a string, so normalize to a path.
        """
        # noinspection PyTypeChecker
        return Path(resource_path(package, resource))