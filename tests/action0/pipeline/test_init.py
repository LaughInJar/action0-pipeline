import re
import unittest

import action0.pipeline


class PackageTestCase(unittest.TestCase):
    """
    tests for the :py:mod:`action0.pipeline` package root
    """

    def test_version(self) -> None:
        """
        Test that the version is a non-empty x.y.z string.
        """
        self.assertRegex(action0.pipeline.__version__, re.compile(r"^\d+\.\d+\.\d+$"))

    def test_all_is_exported(self) -> None:
        """
        Test that every name in __all__ is actually importable from the root.
        """
        for name in action0.pipeline.__all__:
            self.assertTrue(hasattr(action0.pipeline, name), name)
