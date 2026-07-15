"""Smoke checks for the package scaffold."""

import importlib
import pkgutil
import unittest

import telemetry


class ScaffoldTests(unittest.TestCase):
    def test_package_modules_import(self):
        for module in pkgutil.walk_packages(telemetry.__path__, "telemetry."):
            with self.subTest(module=module.name):
                importlib.import_module(module.name)
