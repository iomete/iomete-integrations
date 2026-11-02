import importlib.metadata
import unittest

from iomete_airflow_plugin.hook import IometeHook
from iomete_airflow_plugin.iomete_operator import IometeOperator


class TestIometePlugin(unittest.TestCase):
    def test_entry_point_loads(self):
        (entry_point,) = [ep for ep in importlib.metadata.entry_points(group="airflow.plugins") if ep.name == "iomete"]
        plugin = entry_point.load()
        self.assertEqual(plugin.name, "iomete")
        self.assertEqual(plugin.operators, [IometeOperator])
        self.assertEqual(plugin.hooks, [IometeHook])
