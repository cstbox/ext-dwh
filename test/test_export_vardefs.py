#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import json
import os

from pycstbox.dwh.filters import VariableDefsExportFilter
from pycstbox.devcfg import DeviceNetworkConfiguration
from pycstbox import devcfg


class TestVariableDefsExportNoMetadata(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        devcfg.METADATA_HOME = fixture_path('devcfg.d')

    def setUp(self):
        self.filter = VariableDefsExportFilter("test_site")

    def test_01(self):
        """ Checks all is working as expected with an empty configuration
        """
        cfg = {}
        js = self.filter.export_variable_definitions(cfg)
        self.assertNotEqual(len(js), 0)

        cfg_reloaded = json.loads(js)
        self.assertIsInstance(cfg_reloaded, list)
        self.assertEqual(len(cfg_reloaded), 0)

    def test_02(self):
        """ Checks that the mandatory site code has been provided
        """
        # override the default fixture for this test
        with self.failUnlessRaises(ValueError):
            self.filter = VariableDefsExportFilter(None)
        with self.failUnlessRaises(ValueError):
            self.filter = VariableDefsExportFilter("")

    def test_03(self):
        """ Checks with a minimal configuration and no vars metadata (=> all enabled outputs are exported)
        """
        test_scenarios = (
            ('device_config_01.json', 7),
            ('device_config_02.json', 5)
        )

        for devcfg_name, enabled_outputs in test_scenarios:
            dev_cfg = DeviceNetworkConfiguration(fixture_path(devcfg_name), autoload=True)
            js = self.filter.export_variable_definitions(dev_cfg)
            var_defs = json.loads(js)
            self.assertEqual(len(var_defs), enabled_outputs)


class TestVariableDefsExportWithMetadata(unittest.TestCase):
    devcfg_name = 'device_config_02.json'
    enabled_outputs_cnt = 5

    @classmethod
    def setUpClass(cls):
        devcfg.METADATA_HOME = fixture_path('devcfg.d')
        cls.dev_cfg = DeviceNetworkConfiguration(fixture_path(cls.devcfg_name), autoload=True)

    def test_01(self):
        """ Checks with vars metadata
        """
        vars_meta = json.load(file(fixture_path('vars_metadata.json')))
        self.filter = VariableDefsExportFilter("test_site", vars_metadata=vars_meta)

        js = self.filter.export_variable_definitions(self.dev_cfg)

        var_defs = json.loads(js)
        self.assertEqual(len(var_defs), self.enabled_outputs_cnt)

        d = dict([
            (vdef['varname'], vdef) for vdef in var_defs
        ])
        vd = d['temp_living']
        self.assertEqual(vd['lower_bound'], 10)
        self.assertEqual(vd['upper_bound'], 50)
        self.assertEqual(vd['delta_min'], -5)
        self.assertEqual(vd['delta_max'], 5)

        vd = d['nrj']
        self.assertEqual(vd['lower_bound'], 0)
        self.assertIsNone(vd['upper_bound'])

    def test_02(self):
        """ Filter the exported variables by not including them in the metadata
        """
        # patch the metadata to remove one of the enabled sensors
        vars_meta = json.load(file(fixture_path('vars_metadata.json')))
        del vars_meta['living_mvt']

        self.filter = VariableDefsExportFilter("test_site", vars_metadata=vars_meta)
        js = self.filter.export_variable_definitions(self.dev_cfg)

        var_defs = json.loads(js)
        self.assertEqual(len(var_defs), self.enabled_outputs_cnt - 1)


_HERE_ = os.path.dirname(__file__)


def fixture_path(name):
    return os.path.join(_HERE_, 'fixtures', name)

if __name__ == '__main__':
    unittest.main()
