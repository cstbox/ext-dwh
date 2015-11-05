#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import json
import os
import tempfile
import logging

import requests

from pycstbox.devcfg import DeviceNetworkConfiguration
from pycstbox import devcfg

from pycstbox.dwh.filters import VariableDefsExportFilter
from pycstbox.dwh.process import DWHVariableDefinitionsExportProcess, ProcessConfiguration


class TestFilter01(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        devcfg.METADATA_HOME = fixture_path('devcfg.d')

    def setUp(self):
        self.filter = VariableDefsExportFilter("unittest")

    def test_01(self):
        """ Checks all is working as expected with an empty configuration
        """
        cfg = {}
        defs = self.filter.export_variable_definitions(cfg)
        self.assertIsInstance(defs, list)
        self.assertEqual(len(defs), 0)

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
            var_defs = self.filter.export_variable_definitions(dev_cfg)
            self.assertEqual(len(var_defs), enabled_outputs)


class TestFilter02(unittest.TestCase):
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

        var_defs = self.filter.export_variable_definitions(self.dev_cfg)

        self.assertEqual(len(var_defs), self.enabled_outputs_cnt)

        d = dict([
            (entry['varname'], entry) for entry in var_defs
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
        var_defs = self.filter.export_variable_definitions(self.dev_cfg)

        self.assertEqual(len(var_defs), self.enabled_outputs_cnt - 1)


class TestProcess01(unittest.TestCase):
    devcfg_name = 'device_config_01.json'
    enabled_outputs_cnt = 7

    class MockResponse(object):
        ok = None
        message = None

    def mock_post(self, url, data=None, **kwargs):
        data.seek(0)

        tmp = tempfile.NamedTemporaryFile(suffix='.json', delete=False)
        tmp.write(data.read())
        tmp.close()
        self.tmp = tmp

        resp = self.MockResponse()
        resp.ok = True
        resp.text = json.dumps({'message': 'OK'})

        return resp

    @classmethod
    def setUpClass(cls):
        devcfg.METADATA_HOME = fixture_path('devcfg.d')
        cls.dev_cfg = DeviceNetworkConfiguration(fixture_path(cls.devcfg_name), autoload=True)
        cls.vars_meta = json.load(file(fixture_path('vars_metadata.json')))

    def setUp(self):
        self.process = DWHVariableDefinitionsExportProcess()
        self.process_cfg = ProcessConfiguration()
        self.process_cfg.load_dict({
            ProcessConfiguration.Props.SITE_CODE: 'unit-test',
            ProcessConfiguration.Props.REPORT_TO: 'john.doe@acme.com',
            ProcessConfiguration.Props.SERVER: {
                ProcessConfiguration.Props.HOST: 'unittest',
                ProcessConfiguration.Props.AUTH: {
                    ProcessConfiguration.Props.LOGIN: 'john.doe',
                    ProcessConfiguration.Props.PASSWORD: 'letmein'
                }
            }
        })
        # avoid cluttering unit tests report with logging
        self.process.logger.setLevel(logging.ERROR)

        # monkey patch requests module
        requests.post = self.mock_post

    def test_01(self):
        rc = self.process.run(self.process_cfg, self.dev_cfg, self.vars_meta)
        self.assertEqual(rc, 0)

        try:
            with file(self.tmp.name) as fp:
                defs = json.load(fp)

            self.assertEqual(len(defs), self.enabled_outputs_cnt)
            d = dict([
                (entry['varname'], entry) for entry in defs
            ])
            for v in self.vars_meta:
                self.assertIn(v, d)

        finally:
            os.remove(self.tmp.name)

_HERE_ = os.path.dirname(__file__)


def fixture_path(name):
    return os.path.join(_HERE_, 'fixtures', name)

if __name__ == '__main__':
    unittest.main()
