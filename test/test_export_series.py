#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import datetime
import os
import logging
import requests
import json
import tempfile
import zipfile

from pycstbox.events import TimedEvent

from pycstbox.dwh.filters import EventsExportFilter
from pycstbox.dwh.process import DWHEventsExportJob, ProcessConfiguration, PARM_EXTRACT_DATE

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'


def _create_events():
    return [
            TimedEvent(datetime.datetime(2015, 11, 04, 0, m), vtype, vname, {'value': m})
            for m, (vtype, vname) in enumerate((
                ('type1', 'var10'),
                ('type2', 'var20'),
                ('type1', 'var10'),
                ('type3', 'var30'),
                ('type1', 'var11'),
                ('type2', 'var21'),
            ))
        ]


class TestCase01(unittest.TestCase):
    class MockResponse(object):
        ok = None
        message = None

    def mock_post(self, url, files=None, **kwargs):
        zip = files['zip']
        zip.seek(0)

        tmp = tempfile.NamedTemporaryFile(suffix='.zip', delete=False)
        tmp.write(zip.read())
        tmp.close()
        self.tmp = tmp

        resp = self.MockResponse()
        resp.ok = True
        resp.text = json.dumps({
            'message': 'OK',
            'jobID': 42
        })

        return resp

    @classmethod
    def setUpClass(cls):
        cls.events = _create_events()

    def setUp(self):
        self.filter = EventsExportFilter("unittest")
        requests.post = self.mock_post
        self.tmp = None

    def test_01(self):
        count, files = self.filter.export_events(events=self.events)

        self.assertEqual(count, 6)
        self.assertEqual(len(files), 5)
        self.assertSetEqual(set(files), {
            '/tmp/type1_var10.tsv',
            '/tmp/type1_var11.tsv',
            '/tmp/type2_var20.tsv',
            '/tmp/type2_var21.tsv',
            '/tmp/type3_var30.tsv',
        })

    def test_02(self):
        job_cfg = ProcessConfiguration()
        job_cfg.load_dict({
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
        job_parms = {
            PARM_EXTRACT_DATE: datetime.datetime(2015, 11, 03, 0, 0)
        }
        job = DWHEventsExportJob(jobname='unittest', jobid=42, config=job_cfg, parms=job_parms)
        # avoid cluttering unit tests report with logging
        job.log_setLevel(logging.ERROR)

        count, files = self.filter.export_events(events=self.events)
        arch_name = job.create_archive(files, datetime.datetime(2015, 11, 04, 0, 0))
        self.assertTrue(arch_name)
        self.assertTrue(os.path.isfile(arch_name))

        try:
            job._archive = arch_name
            job.send_data()

            self.assertIsNotNone(self.tmp)
            arch = zipfile.ZipFile(self.tmp.name)
            self.assertTrue(arch.filelist)
            self.assertEqual(len(arch.filelist), 5)

        finally:
            job.cleanup()


_HERE_ = os.path.dirname(__file__)


def fixture_path(name):
    return os.path.join(_HERE_, 'fixtures', name)

