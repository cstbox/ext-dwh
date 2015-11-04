#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This file is part of CSTBox.
#
# CSTBox is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# CSTBox is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with CSTBox.  If not, see <http://www.gnu.org/licenses/>.

""" DataWareHouse data export process building blocks.

This module defines the following classes :
    - DWHEventsExportJob:
        a class derived from the pycstbox generic EventsExportJob, adding the
        specific aspects of the DataWareHouse context

    - DWHEventsExportProcess:
        a class which encapsulates the whole process which is executed at the
        scheduled times. It takes care of re-executing failed jobs if any by
        using the backlog mechanism provided by pycstbox.export.Backlog class.

    - DWHConfigurationExportProcess:
        a class implementing the process of uploading the point definitions
        corresponding to a device network configuration. Immediate retries are
        handled in case of network transfer failure, but no backlog mechanism
        is used here.
"""

import os
import datetime
import tempfile
import time
import zipfile
import json
import requests
import jsonschema
import copy

from pycstbox.log import Loggable
import pycstbox.export
from pycstbox import evtdao
from pycstbox.config import GlobalSettings
from pycstbox.dwh.filters import EventsExportFilter, VariableDefsExportFilter, LINE_END
from pycstbox.dwh.pending_jobs_queue import PendingJobsQueue
from pycstbox.events import VarTypes
from pycstbox.dwh import DWHException

__author__ = 'Eric PASCUAL - CSTB (eric.pascual@cstb.fr)'

PARM_EXTRACT_DATE = 'date'

gs = GlobalSettings()

TEMP_FILES_TIMESTAMP_FORMAT = '%Y%m%d-%H%M%S.%f'


class DWHEventsExportJob(pycstbox.export.EventsExportJob):
    """ A specialized EventsExportJob for exporting sensor events to the
    DataWareHouse server.  """

    def __init__(self, jobname, jobid, parms, config):
        super(DWHEventsExportJob, self).__init__(jobname, jobid, parms)
        self._archive = None
        self._config = config

    def export_events(self):
        """ Creates a ZIP archive containing the time series of the variables to be exported.

        The generated file name is placed in the private attribute ''self._archive'' for later use
        by the sending step.

        :return: the exported events count
        """
        evt_count = 0
        self._archive = None

        filter_ = EventsExportFilter(self._config.site_code, prefix_with_type=False)
        extract_date = self._parms[PARM_EXTRACT_DATE]
        with evtdao.get_dao(gs.get('dao_name')) as dao:
            events = dao.get_events_for_day(extract_date, var_type=VarTypes.ENERGY)
            if events:
                evt_count, series_files = filter_.export_events(events)
                self._archive = self.create_archive(series_files, time_stamp=datetime.datetime.utcnow())

        return evt_count

    def create_archive(self, series_files, time_stamp, cleanup=True):
        """ Creates the archive to be sent, as a temp file packaging created series files.

        :param list series_files: the list of series files
        :param datetime.datetime time_stamp: the archive time stamp
        :param bool cleanup: if True, series files are deleted after the archive has been created
        :return: the generated archive file name, built from the site name and the provided time
        stamp
        """
        archive_name = "/tmp/%s-%s.zip" % (
            self._config.site_code, time_stamp.strftime(TEMP_FILES_TIMESTAMP_FORMAT)
        )
        with zipfile.ZipFile(archive_name, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
            for series_file in series_files:
                archive.write(series_file, os.path.basename(series_file))

        # cleanup individual files if requested
        if cleanup:
            for f in series_files:
                os.remove(f)

        return archive_name

    def send_data(self):
        if not self._archive:
            self.log_warn('No archive previously created. We should not have been called.')
            return

        url = self._config.data_upload_url % self._config.site_code

        with file(self._archive, 'rb') as archive:
            self.log_info('uploading file %s using URL %s', self._archive, url)
            resp = requests.post(
                url,
                files={
                    'zip': archive
                },
                auth=(self._config.login, self._config.password)
            )

        self.log_info('%s - %s', resp, resp.text)
        if resp.ok:
            resp_data = json.loads(resp.text)
            job_id = resp_data['jobID']

            self.log_info('upload successful (%s) - job id=%s' % (resp_data['message'], job_id))

            # nothing else to be done : data have been transmitted
            # if anomalies are detected during the processing, they will be notified to the
            # site manager, and there is no need to activate the backlog retry mechanism here,
            # since it will not fix the anomaly but just reproduce it again and again

            # Anyway, for record's sake, we go one step further by monitoring the job completion status
            # and log the result

            # add the job id to the persistent queue
            queue = PendingJobsQueue()
            queue.append(job_id)

        else:
            try:
                msg = 'failed : %d - %s (%s)' % (resp.status_code, resp.reason, resp.text)
            except ValueError:
                msg = 'unexpected server error : %d - %s' %(resp.status_code, resp.reason)
            self.log_error(msg)
            raise pycstbox.export.ExportError(msg)

    def cleanup(self, error=None):
        """ Final cleanup.

        Removes the generated archive file if any.
        """
        if self._archive:
            if not self._config.debug:
                os.remove(self._archive)
            else:
                self.log_warn('running in debug mode : temp file %s not deleted', self._archive)
            self._archive = None


class DWHEventsExportProcess(Loggable):
    """ Encapsulation of the complete jobs processing chain forsensor events
    export to DataWareHouse, including backlog handling, re-run of failed former
    attempts,...
    """
    ERR_NONE = 0
    ERR_MULTIPLE = 999

    err_messages = {
        ERR_NONE: 'successful',
        ERR_MULTIPLE: 'error on more than 1 job'
    }

    def __init__(self):
        Loggable.__init__(self, logname='evt-expproc')
        self._failed_jobs = {}

    def run(self, cfg):
        """ Runs the job of the day, but before it, runs also all the job
        awaiting in the backlog if any.

        The backlog management strategy consists in adding the job to be
        executed to the backlog before running it, and then removing it if the
        run is successful. This way, whatever way the run fails (even with an
        unexpected error), we can be sure that it is included in the backlog
        for next time.

        :param ProcessConfiguration cfg: configuration data

            The process configuration contains only constant information not depending on the
            execution instance. For instance, the reference time used to filter which events must
            be extracted and exported is not included here, but passed using the ''parms''
            argument of the execution job constructor.

        :returns: error code (ERR_xxx) if something went wrong, 0 if all is ok
        """
        self.log_info('starting')

        backlog = pycstbox.export.Backlog('dwh.events')
        bl_jobs = [j for j in backlog]
        if bl_jobs:
            self.log_warn('not empty backlog : ' + ' '.join([j for j in bl_jobs]))
        else:
            self.log_info('backlog is empty')

        # add the current job to the backlog before running it
        job_id = pycstbox.export.EventsExportJob.make_jobid()
        # compute the events extraction reference date, by applying the
        # requested offset to today's date. Note that the sign of the passed
        # value is ignored, since we have few chances to be able to extract
        # future events :)
        extract_date = (
            datetime.datetime.utcnow() - datetime.timedelta(days=abs(cfg[ProcessConfiguration.Props.DATE_OFFSET]))
        ).date()
        backlog[job_id] = {
            PARM_EXTRACT_DATE: extract_date
        }

        # now execute all the jobs in the backlog
        cfg_retry = cfg[ProcessConfiguration.Props.RETRIES]
        max_try = cfg_retry[ProcessConfiguration.Props.MAX_ATTEMPTS]
        retry_delay = cfg_retry[ProcessConfiguration.Props.DELAY]

        self._failed_jobs = {}
        for job_id, job_parms in backlog.iteritems():
            self.log_info('activating job with id=%s', job_id)
            job = DWHEventsExportJob(
                'dwh.events', job_id, job_parms, cfg
            )
            error_code = job.run(max_try=max_try, retry_delay=retry_delay)
            # if successful run, remove the job from the backlog
            if not error_code:
                del backlog[job_id]
            else:
                self._failed_jobs[job_id] = error_code

        if not self._failed_jobs:
            self.log_info('all jobs successful')
            status_code = self.ERR_NONE
        else:
            self.log_error(
                'job(s) failed (%s)' %
                ' '.join(['%s:%s' % (job_id, pycstbox.export.EventsExportJob.error_text(errcode)) for job_id, errcode in
                          self._failed_jobs.iteritems()])
            )
            if len(self._failed_jobs) == 1:
                status_code = self._failed_jobs.values()[0]
            else:
                status_code = self.ERR_MULTIPLE
        return status_code

    @property
    def failed_jobs(self):
        return self._failed_jobs


class DWHVariableDefinitionsExportProcess(Loggable):
    """ Complete processing chain for configuration data export to DataWareHouse
    variable definitions.

    Since it is supposed to be called only when the device network configuration
    is modified, this process is one-shot, and does not include a backlog
    mechanism (only immediate retries are attempted). Thus we have not split the mechanism in two
    classes as for events. The "job" concept is embedded in the ''run()'' method.
    """

    ERR_NONE = 0
    """successful"""
    ERR_EXPORT = 101
    """configuration export error"""
    ERR_UPLOAD = 201
    """upload failure"""

    err_messages = {
        ERR_NONE: 'successful',
        ERR_EXPORT: 'configuration export error',
        ERR_UPLOAD: 'upload failure'
    }

    @staticmethod
    def error_message(errcode):
        """Returns the error message corresponding to a given code"""
        return DWHVariableDefinitionsExportProcess.err_messages[errcode]

    def __init__(self):
        Loggable.__init__(self, logname='cfg-expproc')

    def run(self, cfg, devices_config, vars_metadata):  #pylint: disable=R0912
        """ Builds the variable definitions dataset, using the current devices
        configuration data, and uploads it to the appropriate area on DataWareHouse
        server.

        Unlike for events date this process is not scheduled to run
        periodically, so no backlog is handled here.

        :param ProcessConfiguration cfg: configuration data
        :param devices_config: devices coonfiguration
        :returns: error code (ERR_xxx) if something went wrong, 0 if all is ok
        """
        self.log_info('starting')
        done = False

        # export the configuration as DataWareHouse point definitions
        error = self.ERR_EXPORT
        try:
            site_code = ProcessConfiguration.Props.SITE_CODE
            exp_filter = VariableDefsExportFilter(
                site_code=cfg[site_code],
                contact=cfg[ProcessConfiguration.Props.REPORT_TO],
                vars_metadata=vars_metadata
            )
            data = exp_filter.export_variable_definitions(devices_config)

        except Exception as e:  #pylint: disable=W0703
            self.log_error('configuration export failure : %s', str(e))

        else:
            self.log_info('configuration export ok')
            cfg_retries = cfg[ProcessConfiguration.Props.RETRIES]
            max_try = cfg_retries[ProcessConfiguration.Props.MAX_ATTEMPTS]
            retry_delay = cfg_retries[ProcessConfiguration.Props.DELAY]

            with tempfile.TemporaryFile() as f:
                # stores the result in the temp file
                json.dump(data, f)
                f.flush()
                f.seek(0)

                # send them to the server
                cfg_server = cfg[ProcessConfiguration.Props.SERVER]
                url = cfg[ProcessConfiguration.Props.API_URLS][ProcessConfiguration.Props.DEFS_UPLOAD] % {
                    'host': cfg_server[ProcessConfiguration.Props.HOST],
                    'site': site_code
                }

                self.log_info('ready to send data')

                cfg_auth = cfg_server[ProcessConfiguration.Props.AUTH]
                auth = (cfg_auth[ProcessConfiguration.Props.LOGIN], cfg_auth[ProcessConfiguration.Props.PASSWORD])
                cnt = 0
                while not done and cnt < max_try:
                    cnt += 1
                    self.log_info('POSTing data to %s', url)
                    resp = requests.post(
                        url,
                        data=f,
                        auth=auth,
                        headers={
                            'Content-Type': 'application/json'
                        }
                    )

                    # self.log_info('%s - %s', resp, resp.text)
                    if resp.ok:
                        done = True
                        resp_data = json.loads(resp.text)
                        self.log_info('!! success (%s)', resp_data['message'])
                    else:
                        try:
                            self.log_error('failed : %d - %s (%s)', resp.status_code, resp.reason, resp.text)
                        except ValueError:
                            self.log_error('unexpected server error : %d - %s', resp.status_code, resp.reason)

                        if cnt < max_try:
                            self.log_info('retrying in %d seconds...', retry_delay)
                            time.sleep(retry_delay)
                        else:
                            self.log_error(
                                'max try count (%d) exhausted => aborting',
                                max_try
                            )

        if done:
            self.log_info('export process successful')
            error = self.ERR_NONE
        else:
            self.log_error('export process failed')

        return error


class ProcessConfiguration(Loggable):
    """ Configuration data manager, using JSON as persistence format.
    """
    class Props(object):
        SITE_CODE = 'site_code'
        DATE_OFFSET = 'date_offset'
        REPORT_TO = 'report_to'
        SERVER = 'server'
        HOST = 'host'
        AUTH = 'auth'
        LOGIN = 'login'
        PASSWORD = 'password'
        API_URLS = 'api_urls'
        DATA_UPLOAD = 'data_upload'
        DEFS_UPLOAD = 'defs_upload'
        JOB_STATUS = 'job_status'
        CONNECT_TIMEOUT = 'connect_timeout'
        RETRIES = 'retries'
        MAX_ATTEMPTS = 'max_attempts'
        DELAY = 'delay'
        STATUS_MONITORING_PERIOD = 'status_monitoring_period'

    SCHEMA = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "title": "Configuration",
        "description": "DataWareHouse extension configuration file",
        "type": "object",
        "properties": {
            Props.SITE_CODE: {
                "description": "The unique identifier of the site which data are pushed to DWH",
                "type": "string"
            },
            Props.DATE_OFFSET: {
                "type": "integer",
                "minimum": 1
            },
            Props.REPORT_TO: {
                "type": "string",
                "format": "email"
            },
            Props.SERVER: {
                "type": "object",
                "properties": {
                    Props.HOST: {
                        "type": "string",
                        "format": "hostname"
                    },
                    Props.AUTH: {
                        "type": "object",
                        "properties": {
                            Props.LOGIN: {
                                "type": "string"
                            },
                            Props.PASSWORD: {
                                "type": "string"
                            }
                        },
                        "required": [Props.LOGIN, Props.PASSWORD]
                    },
                    Props.CONNECT_TIMEOUT: {
                        "type": "integer",
                        "minimum": 1
                    }
                },
                "required": [Props.HOST]
            },
            Props.API_URLS: {
                "type": "object",
                "properties": {
                    Props.DATA_UPLOAD: {
                        "type": "string"
                    },
                    Props.DEFS_UPLOAD: {
                        "type": "string"
                    },
                    Props.JOB_STATUS: {
                        "type": "string"
                    }
                }
            },
            Props.RETRIES: {
                "type": "object",
                "properties": {
                    Props.MAX_ATTEMPTS: {
                        "type": "integer",
                        "minimum": 1
                    },
                    Props.DELAY: {
                        "type": "integer",
                        "minimum": 1
                    }
                }
            },
            Props.STATUS_MONITORING_PERIOD: {
                "type": "integer",
                "minimum": 1
            }
        },
        "required": [Props.SITE_CODE]
    }

    DEFAULTS = {
        Props.DATE_OFFSET: 1,
        Props.SERVER: {
            Props.CONNECT_TIMEOUT: 60
        },
        Props.API_URLS: {
            Props.DATA_UPLOAD: 'http://%(host)s/api/dss/sites/%(site)s/series',
            Props.DEFS_UPLOAD: 'http://%(host)s/api/dss/sites/%(site)s/vardefs',
            Props.JOB_STATUS: 'http://%(host)s/api/dss/sites/%(site)s/jobs/%(job_id)s/status'
        },
        Props.RETRIES: {
            Props.MAX_ATTEMPTS: 3,
            Props.DELAY: 10
        },
        Props.STATUS_MONITORING_PERIOD: 60
    }

    def __init__(self):
        Loggable.__init__(self, logname='cfg-proc')
        self.data = None

    def __getitem__(self, item):
        return self.data[item]

    def load(self, path):
        with file(path) as fp:
            data = json.load(fp)
        self.load_dict(data)

    def loads(self, s):
        self.load_dict(json.loads(s))

    def load_dict(self, data):
        # add default values for options not in loaded file
        cfg = copy.deepcopy(self.DEFAULTS)
        _deep_update(cfg, data)

        try:
            jsonschema.validate(cfg, self.SCHEMA)
        except jsonschema.ValidationError() as e:
            raise ConfigurationError(e)

        self.data = cfg

    def save(self, path):
        with open(path, 'wt') as fp:
            json.dump(self.data, fp)

    def as_dict(self, hide_pwd=False):
        """ Returns the configuration attributes as a dictionary.  """
        res = copy.deepcopy(self.data)
        if hide_pwd:
            res['server']['auth']['password'] = '********'
        return res


def _deep_update(d, u):
    for k, v in u.iteritems():
        if k not in d:
            d[k] = copy.deepcopy(v)
        else:
            if isinstance(v, dict):
                _deep_update(d[k], v)


class ConfigurationError(DWHException):
    pass
