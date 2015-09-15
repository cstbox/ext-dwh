#!/usr/bin/env python
# -*- coding: utf-8 -*-

# TODO finish adaptation

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
import ConfigParser
import zipfile
import json
import requests

from pycstbox.log import Loggable
import pycstbox.export
import pycstbox.evtdao
from pycstbox.config import GlobalSettings
from pycstbox.dwh.filters import EventsExportFilter, VariableDefsExportFilter, LINE_END
from pycstbox.dwh.pending_jobs_queue import PendingJobsQueue
from pycstbox.events import VarTypes

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
        """ Creates a ZIP archive containing the time series of the energy related variables.

        The generated file name is placed in the private attribute ''self._archive'' for later use
        by the sending step.

        :return: the exported events count
        """
        evt_count = 0
        self._archive = None

        filter_ = EventsExportFilter(self._config.site_code, prefix_with_type=False)
        extract_date = self._parms[PARM_EXTRACT_DATE]
        with pycstbox.evtdao.get_dao(gs.get('dao_name')) as dao:
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
            # and log the result

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

    def run(self, config):
        """ Runs the job of the day, but before it, runs also all the job
        awaiting in the backlog if any.

        The backlog management strategy consists in adding the job to be
        executed to the backlog before running it, and then removing it if the
        run is successful. This way, whatever way the run fails (even with an
        unexpected error), we can be sure that it is included in the backlog
        for next time.

        :param config:
            process configuration, containing the following attributes:

                site_code
                    the code of the site, used by DataWareHouse portal to identify
                    it
                contact
                    email of the person to which anomaly reports are sent
                date_offset
                    day shift from current time for defining the reference
                    for data extraction (usually set to 1 for "yesterday")
                data_upload_url
                    the full URL (including protocol and credentials) for
                    variable series data upload
                connect_timeout
                    timeout (seconds) for the server connection
                max_try
                    maximum number of data upload attempts
                retry_delay
                    delay (in seconds) before retrying a failed upload
                login, password
                    credentials

            Because these settings come from a configuration file, all the
            values are strings, even numeric ones.

            The process configuration contains only constant information not depending on the
            excution instance. For instance, the reference time used to filter which events must
            be extracted and exported is not included here, but passed using the ''parms''
            argument of the execution job constructor.

        :returns: error code (ERR_xxx) if something went wrong, 0 if all is ok
        """
        self.log_info('starting')

        backlog = pycstbox.export.Backlog('DataWareHouse.events')
        bl_jobs = [j for j in backlog]
        if bl_jobs:
            self.log_warn('not empty backlog : ' + ' '.join([j for j in bl_jobs]))
        else:
            self.log_info('backlog is empty')

        # add the current job to the backlog before running it
        jobid = pycstbox.export.EventsExportJob.make_jobid()
        # compute the events extraction reference date, by applying the
        # requested offset to today's date. Note that the sign of the passed
        # value is ignored, since we have few chances to be able to extract
        # future events :)
        extract_date = (
            datetime.datetime.utcnow()
            - datetime.timedelta(days=abs(int(config.date_offset)))
        ).date()
        backlog[jobid] = {
            PARM_EXTRACT_DATE: extract_date
        }

        # now execute all the jobs in the backlog
        self._failed_jobs = {}
        for jobid, jobparms in backlog.iteritems():
            self.log_info('activating job with id=%s', jobid)
            job = DWHEventsExportJob(
                'DataWareHouse.events', jobid, jobparms, config
            )
            error_code = job.run(
                max_try=config.max_try,
                retry_delay=config.retry_delay
            )
            # if successful run, remove the job from the backlog
            if not error_code:
                del backlog[jobid]
            else:
                self._failed_jobs[jobid] = error_code

        if not self._failed_jobs:
            self.log_info('all jobs successful')
            status_code = self.ERR_NONE
        else:
            self.log_error(
                'job(s) failed (%s)' %
                ' '.join(['%s:%s' % (jobid, pycstbox.export.EventsExportJob.error_text(errcode)) for jobid, errcode in
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


class DWHConfigurationExportProcess(Loggable):
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
        return DWHConfigurationExportProcess.err_messages[errcode]

    def __init__(self):
        Loggable.__init__(self, logname='cfg-expproc')

    def run(self, run_parms, devices_config):  #pylint: disable=R0912
        """ Builds the variable definitions dataset, using the current devices
        configuration data, and uploads it to the appropriate area on DataWareHouse
        server.

        Unlike for events date this process is not scheduled to run
        periodically, so no backlog is handled here.

        :param run_parms:
            process configuration, containing the following attributes:

                site_code
                    the code of the site, used by DataWareHouse portal to identify it
                contact
                    email of the person to which anomaly reports are sent
                cfg_upload_url
                    the full URL (including protocol and credentials) for
                    variable definitions upload
                connect_timeout
                    timeout (seconds) for the server connection
                max_try
                    maximum number of data upload attempts
                retry_delay
                    delay (in seconds) before retrying a failed upload
                login, password
                    credentials

            Because these settings come from a configuration file, all the
            values are strings, even numeric ones.

        :param devices_config:
            devices coonfiguration

        :returns: error code (ERR_xxx) if something went wrong, 0 if all is ok
        """
        self.log_info('starting')
        done = False

        # export the configuration as DataWareHouse point definitions
        error = self.ERR_EXPORT
        try:
            exp_filter = VariableDefsExportFilter(run_parms.site_code, run_parms.contact)
            data = exp_filter.export_devices_configuration(devices_config)

        except Exception as e:  #pylint: disable=W0703
            self.log_error('configuration export failure : %s', str(e))

        else:
            self.log_info('configuration export ok')
            max_try = run_parms.max_try
            retry_delay = run_parms.retry_delay

            with tempfile.TemporaryFile() as f:
                # stores the result in the temp file
                for line in data:
                    f.write(line + LINE_END)
                f.flush()
                f.seek(0)

                # send them to the server
                url = run_parms.cfg_upload_url % (run_parms.site_code)

                self.log_info('ready to send data')

                cnt = 0
                while not done and cnt < max_try:
                    cnt += 1
                    http_err = resp = None
                    self.log_info('POSTing data to %s', url)
                    resp = requests.post(
                        url,
                        files={
                            'metadata': f
                        },
                        auth=(run_parms.login, run_parms.password)
                    )

                    self.log_info('%s - %s', resp, resp.text)
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


def checked_positive(s):
    """ Checks if the parameter represents a strictly positive integer
    :param s: the string to be checked
    :return: the integer value
    :rtype: int
    :raises ValueError: if check fails
    """
    v = int(s)
    if v > 0:
        return v
    else:
        raise ValueError('not a positive integer: %d' % v)


def flag(s):
    """ Interprets a boolean flag represented by '0' or '1' (True only if value equals '1')
    """
    return s == '1'


class ProcessConfiguration(ConfigParser.SafeConfigParser, Loggable):
    """ A ConfigurationParser like class adding typing of the parameters,
    and value checking at reading time.

    Tailored for DataWareHouse export context.
    """

    SECTION = 'DataWareHouse'

    DEFAULTS = {
        'site_code': '',
        'contact': '',
        'data_upload_url': 'https://api.DataWareHouse.eu/v1/users/current/sites/$$/variables/series',
        'cfg_upload_url': 'https://api.DataWareHouse.eu/v1/users/current/sites/$$/variables',
        'job_status_url': 'https://api.DataWareHouse.eu/v1/users/current/sites/$$/jobs/$$',
        'login': '',
        'password': '',
        'connect_timeout': '60',
        'date_offset': '1',
        'max_try': '3',
        'retry_delay': '10',
        'debug': '0'
    }

    _VALUE_HANDLERS = {
        'connect_timeout': checked_positive,
        'max_try': checked_positive,
        'retry_delay': checked_positive,
        'debug': flag
    }

    def __init__(self):
        super(ProcessConfiguration, self).__init__()
        self.add_section(self.SECTION)
        for k, v in self.DEFAULTS.iteritems():
            self.set(self.SECTION, k, v)
        Loggable.__init__(self, logname='cfg-proc')
        self.password = self.login = None

    @classmethod
    def iterkeys(cls):
        return cls.DEFAULTS.iterkeys()

    def read(self, path):
        super(ProcessConfiguration, self).read(path)

        # add default values for options not in loaded file
        loaded_opts = self.options(self.SECTION)
        for k in [k for k in self.DEFAULTS if k not in loaded_opts]:
            self.set(self.SECTION, k, self.DEFAULTS[k])

        hndlr = self._VALUE_HANDLERS
        for k, v in self.items(self.SECTION):
            # Convert placeholders into string format replaceable parts
            # Details:
            # '%' char is used by ConfigParser for its interpolation process, and no escaping
            # is available. Thus we need to use something else not conflicting.
            v = v.replace('$$', '%s')

            if k in hndlr:
                # convert it to the appropriate type, checking it at the same time
                try:
                    v = hndlr[k](v)
                except ValueError as e:
                    raise ConfigParser.Error("invalid value for key '%s' (%s)" % (k, e))

            setattr(self, k, v)

    def write(self, path):
        for option in self.options(self.SECTION):
            if option == 'password':
                value = self.password
            else:
                # for values other than passwords, replace string placeholders by something
                # not clashing with ConfigParser mechanism
                value = str(getattr(self, option)).replace('%s', '$$')
            self.set(self.SECTION, option, value)
        with open(path, 'wt') as f:
            super(ProcessConfiguration, self).write(f)

    def as_dict(self, hide_pwd=False):
        """ Returns the configuration attributes as a dictionary.  """
        return dict(
            (k, getattr(self, k)
                if not hide_pwd or k not in ('password', 'passwd') else '********'
            )
            for k in self.DEFAULTS
        )


