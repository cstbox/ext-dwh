#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Check series upload job status.
"""

import ConfigParser
import os
import sys
import json
import time

import requests

import pycstbox.log as log
import pycstbox.cli
import pycstbox.config

from pycstbox.dwh import CONFIG_FILE_NAME
from pycstbox.dwh.process import ProcessConfiguration
from pycstbox.dwh.pending_jobs_queue import PendingJobsQueue

__author__ = 'Eric PASCUAL - CSTB (eric.pascual@cstb.fr)'

SCRIPT_NAME = os.path.splitext(os.path.basename(__file__))[0]


class Worker(object):
    TERMINATE_CHECK_PERIOD = 1

    JOB_STATUS = {
        1: "in process",
        0: "completed",
        -1: "bad file format",
        -2: "missing variable name",
        -3: "unknown variable name",
        -4: "database connection failure",
        -5: "incoherent data error",
        -6: "invalid data"
    }

    def __init__(self, cfg, debug=False, **kwargs):
        if cfg.status_monitoring_period <= 0:
            raise ValueError('period should be a positive number of seconds')

        self._debug = debug

        self._cfg = cfg

        self._log = log.getLogger(self.__class__.__name__)
        if debug:
            self._log.setLevel(log.DEBUG)

        self._terminated = False

    def run(self):
        site_code = self._cfg.site_code
        period = int(self._cfg.status_monitoring_period)
        auth = (self._cfg.login, self._cfg.password)

        query = self._cfg.job_status_url

        self._log.info('started (site_code=%s period=%d secs)', site_code, period)

        last_check = 0

        while True:
            now = time.time()
            if now - last_check >= period:
                queue = PendingJobsQueue()
                for job_id in queue.items():
                    if self._debug:
                        self._log.debug('requesting status of site/job %s/%s', self._cfg.site_code, job_id)

                    resp = requests.get(url=query % (site_code, job_id), auth=auth)

                    if resp.ok:
                        if self._debug:
                            self._log.debug('got reply : %s', resp.text)

                        reply = json.loads(resp.text)

                        completion_code = reply['code']

                        if completion_code == 0:    # completed
                            # log it and remove the job from the queue
                            log.info('job %s completed ok', job_id)
                            queue.remove(job_id)
                        elif completion_code < 0:
                            # solid error => log it and remove the job from the queue
                            try:
                                code_msg = reply['status']
                            except KeyError:
                                code_msg = "unknown code"
                            log.error('job %s failed with code %d (%s)', job_id, completion_code, code_msg)
                            queue.remove(job_id)

                        # otherwise the job is still pending. Just leave it a is

                    else:
                        log.error("server replied with : %d - %s", resp.status_code, resp.reason)

                last_check = now

            if self._terminated:
                self._log.info('terminate request detected')
                break

            time.sleep(self.TERMINATE_CHECK_PERIOD)

        self._log.info('worker thread terminated')

    def terminate(self):
        self._terminated = True


if __name__ == '__main__':
    gs = pycstbox.config.GlobalSettings()

    pycstbox.log.setup_logging()
    _logger = pycstbox.log.getLogger(name=SCRIPT_NAME)

    # process CLI args
    parser = pycstbox.cli.get_argument_parser(
        description=__doc__
    )
    args = parser.parse_args()

    pycstbox.log.set_loglevel_from_args(_logger, args)

    _logger.info('loading process configuration')
    process_cfg = ProcessConfiguration()

    # Loads the configuration parameters
    try:
        process_cfg.load(pycstbox.config.make_config_file_path(CONFIG_FILE_NAME))

    except ConfigParser.Error as e:
        _logger.fatal('configuration error (%s)', e)
        sys.exit(1)

    else:
        _logger.debug('--> %s:', process_cfg.as_dict())

        worker = Worker(process_cfg, args.debug)
        worker.run()

        _logger.info('process terminated')
