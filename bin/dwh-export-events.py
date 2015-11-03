#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Export events script for DataWareHouse portal.

This script exports the events log from the past 24 hours (by default).
"""

import ConfigParser
import os
import sys

import pycstbox.log
import pycstbox.cli
import pycstbox.config

from pycstbox.dwh import CONFIG_FILE_NAME
from pycstbox.dwh.process import ProcessConfiguration, DWHEventsExportProcess

__author__ = 'Eric PASCUAL - CSTB (eric.pascual@cstb.fr)'


SCRIPT_NAME = os.path.splitext(os.path.basename(__file__))[0]

if __name__ == '__main__':
    gs = pycstbox.config.GlobalSettings()

    pycstbox.log.setup_logging()
    log = pycstbox.log.getLogger(name=SCRIPT_NAME)

    # process CLI args
    parser = pycstbox.cli.get_argument_parser(
        description=__doc__
    )
    args = parser.parse_args()

    pycstbox.log.set_loglevel_from_args(log, args)

    log.info('loading process configuration')
    process_cfg = ProcessConfiguration()

    # Loads the configuration parameters
    try:
        process_cfg.load(pycstbox.config.make_config_file_path(CONFIG_FILE_NAME))

    except ConfigParser.Error as e:
        log.fatal('configuration error (%s)', e)
        sys.exit(1)

    else:
        log.debug('--> %s:', process_cfg.as_dict())
        log.info('initializing export process')
        process = DWHEventsExportProcess()
        process.log_setLevel_from_args(args)

        try:
            error = process.run(process_cfg)

        except Exception as e:      #pylint: disable=W0703
            log.exception(e)
            log.fatal('process failed with an unexpected error')
            sys.exit(1)

        else:
            if error:
                log.error('process failed with errcode=%d', error)
                sys.exit(error)
            else:
                log.info('process completed ok')
