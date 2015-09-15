#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Configuration export script for DataWareHouse portal.

This script exports the points definition based on the current state of the
device network configuration.
"""

import ConfigParser
import os
import sys

import pycstbox.log
import pycstbox.cli
import pycstbox.config
import pycstbox.devcfg

from pycstbox.dwh import CONFIG_FILE_NAME
from pycstbox.dwh.process import ProcessConfiguration, DWHConfigurationExportProcess

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
        process_cfg.read(pycstbox.config.make_config_file_path(CONFIG_FILE_NAME))

    except ConfigParser.Error as e:
        log.fatal('configuration error (%s)', e)
        sys.exit(1)

    else:
        log.info('initializing export process')
        process = DWHConfigurationExportProcess()
        process.log_setLevel_from_args(args)

        devices_cfg = pycstbox.devcfg.DeviceNetworkConfiguration(autoload=True)

        try:
            error = process.run(process_cfg, devices_cfg)

        except Exception as e:      #pylint: disable=W0703
            log.exception(e)
            log.fatal('process failed')
            sys.exit(1)

        else:
            if error:
                sys.exit(error)
            else:
                log.info('process completed ok')
