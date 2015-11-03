#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Variable definitions export script for DataWareHouse portal.

This script exports the definition of the variables which time series are pushed to the server,
based on the current state of the device network configuration and the variables metadata stored
in the relevant configuration file (defined by `pycstbox.dwh.VARS_METATDATA_FILE_NAME`).

The metadata are defined as a dictionary keyed by the variable name, and which entries are
sub-dictionaries containing the following keys:

    name
        the name of the variable
    label
        a short descriptive text
    description
        a more detailed descriptive text
    type
        semantic type of the variable (e.g. temperature, energy,...)
    unit
        unit used to express values
    lower_bound
        lower bound of accepted values
    upper_bound
        upper bound of accepted values
    delta_min
        inclusive minimum difference with last value received
    delta_max
        inclusive maximum difference with last value received

"""

import ConfigParser
import os
import sys
import json

import pycstbox.log
import pycstbox.cli
import pycstbox.config
import pycstbox.devcfg

from pycstbox.dwh import CONFIG_FILE_NAME, VARS_METATDATA_FILE_NAME
from pycstbox.dwh.process import ProcessConfiguration, DWHVariableDefinitionsExportProcess

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
        log.info('initializing export process')
        process = DWHVariableDefinitionsExportProcess()
        process.log_setLevel_from_args(args)

        try:
            devices_cfg = pycstbox.devcfg.DeviceNetworkConfiguration(autoload=True)
            vars_metadata = json.load(file(pycstbox.config.make_config_file_path(VARS_METATDATA_FILE_NAME)))

            error = process.run(process_cfg, devices_cfg, vars_metadata)

        except Exception as e:      #pylint: disable=W0703
            log.exception(e)
            log.fatal('process failed')
            sys.exit(1)

        else:
            if error:
                sys.exit(error)
            else:
                log.info('process completed ok')
