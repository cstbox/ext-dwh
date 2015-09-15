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

""" Export tools for DataWareHouse.

Two types of data set can be exported for upload to DataWareHouse server :

    - **time series**, containing chronologies of variable value changes
    - **definitions**, containing the description of variables which series
      will be uploaded

Both types share the following characteristics:

    - tabulated ASCII files
    - end of line delimiter : line feed (``0x10``)
    - record fields separator : tab (``0x09``)
    - internal time stamps formatted as ISO UTC (``yyyy-mm-ddThh:nn:ss.sssZ``)
    - numerical values formatted according to ISO standard (decimal point, no thousands
      separator, exponential notation 'E' allowed)

The specificity of each export type are detailed in the respective class documentation:

    - variable series export : :class:`EventsExportFilter`
    - variable definitions : :class:`VariableDefsExportFilter`

"""

import datetime
import os
from collections import namedtuple
import itertools

from pycstbox.events import DataKeys
from pycstbox.devcfg import Metadata
from pycstbox.DataWareHouse import DataWareHouseException

__author__ = 'Eric PASCUAL - CSTB (eric.pascual@cstb.fr)'

VAR_DEFS_FORMAT_VERSION = 1
SERIES_FORMAT_VERSION = 1

DTFMT_HEADER = "%Y-%m-%dT%H:%M:%SZ"
""" Time stamp format in headers"""
DTFMT_SERIES_FNAME = "%y%m%d%H%M%S"
""" Format for the time stamp in time series file names"""
DTFMT_POINT = "%Y-%m-%dT%H:%M:%SZ"
""" Time stamp format for series points"""

SERIES_NAME_PATTERN = "%s_%s"
"""Format for the names of the series"""
SERIES_FILENAME_PATTERN = "%s.%s.csv"
"""Format for file names of the series"""

LINE_END = '\n'


class EventsExportFilter(object):
    """ Filter for exporting an event sequence as a collection of variable series.

    The output is a collection of CSV files, one per exported variable. Each one must conform to
    the global directives described in the general documentation of this module. Their content is :

      - one record per series point
      - each record contains the value datation and the value itself.

    The file is composed of two sections, separated by a blank line :

    **Header:**
        Contains global information which can be used in the subsequent processing ::

            VERSION<TAB><format_version_number><LF>
            CREATION_DATE<TAB><file_creation_timestamp><LF>
            ID_SITE<TAB><site_code><LF>
            FEEDBACK_TO<TAB><feedback_email><LF>
            VARNAME<TAB><varname><LF>

    *Placeholders meaning:*

    ``format_version_number``
        The format version allows further evolutions of the format while ensuring backwards
        compatibility (current version given by VAR_DEFS_FORMAT_VERSION).
    ``file_creation_timestamp``
        The file generation date, formatted according to DTFMT_HEADER.
    ``site_identifier``
        The site identifier assigned by the DataWareHouse portal to the related site.
    ``feedback_email``
        email address to be used for processing report communication. If not
        provided no report will be sent.
    ``varname``
        the name of the variable which series is contained in the file

    **Data:**
        The series point
    """
    def __init__(self, site_code, contact=None, prefix_with_type=True):
        """
        :param str site_code: (mandatory) the code of the site, as provided by DataWareHouse
        :param str contact: email of the contact person for process feedback sending
        :param boolean prefix_with_type: True for prefixing the series name with the variable type

        :raises ValueError: if site id not provided
        """
        if not site_code:
            raise ValueError('[EventsExportFilter] site_code parameter is mandatory')

        self._site_code = site_code
        self._contact = contact
        self._prefix_with_type = prefix_with_type

    def export_events(self, events, to_dir='/tmp'):
        """ Export a list of CSTBox events as the corresponding set of
        DataWareHouse files.

        :param list events: the list of events to be exported
        :param str to_dir: path the the directory where export files will be generated (must exist)
        :returns: a tuple containing the number of processed events, and the list of generated
            file paths.
        :rtype: tuple
        :raises ValueError: if site id not provided or if export directory is not valid
        """
        if not to_dir:
            raise ValueError('export dir parameter cannot be empty')
        if not os.path.exists(to_dir):
            raise ValueError('path not found : %s' % to_dir)
        if not os.path.isdir(to_dir):
            raise ValueError('path is not a dir : %s' % to_dir)
        if not os.access(to_dir, os.W_OK | os.X_OK):
            raise ValueError('cannot write to : %s' % to_dir)

        # all generated files will have the same time in their name
        export_time = datetime.datetime.utcnow()

        series_files = {}
        created_files = []
        evt_count = 0
        try:
            for evt in events:
                # keep only energy events coming from Tywatts to avoid messing
                # current DataWareHouse declarations with data issued from plugs
                # TODO replace this dirty hack by something smarter
                if evt.data[DataKeys.UNIT] != 'kWh':
                    continue

                evt_count += 1
                if self._prefix_with_type:
                    series_name = SERIES_NAME_PATTERN % (evt.var_type, evt.var_name)
                else:
                    series_name = evt.var_name
                try:
                    outfile = series_files[series_name]
                except KeyError:
                    outpath = os.path.join(
                        to_dir,
                        self.series_filename(series_name, export_time)
                    )

                    outfile = file(outpath, 'wt')
                    series_files[series_name] = outfile
                    created_files.append(outpath)

                    self._emit_series_header(series_name, outfile)

                # emit the event
                value = maybe_boolean(str(evt.data[DataKeys.VALUE]))
                outfile.write("%s\t%s%s" % (evt.timestamp.strftime(DTFMT_POINT), value, LINE_END))

        finally:
            for f in series_files.itervalues():
                f.close()

        return evt_count, created_files

    def _emit_series_header(self, varname, outfile):
        """ Export the file header for a varname series.

        :param str varname: the name of the variable name)
        :param file outfile: the destination file
        """
        now = datetime.datetime.utcnow()
        outfile.write(LINE_END.join([
            "FORMAT_VERSION\t%d" % VAR_DEFS_FORMAT_VERSION,
            "CREATION_DATE\t%s" % now.strftime(DTFMT_HEADER),
            "ID_SITE\t%s" % self._site_code,
            "FEEDBACK_TO\t%s" % (self._contact if self._contact else ''),
            "VARNAME\t%s" % varname,
            LINE_END
        ]))
        outfile.flush()

    @staticmethod
    def series_filename(varname, export_time):
        """ Returns the DataWareHouse name of the file containing the data for a given series and export
        date.

        :param str varname: the name of the series variable
        :param datetime.datetime export_time: the datetime of the export
        """
        return SERIES_FILENAME_PATTERN % (
            varname,
            export_time.strftime(DTFMT_SERIES_FNAME)
        )


class VariableDefsExportFilter(object):
    """ Filter for exporting the variable definitions corresponding to a device
    network configuration.

    A point is defined for each enabled output of the enabled devices, based on
    the variable which has been defined and associated.

    **Export format details:**

    The variable definitions are uploaded to reflect addition of variables, or modification
    of their properties. Renaming a variable is not possible on purpose, to avoir incoherences
    introduction. If a variable is renamed at the production level, this will tanslate into the
    creation of a new variable which will hold all upcoming data, and "freeze" of the old named one
    which series will not be extended any more, but will still be available.

    The variable definitions file does not need to contain all existing variables, and can convey
    a subset of them (added or modified ones only for instance). If the whole variable definitions
    set is sent, unmodified ones will be left untouched.

    The following properties are defined for each variable :

    *varname*
        **[mandatory]** The name of the variable. It must conform the syntax defined by common
        programming languages.

    *label*
        **[mandatory]** The (short) descriptive text of the variable

    *type*
        **[mandatory]** The semantic type of the variable. Ex: energy, voltage, temperature,
        motion,...

    *value_type*
        **[mandatory]** The data type of the variable value. Must be one of:

            - ''N'' for a numerical value (integer or float)
            - ''L'' for a logical value
            - ''T'' for a textual (string) value

    *units*
        The units used to represent values. No coherence checking will be done between uploaded
        values and declared units, and it is up to the producer to ensure it. For obvious reasons,
        units should not be changed if data have already been uploaded for the variable. If such a
        change is needed, it is advised to create a new variable. The provided units is taken as
        free text and is not checked against a predefined list. A good rule of thumbs is to use
        standard SI units.

    *lower_bound, upper_bound*
        Domain validity range. If provided (one or both) the data integration process on the portal
        will check if provided values fall into the range, and will reject failing ones. Note that
        this is only applied to numeric values, and bounds will be ignored for data types other than
        ''N''.

    *delta_min, delta_max*
        Maximum absolute difference from last value. If provided (one or both) the data integration
        process on the portal will check if provided values conform to the constrainst (only if a
        previous value is available of course). Failing values will be rejected. Note that
        this is only applied to numeric values, and difference limits will be ignored for data types
        other than ''N''.

    The definition file is composed of two sections separated by a blank line. They art detailed
    hereafter.

    **File header:**

        Contains global information which can be used in the subsequent processing

    ::

        FORMAT_VERSION<TAB><format_version_number><LF>
        CREATION_DATE<TAB><file_creation_timestamp><LF>
        ID_SITE<TAB><site_identifier><LF>
        FEEDBACK_TO<TAB><feedback_email><LF>

    *Placeholders meaning:*

    ``format_version_number``
        The format version allows further evolutions of the format while ensuring backwards
        compatibility It is currently set to 1.
    ``file_creation_timestamp``
        The file generation date, formated as ISO UTC.
    ``site_identifier``
        The site identifier assigned by the DataWareHouse portal to the related site.
    ``feedback_email``
        email address to be used for processing report communication. If not
        provided no report will be sent.

    **Definitions:**

    ::

        <variable_definition_line><LF>

    The variable definition lines contains the property values, separated by tabs, not provided
    ones being represented by empty or blank values.
    """
    def __init__(self, site_code, contact=None):
        """
        :param str site_code: (mandatory) the id of the site (aka "system id" in CSTBox context)
        :param str contact: email of the contact person for process feedback sending
        :raises ValueError: if site id not provided
        """
        if not site_code:
            raise ValueError('[VariableDefsExportFilter] site_code parameter is mandatory')

        self._site_code = site_code
        self._contact = contact

    def export_devices_configuration(self, cfg):
        """ Exports the devices configuration as a DataWareHouse variable definitions
        file.

        Points are created for enabled devices and outputs only. In addition
        only outputs to which a variable is attached are processed (which should
        always be the case since a variable name is mandatory for enabling an
        output).

        Devices metadata are used to obtain complementary information such as
        the semantic data type (ie: temperature, voltage, opened,...) of the
        variables.

        :param dict cfg:
                the global configuration dictionary, as returned by the device
                manager (if the value attached to the 'coordinators' key)

        :returns: a string list containing the export data, formated as CSV data
            conforming DataWareHouse specifications. No new line is added to items.
        """
        # build the merged device list by concatenating the list of devices
        # attached to each coordinator. Thanks to itertools, we don't create
        # duplicates of the lists, but only work with iterators.
        all_devices = [d for d in itertools.chain(
            *(d for d in [c.itervalues() for c in cfg.itervalues()])
        )]

        vardefs = self._make_variable_definitions(all_devices)
        now = datetime.datetime.utcnow()

        result = [
            "FORMAT_VERSION\t%d" % VAR_DEFS_FORMAT_VERSION,
            "CREATION_DATE\t%s" % now.strftime(DTFMT_HEADER),
            "ID_SITE\t%s" % self._site_code,
            "FEEDBACK_TO\t%s" % (self._contact if self._contact else ''),
            "",
            _VarDef_attrs.replace(" ", "\t")
        ]

        for vardef in vardefs:
            fields = [s if s is not None else '' for s in (
                vardef.varname,
                vardef.label,
                vardef.type,
                vardef.value_type,
                vardef.units,
                vardef.lower_bound,
                vardef.upper_bound,
                vardef.delta_min,
                vardef.delta_max
            )]
            result.append('\t'.join(fields))

        return result

    def _make_variable_definitions(self, devices):
        """ Returns the definitions of the variables based on the passed devices
        configuration.

        A DataWareHouse variable is the same as a CSTBox variable.

        :param list devices:
                a list of device configurations, as stored in the devices
                configuration file. List of devices attached to different
                coordinators are merged in a single global list.

        :returns:
            the corresponding list of point definitions, each item being an
            instance of the named tuple VariableDefinition
        :rtype: list

        :raises DataWareHouseException: in case of error
        """
        points = []

        def _make_variable_definition(varname_, vartype_, varunits_):
            """ Creates a variable definition, using the following rules :

                - name : variable name
                - label : variable name
                - type: variable type
                - value_type : value type code derived from the type of the variable
                - units : variable units
                - lower_bound : unsupported
                - upper_bound : unsupported
                - delta_min : unsupported
                - delta_max : unsupported

            :param str varname_: the variable name
            :param str vartype_: the variable type
            :param str varunits_: the variable units
            :returns VariableDefinition: the definition named tuple
            """
            return VariableDefinition(
                varname_,
                varname_,
                vartype_,
                self.vartype_to_valuetype(vartype_),
                varunits_,
                None,
                None,
                None,
                None
            )

        # cache for devices metadata
        devmetas = {}
        known_vars = []

        for cfg in [cfg for cfg in devices if cfg.enabled]:
            devtype = cfg.type

            # get the device metadata from the cache, updating it if needed
            try:
                devmeta = devmetas[devtype]
            except KeyError:
                devmeta = Metadata.device(devtype)
                devmetas[devtype] = devmeta

            meta_pdefs = devmeta['pdefs']
            if hasattr(cfg, 'outputs'):
                meta_outputs = meta_pdefs['outputs']
                meta_generic = meta_outputs.get('*', None)
                # case of a multiple outputs device => explore all of them
                for k, v in [(k, v) for (k, v) in cfg.outputs.iteritems()
                             if v.get('enabled') and v.get('varname')]:
                    varname = v['varname']
                    if varname in known_vars:
                        raise DataWareHouseException('duplicated variable : %s' % varname)
                    known_vars.append(varname)

                    # get the output metadata, handling the case where they are
                    # defined as generic (ie a single definition with an id set
                    # to '*')
                    if k in meta_outputs:
                        output_meta = meta_outputs[k]
                    else:
                        output_meta = meta_generic
                    vartype = output_meta['__vartype__']
                    varunits = output_meta.get('__varunits__')
                    points.append(_make_variable_definition(varname, vartype, varunits))

            elif hasattr(cfg, 'varname'):
                # case of a single output device with an attached variable
                varname = cfg.varname
                if varname in known_vars:
                    raise DataWareHouseException('duplicated variable : %s' % varname)
                known_vars.append(varname)

                output_meta = meta_pdefs['root']
                vartype = output_meta['__vartype__']
                varunits = output_meta.get('__varunits__')
                points.append(_make_variable_definition(varname, vartype, varunits))

        return points

    # classification of CSTBox var type to DataWareHouse var types (default: numeric)
    _TEXT_TYPES = ()
    _LOGICAL_TYPES = ('opened', 'motion_detection', 'motion', 'presence')

    @staticmethod
    def vartype_to_valuetype(vartype):
        """ Returns the DataWareHouse variable type corresponding to a given CSTBox variable type,
        defaulting it to numeric if not explicitly declared. """
        if vartype in VariableDefsExportFilter._TEXT_TYPES:
            return 'T'
        elif vartype in VariableDefsExportFilter._LOGICAL_TYPES:
            return 'L'
        else:
            return 'N'


# DataWareHouse variable definition
_VarDef_attrs = 'varname label type value_type units lower_bound upper_bound delta_min delta_max'


class VariableDefinition(namedtuple(
    'VariableDefinition',
    _VarDef_attrs
)):
    __slots__ = ()

    def as_strings(self):
        """
        :return: the tuple elements as an array of strings
        :rtype: array
        """
        return self[:4] + tuple((str(v) if v is not None else '' for v in self[4:]))

_BOOL_TO_NUM = {'true': '1', 'false': '0'}


def maybe_boolean(value):
    """ If the value is the string representation of a boolean, return the
    integer equivalent (ie 0 or 1) as a string. Otherwise, returns the passed
    value unchanged.

    :param value: the value which representation is wanted
    :returns: boolean equivalent if boolean parameter passed
    :rtype: str
    """
    return _BOOL_TO_NUM.get(value.lower(), value)
