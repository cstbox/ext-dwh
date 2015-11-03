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

CONFIG_FILE_NAME = 'dwh.cfg'
""" The name of the configuration file (located in /etc/cstbox by default)
containing the parameters for all DataWareHouse related scripts.
"""

VARS_METATDATA_FILE_NAME = "vars_metadata.cfg"
""" The name of the file containing the exported variables metadata. This file
is located in /etc/cstbox by default.
"""

FILE_TIMESTAMP_FMT = '%Y%m%d-%H%M%S'
""" The format of the time stamp when included in file names.
"""


class DWHException(Exception):
    """ Specialized exception for DataWareHouse related errors
    """