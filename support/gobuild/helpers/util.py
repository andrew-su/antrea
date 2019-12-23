# Copyright (c) 2008 VMware, Inc.  All rights reserved. -- VMware Confidential

import re

"""
Contains miscellaneous helper functions.
"""


def ExtractMacro(filename, macro):
    """
    Return the string value of the macro `macro' defined in `filename'.
    """
    # Simple regex is far from a complete C preprocessor but is useful
    # in many cases
    regexp = re.compile(r'^\s*#\s*define\s+%s\s+"(.+[.].+[.].+)"\s*$' % macro)
    try:
        for line in open(filename):
            m = regexp.match(line)
            if m:
                return m.group(1)
    except EnvironmentError:
        pass
    return ''
