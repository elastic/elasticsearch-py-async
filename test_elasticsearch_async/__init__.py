import sys

import pytest


def required_python(major, minor):
    return pytest.mark.skipif(sys.version_info < (major, minor),
                              reason='Python {}.{} or higher is required'.format(major, minor))
