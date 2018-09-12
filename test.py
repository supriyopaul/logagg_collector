#!/usr/bin/env python

import doctest
import unittest

from logagg import formatters
from logagg import collector

def suite_maker():
    suite= unittest.TestSuite()
    suite.addTests(doctest.DocTestSuite(formatters))

    suite.addTests(doctest.DocTestSuite(collector))

    return suite

