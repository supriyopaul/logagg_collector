#!/usr/bin/env python

import doctest
import unittest

from logagg_collector import formatters
from logagg_collector import collector

def suite_maker():
    suite= unittest.TestSuite()
    suite.addTests(doctest.DocTestSuite(formatters))

    suite.addTests(doctest.DocTestSuite(collector))

    return suite

