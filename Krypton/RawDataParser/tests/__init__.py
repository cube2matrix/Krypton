import os
import unittest2 as unittest

all_suite = unittest.TestLoader().discover(os.path.dirname(__file__), "test_*.py")
