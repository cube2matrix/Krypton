import unittest2 as unittest
from parsers.libs import xml

class TestLibsMethods(unittest.TestCase):

    def test_listXmlPath(self):
        xmlPath = xml.listXmlPath('../data/pubmedMedline/')
        for index, url in enumerate(xmlPath):
            suffix = url.split('.')[1]
            self.assertEqual(suffix, 'xml')

    def test_listNxmlPath(self):
        xmlPath = xml.listNxmlPath('../data/pmc/')
        for index, url in enumerate(xmlPath):
            suffix = url.split('.')[1]
            self.assertEqual(suffix, 'nxml')

    def test_xmlConvDict(self):
        xmlPath = xml.listXmlPath('../data/')
        for index, url in enumerate(xmlPath):
            conv = xml.xmlConvDict(url)
            self.assertEqual(type(conv), dict)