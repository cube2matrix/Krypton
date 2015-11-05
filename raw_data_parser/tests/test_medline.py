import os
import sys
import unittest2 as unittest
from parsers import medline
from parsers.libs import xml

class TestMedlineParserMethods(unittest.TestCase):

    def test_parseSingleXml(self):
        root = xml.xmlConvDict(os.path.dirname(__file__) + '/../data/pubmedMedline/singleTest.xml')

        result = medline.parseSingleArticle(root[u'MedlineCitationSet'][u'MedlineCitation'])
        self.assertEqual(result['PMID'], '13189540')
        self.assertEqual(result['ArticleTitle'], '[The lupus of the lip in a three and a half year old child].')