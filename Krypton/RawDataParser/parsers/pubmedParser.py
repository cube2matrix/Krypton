#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Xinyang Li
# encoding=utf8

"""
MedlineParser provide the parser class to parse medline data and pubmed
"""
from libs import utils
from libs import saxypy
from collections import OrderedDict
import tablib
import os
import codecs


__all__ = [
    'parseSingleXml',
    'parseSingleArticle'
]


class MedlineParser(object):
    '''
    Parser for Medline/PubMed
    '''
    __slots__ = ['xmlPath', 'saveDirc', 'saveFile', 'fileObj', 'headers', 'data']

    def __init__(self, xmlPath, saveDirc):
        self.xmlPath = xmlPath
        self.saveDirc = saveDirc
        self.saveFile = self.generateSaveFileName()
        self.fileObj = open(self.saveFile, 'wb')
        self.headers = ()
        self.data = tablib.Dataset()

    def generateSaveFileName(self):
        '''
        generate filename of saveing file
        '''
        head, tail = os.path.split(self.xmlPath)
        filename = tail.split('.')[0]
        return self.saveDirc + filename + '.tsv'

    def setHeader(self, dataDict):
        '''
        Fill the csv file with header
        '''
        self.headers = (key for key in dataDict.keys())
        self.data.headers = self.headers

    def saveToTsv(self):
        '''
        Dump the data into tsv
        '''
        with codecs.open(self.saveFile, 'wb', encoding="utf-8-sig") as f:
            f.write(self.data.tsv.encode('utf-8'))

    def appendTsv(self, data):
        self.fileObj.write(data)

    def appendData(self, article):
        '''
        Append article in csv data dictionary
        '''
        parsedDict = self.parseSingleArticle(article)
        # print parsedDict
        # if self.data.headers is None:
        #     self.setHeader(parsedDict)

        values = [value for key, value in parsedDict.iteritems()]
        self.data.append(tuple(values))

    def tryGetAttr(self, root, attr):
        try:
            return root[attr]
        except Exception, e:
            return ''

    def parseSingleXml(self):
        '''
        Given single xml path, extract information from xml file
        and return as a list
        '''
        try:
            # Init the xml dict
            print self.xmlPath
            xmlDict = saxypy.parse(open(self.xmlPath, 'r'))
            # xmlDict = saxypy.parse(np.loadtxt(self.xmlPath))
            root = xmlDict[u'MedlineCitationSet']

            try:
                root = root[u'MedlineCitation']
            except Exception as e:
                root = root

            # for every article in this file
            if isinstance(root, dict):
                self.appendData(root)
            elif isinstance(root, list):
                for article in root:
                    """
                    each file may contains many articles, deal with each other one by one
                    """
                    self.appendData(article)
        except Exception, e:
            raise e
        finally:
            del root
            del xmlDict
            self.saveToTsv()
            self.fileObj.close()

    def parsePMID(self, article):
        '''
        Parse PMID
        '''
        try:
            PMID = article[u'PMID'][u'*content*']
        except:
            PMID = ''
        return PMID

    def parseDateCreated(self, article):
        try:
            DateCreatedYear = article[u'DateCreated'][u'Year']
        except:
            DateCreatedYear = '1500'
        try:
            DateCreatedMonth = article[u'DateCreated'][u'Month']
        except:
            DateCreatedMonth = '01'
        try:
            DateCreatedDay = article[u'DateCreated'][u'Day']
        except:
            DateCreatedDay = '01'
        DateCreated = DateCreatedMonth + '-' + DateCreatedDay + '-' + DateCreatedYear
        return utils.parseEpochDeltaFromDate(DateCreated)


    def parseDateCompleted(self, article):
        try:
            DateCompletedYear = article[u'DateCompleted'][u'Year']
        except:
            DateCompletedYear = '1500'
        try:
            DateCompletedMonth = article[u'DateCompleted'][u'Month']
        except:
            DateCompletedMonth = '01'
        try:
            DateCompletedDay = article[u'DateCompleted'][u'Day']
        except:
            DateCompletedDay = '01'
        DateCompleted = DateCompletedMonth + '-' + DateCompletedDay + '-' + DateCompletedYear
        return utils.parseEpochDeltaFromDate(DateCompleted)


    def parseDateRevised(self, article):
        try:
            DateRevisedYear = article[u'DateRevised'][u'Year']
        except:
            DateRevisedYear = '1500'
        try:
            DateRevisedMonth = article[u'DateRevised'][u'Month']
        except:
            DateRevisedMonth = '01'
        try:
            DateRevisedDay = article[u'DateRevised'][u'Day']
        except:
            DateRevisedDay = '01'
        DateRevised = DateRevisedMonth + '-' + DateRevisedDay + '-' + DateRevisedYear
        return utils.parseEpochDeltaFromDate(DateRevised)


    def parseJournalInfo(self, article):
        JournalInfo = {}

        try:
            JournalInfo['Journal'] = article[u'Article'][u'Journal'][u'Title']
        except:
            JournalInfo['Journal'] = ''
        try:
            JournalInfo['ISSN'] = article[u'Article'][u'Journal'][u'ISSN'][u'*content*']
        except:
            JournalInfo['ISSN'] = ''
        try:
            JournalInfo['Volume'] = article[u'Article'][u'Journal'][u'JournalIssue'][u'Volume']
        except:
            JournalInfo['Volume'] = ''
        try:
            JournalInfo['Issue'] = article[u'Article'][u'Journal'][u'JournalIssue'][u'Issue']
        except:
            JournalInfo['Issue'] = ''
        try:
            PubDateYear = article[u'Article'][u'Journal'][u'JournalIssue'][u'PubDate'][u'Year']
        except:
            PubDateYear = '1500'
        try:
            PubDateMonth = article[u'Article'][u'Journal'][u'JournalIssue'][u'PubDate'][u'Month']
        except:
            PubDateMonth = '01'
        try:
            PubDateDay = article[u'Article'][u'Journal'][u'JournalIssue'][u'PubDate'][u'Day']
        except:
            PubDateDay = '01'
        JournalInfo['PubDate'] = utils.parseEpochDeltaFromDate(PubDateMonth + '-' + PubDateDay + '-' + PubDateYear)
        try:
            JournalInfo['ISOAbbreviation'] = article[u'Article'][u'Journal'][u'ISOAbbreviation']
        except:
            JournalInfo['ISOAbbreviation'] = ''

        return JournalInfo


    def parseArticleTitle(self, article):
        try:
            ArticleTitle = article[u'Article'][u'ArticleTitle']
        except:
            ArticleTitle = ''
        return ArticleTitle.replace("\\", '')


    def parseElocationId(self, article):
        try:
            ELocationID = article[u'Article'][u'ELocationID'][u'*content*']
        except:
            ELocationID = ''
        return ELocationID


    def parseNlmId(self, article):
        try:
            NlmUniqueID = article[u'MedlineJournalInfo'][u'NlmUniqueID']
        except:
            NlmUniqueID = ''
        return NlmUniqueID


    def parseCountry(self, article):
        try:
            Country = article[u'MedlineJournalInfo'][u'Country']
        except:
            Country = ''
        return Country


    def parseAbstract(self, article):
        try:
            Abstract = article[u'Article'][u'Abstract'][u'AbstractText']
        except:
            try:
                Abstract = article[u'Article'][u'Abstract']
            except:
                Abstract = ''

        finalAbstract = ''

        if isinstance(Abstract, list):
            for item in Abstract:
                finalAbstract += self.tryGetAttr(item, u'*content*').replace("\\", '').encode('utf-8')
        elif isinstance(Abstract, dict):
            finalAbstract += self.tryGetAttr(Abstract, u'*content*').replace("\\", '').encode('utf-8')
        else:
            finalAbstract = Abstract.replace("\\", '').encode('utf-8')
        return finalAbstract


    def parseAuthor(self, article):
        try:
            Authors = []

            Author = article[u'Article'][u'AuthorList'][u'Author']
            if isinstance(Author, list):
                for item in Author:
                    Authors.append({
                        "LastName": self.tryGetAttr(item, u'LastName').encode('utf-8'),
                        "ForeName": self.tryGetAttr(item, u'ForeName').encode('utf-8'),
                        "Suffix": self.tryGetAttr(item, u'Suffix').encode('utf-8'),
                        "Initials": self.tryGetAttr(item, u'Initials').encode('utf-8'),
                        "Identifier": self.tryGetAttr(item, u'Identifier').encode('utf-8'),
                        "AffiliationInfo": self.tryGetAttr(self.tryGetAttr(item, u'AffiliationInfo'), u'Affiliation').encode('utf-8')
                    })
            elif isinstance(Author, dict):
                Authors.append({
                    "LastName": self.tryGetAttr(Author, u'LastName').encode('utf-8'),
                    "ForeName": self.tryGetAttr(Author, u'ForeName').encode('utf-8'),
                    "Suffix": self.tryGetAttr(Author, u'Suffix').encode('utf-8'),
                    "Initials": self.tryGetAttr(Author, u'Initials').encode('utf-8'),
                    "Identifier": self.tryGetAttr(Author, u'Identifier').encode('utf-8'),
                    "AffiliationInfo": self.tryGetAttr(Author, u'AffiliationInfo').encode('utf-8')
                })
            else:
                pass
        except:
            Authors = []
        return Authors


    def parseLanguage(self, article):
        try:
            Language = article[u'Article'][u'Language']
        except:
            Language = ''
        return Language


    def parseGrants(self, article):
        try:
            Grants = []

            Grant = article[u'Article'][u'GrantList'][u'Grant']

            if isinstance(Grant, list):
                for item in Grant:
                    Grants.append({
                        "GrantID": self.tryGetAttr(item, u'GrantID').encode('utf-8'),
                        "Acronym": self.tryGetAttr(item, u'Acronym').encode('utf-8'),
                        "Agency": self.tryGetAttr(item, u'Agency').encode('utf-8'),
                        "Country": self.tryGetAttr(item, u'Country').encode('utf-8'),
                    })
            elif isinstance(Grant, dict):
                Grants.append({
                    "GrantID": self.tryGetAttr(Grant, u'GrantID').encode('utf-8'),
                    "Acronym": self.tryGetAttr(Grant, u'Acronym').encode('utf-8'),
                    "Agency": self.tryGetAttr(Grant, u'Agency').encode('utf-8'),
                    "Country": self.tryGetAttr(Grant, u'Country').encode('utf-8'),
                })
            else:
                pass
        except:
            Grants = []
        return Grants


    def parsePubType(self, article):
        try:
            PublicationTypes = []

            PublicationType = article[u'Article'][u'PublicationTypeList']

            if isinstance(PublicationType, list):
                for item in PublicationType:
                    PublicationTypeList.append(item[u'*content*'].replace("\\", '').encode('utf-8'))
            elif isinstance(PublicationType, dict):
                PublicationTypes.append(PublicationType[u'*content*'].replace("\\", '').encode('utf-8'))
            else:
                pass
        except:
            PublicationTypes = []
        return PublicationTypes


    def parseMeshHeading(self, article):
        try:
            MeshHeadings = []

            for MeshHeading in article[u'MeshHeadingList']:
                try:
                    MeshHeadings.append(MeshHeading[u'DescriptorName'][u'*content*'].replace("\\", '').encode('utf-8'))

                    QualifierName = MeshHeading[u'QualifierName']

                    if isinstance(QualifierName, list):
                        for item in QualifierName:
                            MeshHeadings.append(item[u'*content*'].replace("\\", '').encode('utf-8'))
                    elif isinstance(QualifierName, dict):
                        MeshHeadings.append(MeshHeading[u'*content*'].replace("\\", '').encode('utf-8'))
                    else:
                        continue
                except:
                    continue
        except:
            MeshHeadings = []
        return MeshHeadings


    def parseKeywords(self, article):
        try:
            Keywords = []

            Keyword = article[u'KeywordList'][u'Keyword']

            if isinstance(Keyword, list):
                for item in Keyword:
                    Keywords.append(item[u'*content*'].replace("\\", '').encode('utf-8'))
            elif isinstance(Keyword, dict):
                Keywords.append(Keyword[u'*content*'].replace("\\", '').encode('utf-8'))
            else:
                pass
        except:
            Keywords = []
        return Keywords


    def parseSingleArticle(self, article):
        '''
        Given single article in xml file, extract information from this article and return list
        '''
        # PMID
        PMID = self.parsePMID(article)

        # DateCreated
        DateCreated = self.parseDateCreated(article)

        # DateCompleted
        DateCompleted = self.parseDateCompleted(article)

        # DateRevised
        DateRevised = self.parseDateRevised(article)

        # Journal Infomation
        JournalInfo = self.parseJournalInfo(article)
        Journal = JournalInfo['Journal']
        ISSN = JournalInfo['ISSN']
        Volume = JournalInfo['Volume']
        Issue = JournalInfo['Issue']
        PubDate = JournalInfo['PubDate']
        ISOAbbreviation = JournalInfo['ISOAbbreviation']

        # ArticleTitle
        ArticleTitle = self.parseArticleTitle(article)

        # ELocationID
        ELocationID = self.parseElocationId(article)

        # NlmUniqueID
        NlmUniqueID = self.parseNlmId(article)

        # Country
        Country = self.parseCountry(article)

        # Abstract/AbstractText
        Abstract = self.parseAbstract(article)

        # Authors
        Authors = self.parseAuthor(article)

        # Language
        Language = self.parseLanguage(article)

        # GrantList
        Grants = self.parseGrants(article)

        # PublicationTypeList
        PublicationTypes = self.parsePubType(article)

        # MeshHeadingList
        MeshHeadings = self.parseMeshHeading(article)

        # KeywordList
        Keywords = self.parseKeywords(article)

        # Final Parse Result
        result = OrderedDict()
        result['PMID'] = PMID
        result['ArticleTitle'] = ArticleTitle.strip('\\')
        result['Abstract'] = Abstract
        # result['Authors'] = Authors
        # result['Grants'] = Grants
        # result['PublicationTypes'] = '|'.join(PublicationTypes).strip('\\').replace('\t', '')
        # result['MeshHeadings'] = '|'.join(MeshHeadings).strip('\\').replace('\t', '')
        result['Keywords'] = '|'.join(Keywords).strip('\\').replace('\t', '')
        # result['ELocationID'] = ELocationID.replace('\\', '')
        # result['ISOAbbreviation'] = ISOAbbreviation
        # result['Language'] = Language
        # result['Journal'] = Journal
        # result['Volume'] = Volume
        # result['Issue'] = Issue
        # result['ISSN'] = ISSN
        # result['NlmUniqueID'] = NlmUniqueID
        # result['Country'] = Country
        # result['DateCreated'] = DateCreated
        # result['DateCompleted'] = DateCompleted
        # result['DateRevised'] = DateRevised
        # result['PubDate'] = PubDate
        # result = {
        #     "PMID": PMID,
        #     "ArticleTitle": ArticleTitle,
        #     "DateCreated": DateCreated,
        #     "DateCompleted": DateCompleted,
        #     "DateRevised": DateRevised,
        #     "PubDate": PubDate,
        #     "Volume": Volume,
        #     "Journal": Journal,
        #     "Issue": Issue,
        #     "ISSN": ISSN,
        #     "ISOAbbreviation": ISOAbbreviation,
        #     "ELocationID": ELocationID,
        #     "NlmUniqueID": NlmUniqueID,
        #     "Abstract": Abstract,
        #     "Authors": Authors,
        #     "Language": Language,
        #     "Grants": Grants,
        #     "PublicationTypes": '|'.join(PublicationTypes),
        #     "MeshHeadings": '|'.join(MeshHeadings),
        #     "Keywords": '|'.join(Keywords),
        #     "Country": Country
        # }

        return result
