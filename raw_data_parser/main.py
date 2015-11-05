#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Xinyang (Darren) Li

"""
Main entry for program
"""
import click
import time
import itertools
import sys
from parsers.pubmedParser import MedlineParser
from parsers.libs import xml
from parsers.libs import cocurrency

@click.command()
@click.option('--parse', default="pubmed", help='Which dataset need to be parsed.')
@click.option('--source', default="./test_data/pubmed/", help='source files.')
@click.option('--output', default="./test_data/save/", help='Save files.')
@click.option('--process', default=3, help='Num of cocurrency.')
def cli(parse, source, output, process):
    '''
    Use respectly parser class to parse the dataset
    '''
    if parse in ["pubmed", "medline"]:
        medlineParser(source, output, process)


def medlineProcess(url, saveDict):
    medlineParse = MedlineParser(url, saveDict)
    medlineParse.parseSingleXml()
    del medlineParse


def medlineEntry(url_path):
    return medlineProcess(*url_path)


def medlineParser(xmlDict, saveDict, numOfProcess):
    '''
    Parse medline raw data files
    '''
    path = xml.listXmlPath(xmlDict)

    pool = cocurrency.processPool(numOfProcess)

    try:
        pool.map(medlineEntry, itertools.izip(path, itertools.repeat(saveDict)))

    except Exception:
        raise

def main():
    reload(sys)
    sys.setdefaultencoding('utf8')
    cli()

if __name__ == '__main__':
    main()
