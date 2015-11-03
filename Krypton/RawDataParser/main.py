#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Xinyang (Darren) Li

"""
Main entry for program
"""
import time
import itertools
import sys
import argparse
from parsers.pubmedParser import MedlineParser
from parsers.libs import xml
from parsers.libs import cocurrency

def cli():
    '''
    Use respectly parser class to parse the dataset
    '''

    # parse command line arguments
    parser = argparse.ArgumentParser(description="Krypton Service")

    parser.add_argument("--source",
                        required=True,
                        metavar='AVALIABLE_ROOM',
                        help="source directory")
    parser.add_argument("--output",
                        required=True,
                        metavar='SCHEDULE_DATE',
                        help="destination directory")
    parser.add_argument("--process",
                        required=False,
                        default=3,
                        metavar='CHECK_OUT_DATE',
                        help="nounmber of processes")

    # Try parse all args, if any args missed, the except will be caught, and
    # print error message and print help information, finally exit program
    try:
        args=parser.parse_args()
    except:
        sys.exit(0)

    medlineParser(args.source.strip(), args.output.strip(), args.process)


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
