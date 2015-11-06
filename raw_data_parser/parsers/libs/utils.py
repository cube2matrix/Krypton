#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import json
import datetime
from dateutil import parser


def parseDate(dateString):
    '''
    Parse a date string to a date object
    '''
    try:
        date = parser.parse(dateString)
    except:
        date = datetime.datetime(1970, 1, 1, 10, 15)
    return date

def epochDelta(date):
    '''
    Total seconds from epoch(unix time 1970-01-01)
    '''
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = date - epoch
    return int(delta.total_seconds())

def parseEpochDeltaFromDate(dateString):
    return epochDelta(parseDate(dateString))