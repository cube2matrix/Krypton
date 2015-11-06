#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Xinyang (Darren) Li


def run_in_thread(func, *args, **kwargs):
    '''
    Run function in thread
    '''
    from threading import Thread
    thread = Thread(target=func, args=args, kwargs=kwargs)
    thread.daemon = True
    thread.start()
    return thread

def run_in_subprocess(func, *args, **kwargs):
    '''
    Run function in subprocess
    '''
    from multiprocessing import Process
    thread = Process(target=func, args=args, kwargs=kwargs)
    thread.daemon = True
    thread.start()
    return thread

def processPool(size):
    '''
    Run function with process pool
    '''
    from multiprocessing import Pool
    pool = Pool(size)
    return pool