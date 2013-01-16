import multiprocessing
import time
import sys
import types
import traceback
import os
import stat
import signal
import time
import logging
import threading
import warnings
from logging import DEBUG
from logging import INFO
from logging import ERROR
from logging import CRITICAL
from logging import FATAL
from distalgo.runtime.udp import UdpEndPoint
from distalgo.runtime.tcp import TcpEndPoint
from distalgo.runtime.event import *
from distalgo.runtime.util import *
from distalgo.runtime import DistProcess
'\nCreated on Nov 14, 2012\n\n@author: KSC\n'
from genericpath import exists
from collections import defaultdict
import time
from distalgo.runtime.util import createprocs
dist_source('.', 'MapReduce.da')
from MapReduce import Master
from MapReduce import MapWorker
from MapReduce import ReduceWorker
from Utils import Utils
from KeyValueType import KeyValueType
from KeyValueType import KeyValue

def DefaultPartitioner(n):
    '\n    Default Partitioner function\n    Partiion based on hash modulo\n    '
    return lambda kv: hash(kv) % (n)


class MapReduceAPI:
    '\n    API to run execute MapReduce framework\n    '

    def __init__(self):
        '\n        Declare the configuration parameters\n        '
        self.inputFile = None
        self.inputSplitSize = 3
        self.noOfMapWorkers = 2
        self.noOfReduceWorkers = 2
        self.mapFunc = None
        self.reduceFunc = None
        self.partitionerFunc = None
        self.combinerFunc = None
        self.workerTimeout = 6
        self.skipBadRecords = False
        self.orderingGaurantee = False
        self.debugFlag = False

    def execute(self):
        if (not self.inputFile):
            raise 
            RuntimeError('inputFile is not set')
            return 
        if (not self.mapFunc):
            raise 
            RuntimeError('mapFunc not set')
            return 
        if (not self.reduceFunc):
            raise 
            RuntimeError('reduceFunc not set')
        if ((self.noOfMapWorkers <= 0) or (self.noOfReduceWorkers <= 0) or (self.workerTimeout <= 2)):
            print('one of these parameters wrongly set noOfMapWorkers, noOfReduceWorkers, workerTimeout')
            return 
        if (not self.partitionerFunc):
            print('Warning!! partitionFunc not set, setting to DefaultPartitioner')
            self.partitionerFunc = DefaultPartitioner(self.noOfReduceWorkers)
        use_channel('tcp')
        if (not exists(self.inputFile)):
            raise 
            IOError(self.inputFile & (' not found'))
            return 
        inputSplits = Utils.getInputSplits(self.inputFile, self.inputSplitSize)
        kvType = KeyValueType(str, int)
        mapOutputDir = './MapOutput/'
        mapWorkers = createprocs(MapWorker, self.noOfMapWorkers)
        for mapWorker in mapWorkers:
            setupprocs([mapWorker], [kvType, self.mapFunc, mapOutputDir, self.partitionerFunc, self.combinerFunc, self.skipBadRecords])
        kvType = KeyValueType(str, int)
        reduceOutputDir = './ReduceOutput/'
        reduceWorkers = createprocs(ReduceWorker, self.noOfReduceWorkers)
        for reduceWorker in reduceWorkers:
            setupprocs([reduceWorker], [kvType, self.reduceFunc, reduceOutputDir, self.skipBadRecords, self.orderingGaurantee])
        master = createprocs(Master, 1)
        setupprocs(master, [mapWorkers, reduceWorkers, inputSplits, self.workerTimeout, self.debugFlag])
        import glob
        for filepath in glob.glob(mapOutputDir + ('*.txt')):
            glob.os.remove(filepath)
        for filepath in glob.glob(reduceOutputDir + ('*.txt')):
            glob.os.remove(filepath)
        startprocs(mapWorkers)
        startprocs(master)
        startprocs(reduceWorkers)
        for m in master:
            m.join()