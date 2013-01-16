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
dist_source('.', 'MapReduceAPI.da')
from MapReduceAPI import MapReduceAPI
import re
pattern = 'first'
regex = re.compile(pattern)

def MatchLine(input):
    '\n    User Map function\n    input - filename\n    Sanitize the line and report (line, matchedstring) for each line\n    '
    filename = input
    output = list()
    import string
    TT = ''.maketrans(string.punctuation, ' ' * (len(string.punctuation)))
    print('<MapFunc> Match: Reading', 
    str(filename))
    import re
    with open(filename, 'rt') as f:
        for line in f:
            line = line.translate(TT)
            mo = regex.search(line)
            if mo:
                output.append((line, 
                mo.group(0)))
    return output

def OutputLine(input):
    '\n    User Reduce function\n    input - matched line, value\n    Output the matched line along with the matched string\n    '
    (key, values) = input
    for value in values:
        return (key, value)

def ModuloFirstLetter(kv):
    '\n    User Partition function\n    Partition mapped values by their first letter\n    '
    (key, value) = kv
    return ord(key[0]) % (4)

def main():
    MRAPI = MapReduceAPI()
    MRAPI.inputFile = str(sys.argv[1]) if (len(sys.argv) > 1) else './inputFile.txt'
    MRAPI.inputSplitSize = int(sys.argv[2]) if (len(sys.argv) > 1) else 3
    MRAPI.noOfMapWorkers = int(sys.argv[3]) if (len(sys.argv) > 1) else 2
    MRAPI.noOfReduceWorkers = int(sys.argv[4]) if (len(sys.argv) > 1) else 2
    MRAPI.mapFunc = MatchLine
    MRAPI.reduceFunc = OutputLine
    MRAPI.partitionerFunc = ModuloFirstLetter
    MRAPI.combinerFunc = None
    MRAPI.workerTimeout = 6
    MRAPI.skipBadRecords = False
    MRAPI.orderingGaurantee = True
    MRAPI.debugFlag = False
    MRAPI.execute()