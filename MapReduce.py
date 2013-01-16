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
import datetime
from distalgo.runtime.util import createprocs
from genericpath import exists
from collections import defaultdict
import pickle
import time
from Utils import Utils
testFaultTolerance = False


class InputMetaData:
    '\n    Stores the metadata information of the splits\n    '
    inputData = list()
    isAssigned = False
    isProcessedBy = None
    countFuncCalls = 0
    countSkippedRecords = 0

    def __init__(self, inputData):
        '\n        Constructor to set the initial values of the attributes and store inputData\n        '
        self.inputData = inputData
        self.isAssigned = False
        self.isProcessedBy = None
        self.countFuncCalls = 0
        self.countSkippedRecords = 0


class WorkerMetaData:
    '\n    Stores the metadata information of the worker\n    '
    inputSplit = None
    isIdle = True
    isAlive = True
    lastRepliedTime = None
    lastPingedTime = None
    outputFiles = None

    def __init__(self, inputSplit=
    list(), outputFiles=
    list()):
        '\n        Constructor to set the initial values of the attributes and store inputSplit\n        '
        self.inputSplit = inputSplit
        self.isIdle = True
        self.isAlive = True
        self.lastRepliedTime = time.time()
        self.lastPingedTime = time.time()
        self.outputFiles = outputFiles


class Master(DistProcess):

    def __init__(self, parent, initq, channel, log):
        DistProcess.__init__(self, parent, initq, channel, log)
        self._event_patterns = [EventPattern(Event.receive, 'ReportMapWork', [], [(1, 'outputFiles'), (2, 'countMapCalls'), (3, 'countSkippedRecords')], [self._event_handler_0]), EventPattern(Event.receive, 'ReportReduceWork', [], [(1, 'outputFile'), (2, 'countReduceCalls'), (3, 'countSkippedRecords')], [self._event_handler_1]), EventPattern(Event.receive, 'MapPingResponse', [], [], [self._event_handler_2]), EventPattern(Event.receive, 'ReducePingResponse', [], [], [self._event_handler_3])]
        self._sent_patterns = []
        self._label_events = {'pingAndCheckWorkers': self._event_patterns, 'PingingWorkers': self._event_patterns, 'exit': self._event_patterns, 'ReceiveResults': self._event_patterns}
    '\n    Master process class\n    '

    def setup(self, mWorkers, rWorkers, iSplits, workerTimeout, debugFlag):
        '\n        Initial setup method for process\n        mWorkers - set of mapper workers\n        rWorkers - set of reduce workers\n        iSplits - list of inputSplits\n        workerTimeout - time in seconds denoting the maximum allowed response time for a worker\n        '
        self.inputSplits = list()
        for isplit in iSplits:
            self.inputSplits.append(
            InputMetaData(isplit))
        self.mapWorkers = defaultdict()
        for mWorker in mWorkers:
            self.mapWorkers[mWorker] = WorkerMetaData(list(), defaultdict(list))
        self.reduceWorkers = defaultdict()
        for rWorker in rWorkers:
            self.reduceWorkers[rWorker] = WorkerMetaData(list(), list())
        self.partitionSplits = list()
        self.workerTimeout = workerTimeout
        self.debugFlag = debugFlag
        self.workerTimeout = workerTimeout
        self.rWorkers = rWorkers
        self.iSplits = iSplits
        self.mWorkers = mWorkers
        self.debugFlag = debugFlag

    def main(self):
        '\n        Main method invoked when process starts, represents the main flow of execution\n        '
        self.myPrint('***** Master *****')
        self.myPrint('Input Splits: %s ' % (str([inputSplit.inputData for inputSplit in self.inputSplits])))
        startTime = time.time()
        self.initialSplitsAssignmentToWorkers(self.inputSplits, self.mapWorkers)
        pingFreq = int(self.workerTimeout / (3))
        lastPingedTime = time.time()
        while any(((not inputSplit.isProcessedBy) for inputSplit in self.inputSplits)):
            self._label_('ReceiveResults')
            currentTime = time.time()
            if (int(currentTime - (lastPingedTime)) >= pingFreq):
                self._label_('pingAndCheckWorkers')
                self.pingAndCheckWorkers(self.mapWorkers, currentTime)
                lastPingedTime = currentTime
        if (not all((inputSplit.isProcessedBy for inputSplit in self.inputSplits))):
            raise 
            RuntimeError('all inputSplits are not processed')
        self.send(('ExitCommand',), 
        self.mapWorkers.keys())
        mapOutputFiles = list()
        for mapWorker in self.mapWorkers.keys():
            mapOutputFiles.extend(
            self.mapWorkers[mapWorker].outputFiles.values())
        self.myPrint('Map output files: %s' % (mapOutputFiles))
        self.myPrint('********* Map step completed *********')
        listKeyValue = Utils.loadFromFiles(mapOutputFiles, 'rb')
        if self.debugFlag:
            totalMapValues = sum((v for (k, v) in listKeyValue))
            self.myPrint(totalMapValues)
        self.myPrint(listKeyValue)
        partitionOuputFiles = defaultdict(list)
        for mapWorker in self.mapWorkers.keys():
            for partitionKey in self.mapWorkers[mapWorker].outputFiles.keys():
                partitionOuputFiles[partitionKey].append(self.mapWorkers[mapWorker].outputFiles[partitionKey])
        self.myPrint('Grouped partition files: %s' % (partitionOuputFiles))
        for key in partitionOuputFiles.keys():
            self.partitionSplits.append(
            InputMetaData(partitionOuputFiles[key]))
        self.initialSplitsAssignmentToWorkers(self.partitionSplits, self.reduceWorkers)
        pingFreq = int(self.workerTimeout / (3))
        lastPingedTime = time.time()
        while any(((not partitionSplit.isProcessedBy) for partitionSplit in self.partitionSplits)):
            self._label_('ReceiveResults')
            currentTime = time.time()
            if (int(currentTime - (lastPingedTime)) >= pingFreq):
                self._label_('pingAndCheckWorkers')
                self.pingAndCheckWorkers(self.reduceWorkers, currentTime)
                lastPingedTime = currentTime
        self.send(('ExitCommand',), 
        self.reduceWorkers.keys())
        reduceOutputFiles = list()
        for reduceWorker in self.reduceWorkers.keys():
            reduceOutputFiles.extend(self.reduceWorkers[reduceWorker].outputFiles)
        self.myPrint('Reduce output files: %s' % (reduceOutputFiles))
        self.myPrint('********* Reduce step completed *********')
        listKeyValue = Utils.loadFromFiles(reduceOutputFiles, 'rb')
        if self.debugFlag:
            totalReduceValues = sum((v for (k, v) in listKeyValue))
            self.myPrint(totalReduceValues)
        self.myPrint(listKeyValue)
        if self.debugFlag:
            if (totalMapValues != totalReduceValues):
                raise 
                RuntimeError('outputs of Map and Reduce steps do not match')
        endTime = time.time()
        self.getStats()
        self._label_('exit')
        self.myPrint('Master Exiting')
        print('\n')
        print('No of Mappers: %d' % (len(self.mapWorkers)))
        print('No of Reducers: %d' % (len(self.reduceWorkers)))
        print('No of Map tasks: %d' % (self.noOfMapTasks()))
        print('No of Reduce tasks: %d' % (self.noOfReduceTasks()))
        print('No of failed Map workers: %d' % (self.noOfFailedMapWorkers()))
        print('No of failed Reduce workers: %d' % (self.noOfFailedReduceWorkers()))
        print('No of Map function calls: %d' % (self.noOfUserMapCalls()))
        print('No of Reduce function calls: %d' % (self.noOfUserReduceCalls()))
        print('Total time elapsed: %.2fsec' % (endTime - (startTime)))

    def setIsAlive(self, toBeRemoved, workers):
        '\n        Function to set the attributes like isAlive etc. flags when a worker is removed\n        toBeRemoved - list of workers to be removed\n        workers - complete list of workers\n        '
        for worker in toBeRemoved:
            self.myPrint('********* Removing the worker %s *********' % (worker))
            for inputSplit in self.inputSplits:
                if (inputSplit.isProcessedBy == worker):
                    inputSplit.isAssigned = False
                    inputSplit.isProcessedBy = False
            workers[worker].inputSplit.isAssigned = False
            workers[worker].inputSplit.isProcessedBy = False
            workers[worker].isAlive = False

    def pingAndCheckWorkers(self, workers, currentTime):
        '\n        Function to ping and check liveness of workers\n        workers - dictionary of workers\n        currentTime - time to be set a ping time\n        '
        toBePinged = list()
        toBeRemoved = list()
        for i in workers.keys():
            if (workers[i].isAlive == True):
                if (workers[i].lastRepliedTime >= workers[i].lastPingedTime):
                    toBePinged.append(i)
                    workers[i].lastPingedTime = currentTime
                elif (int(currentTime - (workers[i].lastRepliedTime)) >= self.workerTimeout):
                    toBeRemoved.append(i)
        self._label_('PingingWorkers')
        if toBePinged:
            self.myPrint('********* Pinging workers %s *********' % (toBePinged))
            self.send(('Ping', currentTime), toBePinged)
        self.setIsAlive(toBeRemoved, workers)

    def initialSplitsAssignmentToWorkers(self, splits, workers):
        '\n        Initial method to assign splits to the workers\n        splits - list of splits to be assigned\n        workers - dictionary of workers\n        '
        for split in splits:
            if ((not split.isProcessedBy) and (not split.isAssigned)):
                worker = self.getIdleWorker(workers)
                if worker:
                    self.assignSplitToWorker(split, worker, workers[worker])
                else:
                    break

    def assignSplitToWorker(self, split, worker, workerMetaData):
        '\n        Method to assign a split to worker and update the corresponding fields\n        split - split to be assigned\n        worker - worker to which split has be assigned\n        workerMetaData - worker meta data for the corresponding worker\n        '
        if ((not split) or (not worker)):
            return 
        split.isAssigned = True
        workerMetaData.inputSplit = split
        workerMetaData.isIdle = False
        self.send(('AssignWork', split.inputData), worker)
        self.myPrint('Assigned split %s to %s' % ((str(split.inputData), worker._address[1])))

    def getUnProcessedSplit(self, splits):
        '\n        Get an unprocessed and unassigned split from the list of splits\n        splits - list of splits\n        '
        for split in splits:
            if ((not split.isProcessedBy) and (not split.isAssigned)):
                return split
        return None

    def getIdleWorker(self, workers):
        '\n        Get an idle worker from the set of workers\n        workers - dict of workers\n        '
        for worker in workers:
            if (workers[worker].isIdle and (workers[worker].isAlive == True)):
                return worker
        return None

    def _event_handler_0(self, outputFiles, countMapCalls, countSkippedRecords, _timestamp, _source):
        '\n        Method invoked when work is reported from a map worker\n        '
        self.myPrint('MapWork reported from ' + (str(_source)))
        mapWorker = _source
        self.setWorkerMetaData(mapWorker, self.mapWorkers, countMapCalls, countSkippedRecords)
        self.mapWorkers[mapWorker].outputFiles.update(outputFiles)
        inputSplit = self.getUnProcessedSplit(self.inputSplits)
        self.assignSplitToWorker(inputSplit, mapWorker, self.mapWorkers[mapWorker])

    def _event_handler_1(self, outputFile, countReduceCalls, countSkippedRecords, _timestamp, _source):
        '\n        Method invoked when work is reported from a reduce worker\n        '
        self.myPrint('ReduceWork reported from ' + (str(_source)))
        reduceWorker = _source
        self.setWorkerMetaData(reduceWorker, self.reduceWorkers, countReduceCalls, countSkippedRecords)
        self.reduceWorkers[reduceWorker].outputFiles.append(outputFile)
        partitionSplit = self.getUnProcessedSplit(self.partitionSplits)
        self.assignSplitToWorker(partitionSplit, reduceWorker, self.reduceWorkers[reduceWorker])

    def setWorkerMetaData(self, worker, workers, countFuncCalls, countSkippedRecords):
        '\n        Helper method, used to modularize the code\n        '
        workers[worker].inputSplit.isAssigned = False
        workers[worker].inputSplit.isProcessedBy = worker
        workers[worker].inputSplit.countFuncCalls+=countFuncCalls
        workers[worker].inputSplit.countSkippedRecords+=countSkippedRecords
        workers[worker].inputSplit = None
        workers[worker].isIdle = True
        workers[worker].lastRepliedTime = time.time()

    def _event_handler_2(self, _timestamp, _source):
        '\n        Method invoked when ping is received from a map worker\n        '
        self.myPrint('Received mapper ping response from ' + (str(_source)))
        self.mapWorkers[_source].lastRepliedTime = time.time()

    def _event_handler_3(self, _timestamp, _source):
        '\n        Method invoked when ping is received from a reduce worker\n        '
        self.myPrint('Received reducer ping response from ' + (str(_source)))
        self.reduceWorkers[_source].lastRepliedTime = time.time()

    def noOfMapTasks(self):
        '\n        Returns the total no of map tasks\n        '
        return len(self.inputSplits)

    def noOfReduceTasks(self):
        '\n        Returns the total no of reduce tasks\n        '
        return len(self.partitionSplits)

    def noOfFailedMapWorkers(self):
        '\n        Returns the no of failed map workers\n        '
        return len([worker for worker in self.mapWorkers if (self.mapWorkers[worker].isAlive == False)])

    def noOfFailedReduceWorkers(self):
        '\n        Returns the no of failed reduce workers\n        '
        return len([worker for worker in self.reduceWorkers if (self.reduceWorkers[worker].isAlive == False)])

    def noOfUserMapCalls(self):
        '\n        Returns the no of calls to user map function\n        '
        return sum([split.countFuncCalls for split in self.inputSplits if (split.isProcessedBy != None)])

    def noOfUserReduceCalls(self):
        '\n        Returns the no of calls to user reduce function\n        '
        return sum([split.countFuncCalls for split in self.partitionSplits if (split.isProcessedBy != True)])

    def getStats(self):
        '\n        Returns the tuple of counters/stats\n        '
        return (self.noOfMapTasks(), self.noOfReduceTasks(), self.noOfFailedMapWorkers(), self.noOfFailedReduceWorkers(), self.noOfUserMapCalls(), self.noOfUserReduceCalls())

    def myPrint(self, comment):
        '\n        Helper print function\n        used mainly for debugging purposes\n        '
        print(
        datetime.datetime.now().strftime('%H:%M:%S.%f'), '|', 
        str(self._id._address[1]), '| Master  |', comment)
        sys.stdout.flush()


class MapWorker(DistProcess):

    def __init__(self, parent, initq, channel, log):
        DistProcess.__init__(self, parent, initq, channel, log)
        self._event_patterns = [EventPattern(Event.receive, 'ExitCommand', [], [], [self._receive_handler_0]), EventPattern(Event.receive, 'AssignWork', [], [(1, 'inputData')], [self._event_handler_1]), EventPattern(Event.receive, 'Ping', [], [(1, 'currentTime')], [self._event_handler_2])]
        self._sent_patterns = []
        self._label_events = {'ReceiveWork': self._event_patterns, 'exit': self._event_patterns}
        self._receive_messages_0 = list()
    '\n    Mapper process class\n    '

    def setup(self, kvType, mapFunc, outputDir, partitionerFunc, combinerFunc=None, skipBadRecords=False):
        '\n        Initial setup method for process\n        kvType - defines the data type of input splits\n        mapFunc - user Map function\n        outputDir - outputDir into which output files has to be written\n        partitionerFunc - user Parititioner function\n        combinerFunc - user Combiner function\n        skipBadRecords - boolean flag defining whether to skip bad records or not\n        '
        self.kvType = kvType
        self.mapFunc = mapFunc
        self.outputDir = outputDir
        self.combinerFunc = combinerFunc
        self.partitionerFunc = partitionerFunc
        self.skipBadRecords = skipBadRecords
        self.skipBadRecords = skipBadRecords
        self.combinerFunc = combinerFunc
        self.kvType = kvType
        self.partitionerFunc = partitionerFunc
        self.mapFunc = mapFunc
        self.outputDir = outputDir

    def Map(self, inputData):
        '\n        Method to call the user Map function on each record in the assigned split\n        '
        countMapCalls = 0
        countSkippedRecords = 0
        result = list()
        for input in inputData:
            try:
                result.extend(
                self.mapFunc(input))
                countMapCalls+=1
            except:
                if self.skipBadRecords:
                    countSkippedRecords+=1
                else:
                    raise 
        if self.combinerFunc:
            partitionedData = defaultdict(list)
            for (key, value) in result:
                partitionedData[key].append(value)
            result = list()
            for key in partitionedData.keys():
                result.append(
                self.combinerFunc((key, partitionedData[key])))
        return (result, countMapCalls, countSkippedRecords)

    def _event_handler_1(self, inputData, _timestamp, _source):
        '\n        Method invoked when Master assigns work to this worker\n        '
        time.sleep(2)
        self.myPrint('Received input split: ' + (str(inputData)))
        outputFile = self.outputDir + (str(time.time()).replace('.', '')) + ('_output_') + (str(self._id._address[1])) + ('.txt')
        (listKeyValue, countMapCalls, countSkippedRecords) = self.Map(inputData)
        partitions = self.partitioner(listKeyValue)
        partitionOuputFiles = dict()
        for key in partitions.keys():
            outputFile = self.outputDir + (str(self._id._address[1])) + ('_output_') + (str(key)) + ('.txt')
            self.myPrint('writing result to ' + (outputFile))
            Utils.writeResult(partitions[key], outputFile, 'a+b')
            partitionOuputFiles[key] = outputFile
        if ((not testFaultTolerance) or (self._id._address[1] % (2) == 0)):
            self.send(('ReportMapWork', partitionOuputFiles, countMapCalls, countSkippedRecords), _source)

    def partitioner(self, listKeyValue):
        '\n        Calls the user partitioner function on Map output and create paritions\n        '
        partitions = defaultdict(list)
        for kv in listKeyValue:
            partitionKey = self.partitionerFunc(kv)
            partitions[partitionKey].append(kv)
        return partitions

    def _event_handler_2(self, currentTime, _timestamp, _source):
        '\n        Invoked when Master process pings this worker\n        '
        self.myPrint('Pinged')
        if ((not testFaultTolerance) or (self._id._address[1] % (2) == 0)):
            self.send(('MapPingResponse',), _source)

    def main(self):
        '\n        Main method invoked when the process starts\n        '
        self.myPrint('***** MapWorker *****')
        self._label_('ReceiveWork')
        while (not self._has_receive_0()):
            self._process_event(self._event_patterns, True, None)
        self._label_('exit')
        self.myPrint('MapWorker Exiting')

    def myPrint(self, comment):
        '\n        Helper print function\n        used mainly for debugging purposes\n        '
        print(
        datetime.datetime.now().strftime('%H:%M:%S.%f'), '|', 
        str(self._id._address[1]), '| Mapper  |', comment)
        sys.stdout.flush()

    def _receive_handler_0(self, _timestamp, _source):
        self._receive_messages_0.append(True)

    def _has_receive_0(self):
        return {True for v_ in self._receive_messages_0}


class ReduceWorker(DistProcess):

    def __init__(self, parent, initq, channel, log):
        DistProcess.__init__(self, parent, initq, channel, log)
        self._event_patterns = [EventPattern(Event.receive, 'ExitCommand', [], [], [self._receive_handler_0]), EventPattern(Event.receive, 'AssignWork', [], [(1, 'partitionFiles')], [self._event_handler_1]), EventPattern(Event.receive, 'Ping', [], [(1, 'currentTime')], [self._event_handler_2])]
        self._sent_patterns = []
        self._label_events = {'ReceiveWork': self._event_patterns, 'exit': self._event_patterns}
        self._receive_messages_0 = list()
    '\n    Reducer process class\n    '

    def setup(self, kvType, reduceFunc, outputDir, skipBadRecords=False, orderingGaurantee=True):
        '\n        Initial setup method for process\n        kvType - defines the data type of input splits\n        reduceFunc - user Reduce function\n        outputDir - outputDir into which output files has to be written\n        skipBadRecords - boolean flag defining whether to skip bad records or not\n        orderingGaurantee - boolean flag defining whether output has to be ordered or not\n        '
        self.kvType = kvType
        self.reduceFunc = reduceFunc
        self.outputDir = outputDir
        self.skipBadRecords = skipBadRecords
        self.orderingGaurantee = orderingGaurantee
        self.kvType = kvType
        self.reduceFunc = reduceFunc
        self.outputDir = outputDir
        self.skipBadRecords = skipBadRecords
        self.orderingGaurantee = orderingGaurantee

    def Reduce(self, partitionFiles):
        '\n        Method to call the user Reduce function on unique <key, list(values)>\n        '
        countReduceCalls = 0
        countSkippedRecords = 0
        result = list()
        listKeyValue = Utils.loadFromFiles(partitionFiles, 'rb')
        partitionedData = defaultdict(list)
        for (key, value) in listKeyValue:
            partitionedData[key].append(value)
        if self.orderingGaurantee:
            listKeys = sorted(partitionedData.keys())
        else:
            listKeys = partitionedData.keys()
        for key in listKeys:
            try:
                result.append(
                self.reduceFunc((key, partitionedData[key])))
                countReduceCalls+=1
            except:
                if self.skipBadRecords:
                    countSkippedRecords+=1
                else:
                    raise 
        return (result, countReduceCalls, countSkippedRecords)

    def _event_handler_1(self, partitionFiles, _timestamp, _source):
        '\n        Method invoked when Master assigns work to this process\n        '
        time.sleep(2)
        self.myPrint('Received partition split files: ' + (str(partitionFiles)))
        outputFile = self.outputDir + (str(time.time()).replace('.', '')) + ('_output_') + (str(self._id._address[1])) + ('.txt')
        (result, countReduceCalls, countSkippedRecords) = self.Reduce(partitionFiles)
        self.myPrint('writing result to ' + (outputFile))
        Utils.writeResult(result, outputFile, 'wb')
        if ((not testFaultTolerance) or (self._id._address[1] % (3) == 0)):
            self.send(('ReportReduceWork', outputFile, countReduceCalls, countSkippedRecords), _source)

    def _event_handler_2(self, currentTime, _timestamp, _source):
        '\n        Method invoked when Master process pings this worker\n        '
        self.myPrint('Pinged')
        if ((not testFaultTolerance) or (self._id._address[1] % (3) == 0)):
            self.send(('ReducePingResponse',), _source)

    def main(self):
        '\n        Main method invoked when process starts\n        '
        self.myPrint('***** ReduceWorker *****')
        self._label_('ReceiveWork')
        while (not self._has_receive_0()):
            self._process_event(self._event_patterns, True, None)
        self._label_('exit')
        self.myPrint('ReduceWorker Exiting')

    def myPrint(self, comment):
        '\n        Helper print function\n        used mainly for debugging purposes\n        '
        print(
        datetime.datetime.now().strftime('%H:%M:%S.%f'), '|', 
        str(self._id._address[1]), '| Reducer |', comment)
        sys.stdout.flush()

    def _receive_handler_0(self, _timestamp, _source):
        self._receive_messages_0.append(True)

    def _has_receive_0(self):
        return {True for v_ in self._receive_messages_0}