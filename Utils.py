# @PydevCodeAnalysisIgnore
'''
Created on Nov 14, 2012

@author: KSC
'''
import pickle

class Utils():
    '''
    Set of utility functions that are used across the library
    '''
    @staticmethod
    def loadFromFiles(outputFiles, mode):
        '''
        Unpickle the key value from the outputFiles
        '''
        listKeyValue = list()
        for file in outputFiles:
            with open(file, mode) as f:
                while True:
                    try:
                        listKeyValue.extend(pickle.load(f))
                    except EOFError:
                        break
        return listKeyValue
    
    @staticmethod
    def getInputSplits(inputFile, inputSplitSize):
        '''
        Read and split the inputDataFile given by the user
        '''
        inputData = [line.strip() for line in open(inputFile)]
        inputSplits = []
    
        for index in range(0, len(inputData), inputSplitSize):
            inputSplits.append(inputData[index:index + inputSplitSize])
    
        return inputSplits

    @staticmethod
    def writeResult(result, outputFile, mode):
        '''
        Pickle result to the outputFile in the specified mode
        '''
        fptr = open(outputFile, mode)
        pickle.dump(result, fptr)
        fptr.close()