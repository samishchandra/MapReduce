'''
Created on Nov 14, 2012

@author: KSC
'''


class KeyValueType():
    '''
    Class used for the defining the type of input to the MapReduce API
    '''
    keyType = None
    valueType = None

    def __init__(self, keyType, valueType):
        self.keyType = keyType
        self.valueType = valueType
        
class KeyValue():
    '''
    Class for defining the input to MapReduceAPI
    '''

    def __init__(self, kvType):
        self.key = kvType.keyType()
        self.value = kvType.valueType()
