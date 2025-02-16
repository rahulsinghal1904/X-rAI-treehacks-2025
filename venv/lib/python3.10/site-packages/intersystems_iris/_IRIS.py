import decimal
import math
import intersystems_iris._InStream
import intersystems_iris._IRISOREF
import intersystems_iris._PythonGateway
import intersystems_iris._IRISGlobalNode
import intersystems_iris._IRISIterator
import intersystems_iris._IRISList
import intersystems_iris._IRISObject
import intersystems_iris._IRISReference
import intersystems_iris._LegacyIterator

class _IRIS(object):
    '''
A way to execute basic ObjectScript commands on an IRIS server.

This class has methods to work with globals and to call class methods and routines. Any errors on the server generate Runtime Errors.
'''

    # sysio function codes
    GET_NODE          = 1
    SET_NODE          = 2
    KILL_NODE         = 3
    ORDER             = 5
    INCREMENT         = 6
    VALUE_FUNCTION    = 8
    DATA              = 9
    GET_SUBNODES      = 10
    VALUE_CLASSMETHOD = 11
    VOID_CLASSMETHOD  = 12
    VOID_ROUTINE      = 13
    LOCK              = 14
    UNLOCK            = 15
    UNLOCK_ALL        = 16
    TSTART            = 17
    TCOMMIT           = 18
    TROLLBACK         = 19
    TROLLBACK_ONE     = 20
    GET_TLEVEL        = 21
    VOID_PROCEDURE    = 26
    VALUE_METHOD      = 27
    VOID_METHOD       = 28
    GET_PROPERTY      = 29
    SET_PROPERTY      = 30
    # sysio flags
    FLAG_EMPTY        = 0
    FLAG_VALUE        = 1
    FLAG_SUBSCRIPT    = 2
    FLAG_REVERSE      = 4
    FLAG_STOPON       = 8
    # error code
    ERROR_UNDEFINED   = 1009;
    # data conversion mode
    MODE_RUNTIME = 0
    MODE_GLOBAL = 1
    MODE_LIST = 2

    def __init__(self, connection):
        if connection == None or connection.isClosed():
            raise ValueError("cannot create an IRIS object with a closed connection")
        self._connection = connection
        self._is_unicode = connection._connection_info._is_unicode
        self._locale = connection._connection_info._locale
        self._in_message = intersystems_iris._InStream._InStream(connection)
        self._out_message = intersystems_iris._OutStream._OutStream(connection)
        self._in_message_secondary = intersystems_iris._InStream._InStream(connection)
        self._out_message_secondary = intersystems_iris._OutStream._OutStream(connection)

    def close(self):
        pass

    def getAPIVersion(self):
        '''
Returns the version string of the IRIS Native API.

getAPIVersion()

Return Value
------------
Returns the API version string
'''
        return "2.0.0"

    def getServerVersion(self):
        '''
Returns the version string of the IRIS server.

getServerVersion()

Return Value
------------
Returns the server version string
'''
        return self.classMethodString("%SYSTEM.Version","GetVersion")

    def get(self, globalName, *subscripts):
        '''
Fetches the value of a global node.

get(globalName, subscripts...)

Parameters
----------
globalName : global node name.
subscripts... : global subscripts, optional, variable length.

Return Value
------------
Returns None if the node is undefined; otherwise, returns bytes, Decimal, float, int, or str.
'''
        return self._execute(object, intersystems_iris.IRIS.GET_NODE, globalName, None, subscripts, mode = intersystems_iris.IRIS.MODE_GLOBAL)

    def getObject(self, globalName, *subscripts):
        '''
Fetches the value of a global node.

getObject(globalName, subscripts...)

Parameters
----------
globalName : global node name.
subscripts... : global subscripts, optional, variable length.

Return Value
------------
Returns None if the node is undefined; otherwise, returns bytes, Decimal, float, int, or str.
'''
        return self._execute(object, intersystems_iris.IRIS.GET_NODE, globalName, None, subscripts, mode = intersystems_iris.IRIS.MODE_GLOBAL)

    def getBoolean(self, globalName, *subscripts):
        '''
Fetches the value of a global node as a boolean.

getBoolean(globalName, subscripts...)

Parameters
----------
globalName : global node name.
subscripts... : global subscripts, optional, variable length.

Return Value
------------
Returns None if the node is undefined; otherwise, returns bool.
'''
        return self._execute(bool, intersystems_iris.IRIS.GET_NODE, globalName, None, subscripts, mode = intersystems_iris.IRIS.MODE_GLOBAL)

    def getBytes(self, globalName, *subscripts):
        '''
Fetches the value of a global node as bytes.

getBytes(globalName, subscripts...)

Parameters
----------
globalName : global node name.
subscripts... : global subscripts, optional, variable length.

Return Value
------------
Returns None if the node is undefined; otherwise, returns bytes.
'''
        return self._execute(bytes, intersystems_iris.IRIS.GET_NODE, globalName, None, subscripts, mode = intersystems_iris.IRIS.MODE_GLOBAL)

    def getDecimal(self, globalName, *subscripts):
        '''
Fetches the value of a global node as a decimal.

getDecimal(globalName, subscripts...)

Parameters
----------
globalName : global node name.
subscripts... : global subscripts, optional, variable length.

Return Value
------------
Returns None if the node is undefined; otherwise, returns Decimal.
'''
        return self._execute(decimal.Decimal, intersystems_iris.IRIS.GET_NODE, globalName, None, subscripts, mode = intersystems_iris.IRIS.MODE_GLOBAL)

    def getFloat(self, globalName, *subscripts):
        '''
Fetches the value of a global node as a float

getFloat(globalName, subscripts...)

Parameters
----------
globalName : global node name.
subscripts... : global subscripts, optional, variable length.

Return Value
------------
Returns None if the node is undefined; otherwise, returns float.
'''
        return self._execute(float, intersystems_iris.IRIS.GET_NODE, globalName, None, subscripts, mode = intersystems_iris.IRIS.MODE_GLOBAL)

    def getLong(self, globalName, *subscripts):
        '''
This method has been deprecated. Please use getInteger() instead.
'''
        return self.getInteger(globalName, *subscripts)

    def getInteger(self, globalName, *subscripts):
        '''
Fetches the value of a global node as an integer.

getInteger(globalName, subscripts...)

Parameters
----------
globalName : global node name.
subscripts... : global subscripts, optional, variable length.

Return Value
------------
Returns None if the node is undefined; otherwise, returns int.
'''
        return self._execute(int, intersystems_iris.IRIS.GET_NODE, globalName, None, subscripts, mode = intersystems_iris.IRIS.MODE_GLOBAL)

    def getString(self, globalName, *subscripts):
        '''
Fetches the value of a global node as a string.

getString(globalName, subscripts...)

Parameters
----------
globalName : global node name.
subscripts... : global subscripts, optional, variable length.

Return Value
------------
Returns None if the node is undefined; otherwise, returns str.
'''
        return self._execute(str, intersystems_iris.IRIS.GET_NODE, globalName, None, subscripts, mode = intersystems_iris.IRIS.MODE_GLOBAL)

    def getIRISList(self, globalName, *subscripts):
        '''
Fetches the value of a global node as a IRISList.

getIRISList(globalName, subscripts...)

Parameters
----------
globalName : global node name.
subscripts... : global subscripts, optional, variable length.

Return Value
------------
Returns None if the node is undefined; otherwise, returns IRISList.
'''
        return self._execute(intersystems_iris.IRISList, intersystems_iris.IRIS.GET_NODE, globalName, None, subscripts, mode = intersystems_iris.IRIS.MODE_GLOBAL)

    def set(self, value, globalName, *subscripts):
        '''
Sets the value of a global node.

set(value, globalName, subscripts...)

Parameters
----------
value : new value of the global node. The new value may be bool, bytes, bytearray, Decimal, float, int, str, or IRISList.
globalName : global node name.
subscripts... : global subscripts, optional, variable length.

Return Value
------------
Returns None.
'''
        if type(value) == str and len(value) == 0:
            value = None
        self._execute(None, intersystems_iris.IRIS.SET_NODE, globalName, None, subscripts, value, mode = intersystems_iris.IRIS.MODE_GLOBAL)
        return

    def kill(self, globalName, *subscripts):
        '''
Kills a global node including any descendants.

kill(globalName, subscripts...)

Parameters
----------
globalName : global node name.
subscripts... : global subscripts, optional, variable length.

Return Value
------------
Returns None.
'''
        self._execute(None, intersystems_iris.IRIS.KILL_NODE, globalName, None, subscripts, mode = intersystems_iris.IRIS.MODE_GLOBAL)
        return

    def increment(self, value, globalName, *subscripts):
        '''
Increments a global node by the value argument.

increment(value, globalName, subscripts...)

Parameters
----------
value : amount by which to increment.
globalName : global node name.
subscripts... : global subscripts, optional, variable length.

Return Value
------------
Returns the new value of the global node. The new value may be Decimal, float, or int.
'''
        return self._execute(object, intersystems_iris.IRIS.INCREMENT, globalName, None, subscripts, value, mode = intersystems_iris.IRIS.MODE_GLOBAL)

    def isDefined(self, globalName, *subscripts):
        '''
Returns whether a global node contains data and whether it has children. This method is similar to $DATA in IRIS.

isDefined(globalName, subscripts...)

Parameters
----------
globalName : global node name.
subscripts... : global subscripts, optional, variable length.

Return Value
------------
Returns one of the following four integers:
    0 if the node is undefined and has no children
    1 if the node is defined and has no children
    10 if the node is undefined and has children
    11 if the node is defined and has children.
'''
        return self._execute(int, intersystems_iris.IRIS.DATA, globalName, None, subscripts, intersystems_iris.IRIS.FLAG_EMPTY, mode = intersystems_iris.IRIS.MODE_GLOBAL)

    def nextSubscript(self, reversed, globalName, *subscripts):
        '''
Returns the next subscript of a global node. This method is similar to $ORDER in IRIS.

nextSubscript(reversed, globalName, subscripts...)

Parameters
----------
reversed : boolean to indicate if traversing is reversed
globalName : global node name.
subscripts... : global subscripts, optional, variable length.

Return Value
------------
Returns the next subscript that is the sibling of the last subscript. Returns None if it reaches the end.
'''
        bitflags = intersystems_iris.IRIS.FLAG_SUBSCRIPT + (intersystems_iris.IRIS.FLAG_REVERSE if reversed else intersystems_iris.IRIS.FLAG_EMPTY)
        subscript = self._execute(str, intersystems_iris.IRIS.ORDER, globalName, None, subscripts, bitflags, mode = intersystems_iris.IRIS.MODE_GLOBAL)
        if len(subscript) == 0:
            subscript = None
        return subscript

    def _nextNode(self, reversed, globalName, *subscripts):
        bitflags = intersystems_iris.IRIS.FLAG_SUBSCRIPT + intersystems_iris.IRIS.FLAG_VALUE + (intersystems_iris.IRIS.FLAG_REVERSE if reversed else intersystems_iris.IRIS.FLAG_EMPTY)
        return self._execute(tuple, intersystems_iris.IRIS.ORDER, globalName, None, subscripts, bitflags, 5, mode = intersystems_iris.IRIS.MODE_GLOBAL)

    def _nextNodeWithStop(self, reversed, stop_value, globalName, *subscripts):
        bitflags = intersystems_iris.IRIS.FLAG_SUBSCRIPT + intersystems_iris.IRIS.FLAG_VALUE + (intersystems_iris.IRIS.FLAG_REVERSE if reversed else intersystems_iris.IRIS.FLAG_EMPTY) + intersystems_iris.IRIS.FLAG_STOPON
        return self._execute(tuple, intersystems_iris.IRIS.ORDER, globalName, None, subscripts, bitflags, stop_value, mode = intersystems_iris.IRIS.MODE_GLOBAL)

    def iterator(self, globalName, *subscripts):
        '''
This method is deprecated, please use node() instead.

Returns an iterator which can iterate over the immediate children of a global node.

The iterator can be set to move forwards or backwards, and to return the subscript, the value or both in the iteration. This is similar to using the $ORDER function in ObjectScript.

iterator(globalName, subscripts...)

Parameters
----------
globalName : global node name.
subscripts... : global subscripts, optional, variable length.

Return Value
------------
Returns an iterator
'''
        return intersystems_iris.LegacyIterator(self, globalName, *subscripts)

    def node(self, globalName, *subscripts):
        '''
Returns an IRISGlobalNode object which is an iterable object behaves like a virtual dictionary representing the immediate children of a global node.

IRISGLobalNode is iterable, reversable, indexable and sliceable. Please refer to IRISGLobalNode for more details.

node(globalName, subscripts...)

Parameters
----------
globalName : global node name.
subscripts... : global subscripts, optional, variable length.

Return Value
------------
Returns an IRISGLobalNode object.
'''
        return intersystems_iris.IRISGlobalNode(self, globalName, *subscripts)

    def _sortOrder(self, subscript1, subscript2, globalName, *subscripts):
        return self._execute(int, intersystems_iris.IRIS.VALUE_CLASSMETHOD, "%Net.Remote.Gateway", "%SubscriptSortOrder", [globalName, subscript1, subscript2])

    def lock(self, lockMode, timeout, globalName, *subscripts):
        '''
Locks a global node. It performs an incremental lock and not the implicit unlock before lock feature that is also offered in IRIS. Throws a <TIMEOUT> exception if the timeout is reached waiting to acquire the lock.

lock(lockMode, timeout, globalName, subscripts...)

Parameters
----------
lockMode : a string containing zero or more of the following characters
           S for shared lock, E for escalating lock.
           An empty string is the default mode (exclusive and non-escalating).
timeout : number of seconds to wait to acquire the lock
globalName : global node name.
subscripts... : global subscripts, optional, variable length.

Return Value
------------
Returns None.
'''
        self._execute(None, intersystems_iris.IRIS.LOCK, globalName, None, subscripts, lockMode, timeout, mode = intersystems_iris.IRIS.MODE_GLOBAL)
        return True

    def unlock(self, lockMode, globalName, *subscripts):
        '''
Unlock a global node. It perform an incremental unlock and not the implicit unlock before lock feature that is also offered in IRIS.

unlock(lockMode, globalName, subscripts...)

Parameters
----------
lockMode : a string containing zero or more of the following characters:
           S for shared lock, E for escalating lock, I for immediate unlock, D for deferred unlock.
           An empty string is the default mode (exclusive, non-escalating, always defers releasing an unlocked lock to the end of the current transaction).
globalName : global node name.
subscripts... : global subscripts, optional, variable length.

Return Value
------------
Returns None.
'''
        self._execute(None, intersystems_iris.IRIS.UNLOCK, globalName, None, subscripts, lockMode, mode = intersystems_iris.IRIS.MODE_GLOBAL)
        return

    def releaseAllLocks(self):
        '''
Releases all locks associated with the session (i.e. connection).

releaseAllLocks()

Return Value
------------
Returns None.
'''
        self._execute(None, intersystems_iris.IRIS.UNLOCK_ALL, None, None, None, mode = intersystems_iris.IRIS.MODE_GLOBAL)
        return

    def tStart(self):
        '''
Starts an IRIS transaction.

tStart()

Return Value
------------
Returns None.
'''
        self._execute(None, intersystems_iris.IRIS.TSTART, None, None, None, mode = intersystems_iris.IRIS.MODE_GLOBAL)
        return

    def tCommit(self):
        '''
Commits the current IRIS transaction.

tCommit()

Return Value
------------
Returns None.
'''
        self._execute(None, intersystems_iris.IRIS.TCOMMIT, None, None, None, mode = intersystems_iris.IRIS.MODE_GLOBAL)
        return

    def tRollback(self):
        '''
Rolls back all open IRIS transactions in the session (i.e. connection).

tRollback()

Return Value
------------
Returns None.
'''
        self._execute(None, intersystems_iris.IRIS.TROLLBACK, None, None, None, mode = intersystems_iris.IRIS.MODE_GLOBAL)
        return

    def tRollbackOne(self):
        '''
Rolls back the current level of IRIS transaction. This method is intended for nested transactions, when the caller only wants to roll back one level.

tRollbackOne()

Return Value
------------
Returns None.
'''
        self._execute(None, intersystems_iris.IRIS.TROLLBACK_ONE, None, None, None, mode = intersystems_iris.IRIS.MODE_GLOBAL)
        return

    def getTLevel(self):
        '''
Returns the number of nested transactions currently open in the session (i.e. connection). This is equivalent to fetching the value of the $TLEVEL special variable in IRIS.

getTLevel()

Return Value
------------
Returns an integer indicates the the number of open transactions in the current session (i.e. connection). Returns 0 if there are no transactions open.
'''
        return self._execute(int, intersystems_iris.IRIS.GET_TLEVEL, None, None, None, mode = intersystems_iris.IRIS.MODE_GLOBAL)

    def function(self, functionName, routineName, *args):
        '''
Calls a function that returns a value.

function(functionName, routineName, args...)

Parameters
----------
functionName : name of a function.
routineName : name of an IRIS routine.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns bytes, Decimal, float, int, or str.
'''
        return self._execute(object, intersystems_iris.IRIS.VALUE_FUNCTION, functionName, routineName, args)

    def functionObject(self, functionName, routineName, *args):
        '''
Calls a function that returns a value.

functionObject(functionName, routineName, args...)

Parameters
----------
functionName : name of a function.
routineName : name of an IRIS routine.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns bytes, Decimal, float, int, or str.
'''
        return self._execute(object, intersystems_iris.IRIS.VALUE_FUNCTION, functionName, routineName, args)

    def functionBoolean(self, functionName, routineName, *args):
        '''
Calls a function that returns a value as a boolean.

functionBoolean(functionName, routineName, args...)

Parameters
----------
functionName : name of a function.
routineName : name of an IRIS routine.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns a bool.
'''
        return self._execute(bool, intersystems_iris.IRIS.VALUE_FUNCTION, functionName, routineName, args)

    def functionBytes(self, functionName, routineName, *args):
        '''
Calls a function that returns a value as bytes.

functionBytes(functionName, routineName, args...)

Parameters
----------
functionName : name of a function.
routineName : name of an IRIS routine.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns bytes.
'''
        return self._execute(bytes, intersystems_iris.IRIS.VALUE_FUNCTION, functionName, routineName, args)

    def functionDecimal(self, functionName, routineName, *args):
        '''
Calls a function that returns a value as a Decimal.

functionDecimal(functionName, routineName, args...)

Parameters
----------
functionName : name of a function.
routineName : name of an IRIS routine.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns a Decimal.
'''
        return self._execute(decimal.Decimal, intersystems_iris.IRIS.VALUE_FUNCTION, functionName, routineName, args)

    def functionFloat(self, functionName, routineName, *args):
        '''
Calls a function that returns a value as a float.

functionFloat(functionName, routineName, args...)

Parameters
----------
functionName : name of a function.
routineName : name of an IRIS routine.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns a float.
'''
        return self._execute(float, intersystems_iris.IRIS.VALUE_FUNCTION, functionName, routineName, args)

    def functionInteger(self, functionName, routineName, *args):
        '''
Calls a function that returns a value as an integer.

functionInteger(functionName, routineName, args...)

Parameters
----------
functionName : name of a function.
routineName : name of an IRIS routine.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns an int.
'''
        return self._execute(int, intersystems_iris.IRIS.VALUE_FUNCTION, functionName, routineName, args)

    def functionString(self, functionName, routineName, *args):
        '''
Calls a function that returns a value as a string.

functionString(functionName, routineName, args...)

Parameters
----------
functionName : name of a function.
routineName : name of an IRIS routine.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns a str.
'''
        return self._execute(str, intersystems_iris.IRIS.VALUE_FUNCTION, functionName, routineName, args)

    def functionIRISList(self, functionName, routineName, *args):
        '''
Calls a function that returns a value as an IRISList.

functionIRISList(functionName, routineName, args...)

Parameters
----------
functionName : name of a function.
routineName : name of an IRIS routine.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns an IRISList.
'''
        return self._execute(intersystems_iris.IRISList, intersystems_iris.IRIS.VALUE_FUNCTION, functionName, routineName, args)

    def procedure(self, procedureName, routineName, *args):
        '''
Calls a procedure that does not return a value.

procedure(procedureName, routineName, args...)

Parameters
----------
procedureName : name of a procedure.
routineName : name of an IRIS routine.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None.
'''
        if len(args) > 0:
            self._execute(None, intersystems_iris.IRIS.VOID_PROCEDURE, procedureName, routineName, args)
        else:
            self._execute(None, intersystems_iris.IRIS.VOID_ROUTINE, procedureName, routineName, args)
        return

    def classMethodValue(self, className, methodName, *args):
        '''
Invoke a classmethod that returns a value.

classMethodValue(className, methodName, args...)

Parameters
----------
className : name of a class.
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns bytes, Decimal, float, int, or str.
'''
        return self._execute(object, intersystems_iris.IRIS.VALUE_CLASSMETHOD, className, methodName, args)

    def classMethodObject(self, className, methodName, *args):
        '''
Invoke a classmethod that returns a value.

classMethodObject(className, methodName, args...)

Parameters
----------
className : name of a class.
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns bytes, Decimal, float, int, or str.
'''
        return self._execute(object, intersystems_iris.IRIS.VALUE_CLASSMETHOD, className, methodName, args)

    def classMethodBoolean(self, className, methodName, *args):
        '''
Invoke a classmethod that returns a value as a boolean.

classMethodBoolean(className, methodName, args...)

Parameters
----------
className : name of a class.
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns a bool.
'''
        return self._execute(bool, intersystems_iris.IRIS.VALUE_CLASSMETHOD, className, methodName, args)

    def classMethodBytes(self, className, methodName, *args):
        '''
Invoke a classmethod that returns a value as bytes.

classMethodBytes(className, methodName, args...)

Parameters
----------
className : name of a class.
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns bytes.
'''
        return self._execute(bytes, intersystems_iris.IRIS.VALUE_CLASSMETHOD, className, methodName, args)

    def classMethodDecimal(self, className, methodName, *args):
        '''
Invoke a classmethod that returns a value as a Decimal.

classMethodDecimal(className, methodName, args...)

Parameters
----------
className : name of a class.
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns a Decimal.
'''
        return self._execute(decimal.Decimal, intersystems_iris.IRIS.VALUE_CLASSMETHOD, className, methodName, args)

    def classMethodFloat(self, className, methodName, *args):
        '''
Invoke a classmethod that returns a value as a float.

classMethodFloat(className, methodName, args...)

Parameters
----------
className : name of a class.
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns a float.
'''
        return self._execute(float, intersystems_iris.IRIS.VALUE_CLASSMETHOD, className, methodName, args)

    def classMethodInteger(self, className, methodName, *args):
        '''
Invoke a classmethod that returns a value as a integer.

classMethodInteger(className, methodName, args...)

Parameters
----------
className : name of a class.
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns a int.
'''
        return self._execute(int, intersystems_iris.IRIS.VALUE_CLASSMETHOD, className, methodName, args)

    def classMethodString(self, className, methodName, *args):
        '''
Invoke a classmethod that returns a value as a string.

classMethodString(className, methodName, args...)

Parameters
----------
className : name of a class.
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns a string.
'''
        return self._execute(str, intersystems_iris.IRIS.VALUE_CLASSMETHOD, className, methodName, args)

    def classMethodIRISList(self, className, methodName, *args):
        '''
Invoke a classmethod that returns a value as an IRISList.

classMethodIRISList(className, methodName, args...)

Parameters
----------
className : name of a class.
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns an IRISList.
'''
        return self._execute(intersystems_iris.IRISList, intersystems_iris.IRIS.VALUE_CLASSMETHOD, className, methodName, args)

    def classMethodVoid(self, className, methodName, *args):
        '''
Invoke a classmethod that does not return a value.

classMethodVoid(className, methodName, args...)

Parameters
----------
className : name of a class.
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None.
'''
        self._execute(None, intersystems_iris.IRIS.VOID_CLASSMETHOD, className, methodName, args)
        return

    def _execute(self, return_type, function_code, name_or_object, secondary_name, args, *post_values, allowedErrors = None, mode = MODE_RUNTIME, honorByReference = True):
        if mode == intersystems_iris.IRIS.MODE_GLOBAL:
            honorByReference = False
        self._out_message.wire._write_header_sysio(function_code)
        if name_or_object != None:
            if isinstance(name_or_object, intersystems_iris.IRISObject):
                self._out_message.wire._set(intersystems_iris._IRISOREF._IRISOREF(name_or_object._oref))
            else:
                self._out_message.wire._set(name_or_object, True)
        if secondary_name != None:
            self._out_message.wire._set(secondary_name)
        if args != None:
            self._marshal_parameters(*args, mode = mode, honorByReference = honorByReference)
        for i in range(len(post_values)):
            self._marshal_one_parameter(post_values[i], mode = mode, honorByReference = honorByReference)
        allowedErrors = [intersystems_iris.IRIS.ERROR_UNDEFINED] if mode == intersystems_iris.IRIS.MODE_GLOBAL else None
        with self._connection._lock:
            sequence_number = self._connection._get_new_sequence_number()
            self._out_message._send(sequence_number)
            code = self._in_message._read_message_sysio(sequence_number, allowedErrors)
            if code != 0:
                return None
            if return_type == None:
                returned_value = None
            elif return_type == tuple:
                returned_value = (self._unmarshal_return_value(str, mode, self._locale, self._is_unicode, self._connection.compact_double), self._unmarshal_return_value(object, mode, self._locale, self._is_unicode, self._connection.compact_double))
            else:
                returned_value = self._unmarshal_return_value(return_type, mode, self._locale, self._is_unicode, self._connection.compact_double)
        if honorByReference and args != None:
            self._process_pass_by_reference(*args)
        self._release_closed_iris_object()
        return returned_value

    def _marshal_parameters(self, *args, mode, honorByReference):
        self._out_message.wire._set(len(args))
        for arg in args:
            self._marshal_one_parameter(arg, mode = mode, honorByReference = honorByReference)
        return

    def _marshal_one_parameter(self, arg, *, mode, honorByReference):
        is_reference = honorByReference and type(arg) == intersystems_iris.IRISReference
        if is_reference:
            arg = arg.get_value()
            self._out_message.wire._save_current_offset()
        if intersystems_iris._PythonGateway._PythonGateway._is_datatype(type(arg)):
            self._out_message.wire._set(arg, True)
        elif mode == intersystems_iris.IRIS.MODE_GLOBAL and type(arg) == bytearray:
            self._out_message.wire._set(arg)
        elif type(arg) == intersystems_iris.IRISList:
            if not self._connection.compact_double and arg.compact_double:
                raise ValueError("Cannot store an IRISList with Compact Double enabled on a server with Compact Double disabled")
            self._out_message.wire._set(arg.getBuffer())
        else:
            if mode == intersystems_iris.IRIS.MODE_GLOBAL:
                raise TypeError("Unsupported type as global subscript: " + type(arg).__name__)
            oref = self._connection._oref_registry_lookup(arg)
            if oref is None:
                oref = self._connection._map_local_object_to_oref(self._in_message_secondary, self._out_message_secondary, arg)
            if oref != None:
                self._out_message.wire._set(intersystems_iris._IRISOREF._IRISOREF(oref))
            else:
                raise  _GatewayException._GatewayException("Unable to map object: " + arg)
        if is_reference:
            self._out_message.wire._set_saved_offset_type_as_pass_by_reference()
        return

    def _unmarshal_return_value(self, return_type, mode, locale, is_unicode, compact_double):
        asBytes = return_type != str and return_type != object
        value = self._in_message.wire._get(asBytes, True)
        if self._in_message.wire.list_item.is_undefined:
            return None
        if type(value) == intersystems_iris._IRISOREF._IRISOREF:
            if return_type == object:
                return self._connection._map_local_object_from_oref(value._oref)
            else:
                self._connection._close_unused_oref(value._oref)
                value = value._oref
        if return_type == bool:
            return intersystems_iris.IRIS._convertToBoolean(value, mode, locale)
        elif return_type == bytes:
            return intersystems_iris.IRIS._convertToBytes(value, mode, locale, is_unicode)
        elif return_type == bytearray:
            return bytearray(intersystems_iris.IRIS._convertToBytes(value, mode, locale, is_unicode))
        elif return_type == decimal.Decimal:
            return intersystems_iris.IRIS._convertToDecimal(value, mode, locale)
        elif return_type == float:
            return intersystems_iris.IRIS._convertToFloat(value, mode, locale)
        elif return_type == int:
            return intersystems_iris.IRIS._convertToInteger(value, mode, locale)
        elif return_type == str:
            return intersystems_iris.IRIS._convertToString(value, mode, locale)
        elif return_type == object:
            return intersystems_iris.IRIS._convertToObject(value, mode, locale)
        elif return_type == intersystems_iris.IRISList:
            if value == None:
                return intersystems_iris.IRISList(None, locale, is_unicode, compact_double) if mode == intersystems_iris.IRIS.MODE_GLOBAL else None
            return intersystems_iris.IRISList(intersystems_iris.IRIS._convertToBytes(value, mode, locale, is_unicode), locale, is_unicode, compact_double)
        else:
            return intersystems_iris.IRIS._convertToObject(value, mode, locale)

    def _process_pass_by_reference(self, *args):
        for arg in args:
            if type(arg) == intersystems_iris.IRISReference:
                value = self._unmarshal_return_value(object, intersystems_iris.IRIS.MODE_RUNTIME, self._locale, self._is_unicode, self._connection.compact_double)
                arg._value = value
                arg._locale = self._locale
                arg._is_unicode = self._is_unicode
        return

    @staticmethod
    def __parse_iris_number(value, return_type):
        if len(value)==0:
            return return_type(0)
        if type(value) == bytes:
            value = value.decode("latin-1")
        if value[0:3].lower() == "inf":
            return 0 if return_type == int else return_type("inf")
        if value[0:4].lower() == "-inf":
            return 0 if return_type == int else return_type("-inf")
        if value[0:3].lower() == "nan" or value[0:4].lower() == "-nan" or value[0:4].lower() == "snan" or value[0:5].lower() == "-snan":
            return 0 if return_type == int else return_type("nan")
        beginning = True
        acceptDot = True
        negative = False
        base = ""
        i = 0
        for i in range(len(value)):
            if value[i] == "+" and beginning:
                continue
            if value[i] == "-" and beginning:
                negative = not negative
                continue
            if value[i] == "." and acceptDot:
                base = base + "."
                beginning = False
                acceptDot = False
                continue
            if ord(value[i]) >= 48 and ord(value[i]) <= 57:
                base = base + value[i]
                beginning = False
                continue
            break
        power = ""
        if value[i] == "E" or value[i] == "e":
            i = i+1
            if value[i] == "+" or value[i] == "-":
                power = value[i]
                i = i+1
            for j in range(i, len(value), 1):
                if 48 <= ord(value[j]) <= 57 :
                    power = power + value[j]
                    continue
                break
        if power == "" or power == "+" or power == "-":
            result = ("-" if negative else "") + base
        else:
            result = ("-" if negative else "") + base + "E" + power
        if len(result)==0:
            return return_type(0)
        if return_type == float:
            return float(result)
        result = intersystems_iris.IRIS._remove_scientific_notation(result)
        if return_type == int:
            return int(result.split(".")[0])
        else:
            return decimal.Decimal(result)

    @staticmethod
    def _convertToBoolean(value, mode, locale):
        if value == None:
            return False if mode == intersystems_iris.IRIS.MODE_GLOBAL else None
        if type(value) == bool:
            return value
        return intersystems_iris.IRIS._convertToFloat(value, mode, locale) != 0

    @staticmethod
    def _convertToBytes(value, mode, locale, is_unicode):
        if value == None:
            return bytes() if mode == intersystems_iris.IRIS.MODE_GLOBAL else None
        if type(value) == bool:
            return b'1' if value else b'0'
        if type(value) == bytes:
            return value
        if type(value) == decimal.Decimal:
            return bytes(intersystems_iris.IRIS._convert_decimal_to_str(value),"latin_1")
        if type(value) == float:
            return bytes(intersystems_iris.IRIS._convert_float_to_str(value),"latin_1")
        if type(value) == int:
            return bytes(str(value),"latin_1")
        if type(value) == str:
            try:
                return value.encode(locale)
            except UnicodeEncodeError as e:
                if is_unicode:
                    return value.encode("utf-16LE")
                else:
                    raise e
        raise RuntimeError("the value of this node cannot be interpreted as a bytes.")

    @staticmethod
    def _convertToDecimal(value, mode, locale):
        if value == None:
            return decimal.Decimal(0) if mode == intersystems_iris.IRIS.MODE_GLOBAL else None
        if type(value) == bytes:
            return intersystems_iris.IRIS.__parse_iris_number(value, decimal.Decimal)
        if type(value) == bool:
            return decimal.Decimal(1) if value else decimal.Decimal(0)
        if type(value) == decimal.Decimal:
            return value
        if type(value) == float:
            return decimal.Decimal(value)
        if type(value) == int:
            return decimal.Decimal(value)
        if type(value) == str:
            return intersystems_iris.IRIS.__parse_iris_number(value, decimal.Decimal)
        raise RuntimeError("the value of this node cannot be interpreted as a decimal.Decimal.")

    @staticmethod
    def _convertToFloat(value, mode, locale):
        if value == None:
            return 0.0 if mode == intersystems_iris.IRIS.MODE_GLOBAL else None
        if type(value) == bool:
            return 1.0 if value else 0.0
        if type(value) == bytes:
            return intersystems_iris.IRIS.__parse_iris_number(value, float)
        if type(value) == decimal.Decimal:
            return float(value)
        if type(value) == float:
            return value
        if type(value) == int:
            return float(value)
        if type(value) == str:
            return intersystems_iris.IRIS.__parse_iris_number(value, float)
        raise RuntimeError("the value of this node cannot be interpreted as a float.")

    @staticmethod
    def _convertToInteger(value, mode, locale):
        if value == None:
            return 0 if mode == intersystems_iris.IRIS.MODE_GLOBAL else None
        if type(value) == bool:
            return 1 if value else 0
        if type(value) == bytes:
            return intersystems_iris.IRIS.__parse_iris_number(value, int)
        if type(value) == decimal.Decimal:
            return int(value)
        if type(value) == float:
            return int(value)
        if type(value) == int:
            return value
        if type(value) == str:
            return intersystems_iris.IRIS.__parse_iris_number(value, int)
        raise RuntimeError("the value of this node cannot be interpreted as a int.")

    @staticmethod
    def _convertToString(value, mode, locale):
        if value == None:
            return "" if mode == intersystems_iris.IRIS.MODE_GLOBAL else None
        if type(value) == bool:
            return "1" if value else "0"
        if type(value) == bytes:
            return value.decode(locale)
        if type(value) == decimal.Decimal:
            return intersystems_iris.IRIS._convert_decimal_to_str(value)
        if type(value) == float:
            return intersystems_iris.IRIS._convert_float_to_str(value)
        if type(value) == int:
            return str(value)
        if type(value) == str:
            return value
        raise RuntimeError("the value of this node cannot be interpreted as a str.")

    @staticmethod
    def _convertToObject(value, mode, locale):
        if value == None:
            return "" if mode == intersystems_iris.IRIS.MODE_GLOBAL else None
        return value

    @staticmethod
    def _convert_float_to_str(value):
        if math.isinf(value) and value > 0:
            return "INF"
        if math.isinf(value) and value < 0:
            return "-INF"
        if math.isnan(value):
            return "NAN"
        float_context = decimal.Context(prec=20, rounding=decimal.ROUND_05UP, Emin=-308, Emax=308, capitals=1)
        value = str(float_context.create_decimal(value))
        return intersystems_iris.IRIS._remove_scientific_notation(value)

    @staticmethod
    def _convert_decimal_to_str(value):
        return intersystems_iris.IRIS._remove_scientific_notation(str(value))

    @staticmethod
    def _remove_scientific_notation(value):
        if value.startswith("-"):
            negative_sign = "-"
            value = value[1:]
        else:
            negative_sign = ""
        if "E" in value:
            number = value.split("E")[0]
            exponent = int(value.split("E")[1])
        else:
            number = value
            exponent = 0
        if exponent>0:
            if "." in number:
                index = number.index(".")
                if exponent - len(number) + index + 1 >0:
                    number = number + "0" * (exponent - len(number) + index + 1)
                number = number[0:index] + number[index+1:index+exponent+1] + "." + number[index+exponent+1:]
                if number.endswith("."):
                    number = number[:-1]
            else:
                number = number + ("0" * exponent)
        elif exponent<0:
            exponent = abs(exponent)
            if "." not in value:
                number = number + "."
            index = number.index(".")
            if index < exponent:
                number = ("0" * (exponent - index)) + number
                index = exponent
            number = number[0:index-exponent]+"."+number[index-exponent:index]+number[index+1:]
        if "." in number:
            while number.endswith("0"): number = number[:-1]
            if number.endswith("."): number = number[:-1]
        while number.startswith("0"):
            number = number[1:]
        if number == "" or number == ".":
            return "0"
        return negative_sign + number

    def _release_closed_iris_object(self, force = False):
        if not force and len(self._connection._iris_object_proxy_closed) <= intersystems_iris.IRISConnection.CLOSED_PROXY_UPDATE_THRESHOLD:
            return
        closed_iris_objects = self._connection._get_closed_iris_objects()
        closed_orefs = self._execute(str, intersystems_iris.IRIS.VALUE_CLASSMETHOD, "%Net.Remote.Gateway", "%ReleaseObjects", [closed_iris_objects])
        if closed_orefs != None:
            closed_orefs_list = closed_orefs.split(",")
            for i in range(len(closed_orefs_list)):
                del self._connection._oref_registry[closed_orefs_list[i]]
        return
