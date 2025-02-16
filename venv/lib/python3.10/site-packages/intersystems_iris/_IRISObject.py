import decimal
import weakref
import intersystems_iris._IRIS

class _IRISObject(object):
    '''
Class for Python proxy objects created on behalf of IRIS objects.
'''

    def __init__(self, connection, oref):
        self._connection = connection
        self._iris = intersystems_iris.IRIS(connection)
        self._oref = oref
        self._closed = False
        self._connection._iris_object_proxy_map[oref] = weakref.ref(self)

    def getOREF(self):
        '''
Returns the IRIS object OREF as a string.

getOREF()

Return Value
------------
Returns str.
        '''
        return self._oref

    def get(self, propertyName):
        '''
Fetches the value of a property of the IRISObject object.

get(propertyName)

Parameters
----------
propertyName : name of a property

Return Value
------------
Returns None for IRIS empty string ($$$NULLOREF); otherwise, returns bytes, Decimal, float, int, or str.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        return self._iris._execute(object, intersystems_iris.IRIS.GET_PROPERTY, self, propertyName, None)

    def getObject(self, propertyName):
        '''
Fetches the value of a property of the IRISObject object.

getObject(propertyName)

Parameters
----------
propertyName : name of a property

Return Value
------------
Returns None for IRIS empty string ($$$NULLOREF); otherwise, returns bytes, Decimal, float, int, or str.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        return self._iris._execute(object, intersystems_iris.IRIS.GET_PROPERTY, self, propertyName, None)

    def getBoolean(self, propertyName):
        '''
Fetches the value of a property of the IRISObject object as a boolean.

getBoolean(propertyName)

Parameters
----------
propertyName : name of a property

Return Value
------------
Returns None for IRIS empty string ($$$NULLOREF); otherwise, returns a bool.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        return self._iris._execute(bool, intersystems_iris.IRIS.GET_PROPERTY, self, propertyName, None)

    def getBytes(self, propertyName):
        '''
Fetches the value of a property of the IRISObject object as bytes.

getBytes(propertyName)

Parameters
----------
propertyName : name of a property

Return Value
------------
Returns None for IRIS empty string ($$$NULLOREF); otherwise, returns bytes.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        return self._iris._execute(bytes, intersystems_iris.IRIS.GET_PROPERTY, self, propertyName, None)

    def getDecimal(self, propertyName):
        '''
Fetches the value of a property of the IRISObject object as a Decimal.

getDecimal(propertyName)

Parameters
----------
propertyName : name of a property

Return Value
------------
Returns None for IRIS empty string ($$$NULLOREF); otherwise, returns a Decimal.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        return self._iris._execute(decimal.Decimal, intersystems_iris.IRIS.GET_PROPERTY, self, propertyName, None)

    def getFloat(self, propertyName):
        '''
Fetches the value of a property of the IRISObject object as a float.

getFloat(propertyName)

Parameters
----------
propertyName : name of a property

Return Value
------------
Returns None for IRIS empty string ($$$NULLOREF); otherwise, returns a float.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        return self._iris._execute(float, intersystems_iris.IRIS.GET_PROPERTY, self, propertyName, None)

    def getInteger(self, propertyName):
        '''
Fetches the value of a property of the IRISObject object as an integer.

getInteger(propertyName)

Parameters
----------
propertyName : name of a property

Return Value
------------
Returns None for IRIS empty string ($$$NULLOREF); otherwise, returns an int.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        return self._iris._execute(int, intersystems_iris.IRIS.GET_PROPERTY, self, propertyName, None)

    def getString(self, propertyName):
        '''
Fetches the value of a property of the IRISObject object as a string.

getString(propertyName)

Parameters
----------
propertyName : name of a property

Return Value
------------
Returns None for IRIS empty string ($$$NULLOREF); otherwise, returns a str.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        return self._iris._execute(str, intersystems_iris.IRIS.GET_PROPERTY, self, propertyName, None)

    def getIRISList(self, propertyName):
        '''
Fetches the value of a property of the IRISObject object as an IRISList.

getIRISList(propertyName)

Parameters
----------
propertyName : name of a property

Return Value
------------
Returns None for IRIS empty string ($$$NULLOREF); otherwise, returns an IRISList.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        return self._iris._execute(intersystems_iris.IRISList, intersystems_iris.IRIS.GET_PROPERTY, self, propertyName, None)

    def set(self, propertyName, propertyValue):
        '''
Set the value of a property of the IRISObject object.

set(propertyName, propertyValue)

Parameters
----------
propertyName : name of a property
propertyValue : new value of the property

Return Value
------------
Returns None.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        self._iris._execute(None, intersystems_iris.IRIS.SET_PROPERTY, self, propertyName, None, propertyValue, honorByReference = False)
        return

    def invoke(self, methodName, *args):
        '''
Invoke a method of the IRISObject object that returns a value.

invoke(methodName, args...)

Parameters
----------
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns bytes, Decimal, float, int, or str.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        return self._iris._execute(object, intersystems_iris.IRIS.VALUE_METHOD, self, methodName, args)

    def invokeObject(self, methodName, *args):
        '''
Invoke a method of the IRISObject object that returns a value.

invokeObject(methodName, args...)

Parameters
----------
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns bytes, Decimal, float, int, or str.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        return self._iris._execute(object, intersystems_iris.IRIS.VALUE_METHOD, self, methodName, args)

    def invokeBoolean(self, methodName, *args):
        '''
Invoke a method of the IRISObject object that returns a value as a boolean.

invokeBoolean(methodName, args...)

Parameters
----------
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns a bool.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        return self._iris._execute(bool, intersystems_iris.IRIS.VALUE_METHOD, self, methodName, args)

    def invokeBytes(self, methodName, *args):
        '''
Invoke a method of the IRISObject object that returns a value as bytes.

invokeBytes(methodName, args...)

Parameters
----------
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns bytes.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        return self._iris._execute(bytes, intersystems_iris.IRIS.VALUE_METHOD, self, methodName, args)

    def invokeDecimal(self, methodName, *args):
        '''
Invoke a method of the IRISObject object that returns a value as a Decimal.

invokeDecimal(methodName, args...)

Parameters
----------
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns a Decimal.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        return self._iris._execute(decimal.Decimal, intersystems_iris.IRIS.VALUE_METHOD, self, methodName, args)

    def invokeFloat(self, methodName, *args):
        '''
Invoke a method of the IRISObject object that returns a value as a float.

invokeFloat(methodName, args...)

Parameters
----------
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns a float.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        return self._iris._execute(float, intersystems_iris.IRIS.VALUE_METHOD, self, methodName, args)

    def invokeInteger(self, methodName, *args):
        '''
Invoke a method of the IRISObject object that returns a value as an integer.

invokeInteger(methodName, args...)

Parameters
----------
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns a int.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        return self._iris._execute(int, intersystems_iris.IRIS.VALUE_METHOD, self, methodName, args)

    def invokeString(self, methodName, *args):
        '''
Invoke a method of the IRISObject object that returns a value as a string.

invokeString(methodName, args...)

Parameters
----------
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns a str.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        return self._iris._execute(str, intersystems_iris.IRIS.VALUE_METHOD, self, methodName, args)

    def invokeIRISList(self, methodName, *args):
        '''
Invoke a method of the IRISObject object that returns a value as an IRISList.

invokeIRISList(methodName, args...)

Parameters
----------
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None if IRIS empty string ($$$NULLOREF) is returned; otherwise, returns an IRISList.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        return self._iris._execute(intersystems_iris.IRISList, intersystems_iris.IRIS.VALUE_METHOD, self, methodName, args)

    def invokeVoid(self, methodName, *args):
        '''
Invoke a method of the IRISObject object that does not return a value.

invokeVoid(methodName, args...)

Parameters
----------
methodName : name of a method.
args... : zero or more arguments to be passed to the function, optional, variable length.
          None is projected as empty string ($$$NULLOREF) in IRIS.
          bool, bytes, Decimal, float, int, str, and IRISList are projected as literals in IRIS.
          all other types are projected as proxy objects.

Return Value
------------
Returns None.
        '''
        if ( self._closed ): raise RuntimeError("IRISObject is closed")
        self._iris._execute(None, intersystems_iris.IRIS.VOID_METHOD, self, methodName, args)
        return

    def getConnection():
        return self._connection

    def close(self):
        if not self._closed:
            with self._connection._lock_closed_oref:
                del self._connection._iris_object_proxy_map[self._oref]
                self._connection._iris_object_proxy_closed.append(self._oref)
                self._closed = True

    def __del__(self):
        if not self._closed:
            self.close()
