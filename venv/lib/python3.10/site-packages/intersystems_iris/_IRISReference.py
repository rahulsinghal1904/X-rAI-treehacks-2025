import decimal
import intersystems_iris._IRISList
import intersystems_iris._IRISObject

class _IRISReference(object):
    '''
Used to call method/routine for pass-by-reference arguments

iris.IRISReference(value, type)

Parameters
----------
type : Python type used as a type hint for unmarshaling modified value of the argument. Supported types are  bool, bytes, bytearray, Decimal, float, int, str or IRISList. if none is specified, Python Gateway uses the type that matches the original IRIS type.
'''

    def __init__(self, value, type = None):
        self._value = value
        self._type = type
        self._locale = "latin-1"
        self._is_unicode = True

    def setValue(self, value):
        '''set the value of this IRISReference object.'''
        self._value = value
        return

    def getValue(self):
        '''Get the value of this IRISReference object.'''
        return self.getObject()

    def getObject(self):
        '''Get the value of this IRISReference object.'''
        if self._type == None:
            return self._value
        elif self._type == bool:
            return self.getBoolean()
        elif self._type == bytes:
            return self.getBytes()
        elif self._type == decimal.Decimal:
            return self.getDecimal()
        elif self._type == float:
            return self.getFloat()
        elif self._type == int:
            return self.getInteger()
        elif self._type == str:
            return self.getString()
        elif self._type == intersystems_iris.IRISList:
            return self.getIRISList()
        else:
            return self._value

    def getBoolean(self):
        '''Get the value of this IRISReference object as a bool.'''
        if self._value is None: return None
        list = intersystems_iris.IRISList(None, self._locale, self._is_unicode)
        if isinstance(self._value, intersystems_iris.IRISObject):
            list.add(self._value._oref)
        else:
            list.add(self._value)
        return list.getBoolean(1)
        
    def getBytes(self):
        '''Get the value of this IRISReference object as bytes.'''
        if self._value is None: return None
        list = intersystems_iris.IRISList(None, self._locale, self._is_unicode)
        if isinstance(self._value, intersystems_iris.IRISObject):
            list.add(self._value._oref)
        else:
            list.add(self._value)
        return list.getBytes(1)
        
    def getDecimal(self):
        '''Get the value of this IRISReference object as a Decimal.'''
        if self._value is None: return None
        list = intersystems_iris.IRISList(None, self._locale, self._is_unicode)
        if isinstance(self._value, intersystems_iris.IRISObject):
            list.add(self._value._oref)
        else:
            list.add(self._value)
        return list.getDecimal(1)
        
    def getFloat(self):
        '''Get the value of this IRISReference object as a float.'''
        if self._value is None: return None
        list = intersystems_iris.IRISList(None, self._locale, self._is_unicode)
        if isinstance(self._value, intersystems_iris.IRISObject):
            list.add(self._value._oref)
        else:
            list.add(self._value)
        return list.getFloat(1)
        
    def getInteger(self):
        '''Get the value of this IRISReference object as a int.'''
        if self._value is None: return None
        list = intersystems_iris.IRISList(None, self._locale, self._is_unicode)
        if isinstance(self._value, intersystems_iris.IRISObject):
            list.add(self._value._oref)
        else:
            list.add(self._value)
        return list.getInteger(1)
        
    def getString(self):
        '''Get the value of this IRISReference object as a str.'''
        if self._value is None: return None
        list = intersystems_iris.IRISList(None, self._locale, self._is_unicode)
        if isinstance(self._value, intersystems_iris.IRISObject):
            list.add(self._value._oref)
        else:
            list.add(self._value)
        return list.getString(1)
        
    def getIRISList(self):
        '''Get the value of this IRISReference object as an IRISList.'''
        if self._value is None: return None
        return intersystems_iris.IRISList(self.getBytes(), self._locale, self._is_unicode)
        
    def get_value(self):
        '''This method is deprecated. Use getObject() instead.'''
        return self.getObject()

    def set_value(self, value):
        '''This method is deprecated. Use setValue() instead.'''
        self.setValue(value)
        return

    def get_type(self):
        '''This method is deprecated. Use an appropriate typed getter instead.'''
        return self._type

    def set_type(self, type):
        '''This method is deprecated. Use an appropriate typed getter instead.'''
        self._type = type
        return
