import collections.abc
import intersystems_iris._IRISIterator

class _LegacyIterator(collections.abc.Iterator, collections.abc.Iterable):
    '''
This class has been deprecated. Please call IRIS.node() in order to use IRISGlobalNode instead.

LegacyIterator is an iterator over the immediate children of a global node.

The iterator can be set to move forwards or backwards, and to return the subscript and value, just the subscript, or just the value for each node in the iteration. This is similar to using the $ORDER function in ObjectScript.
'''

    def __init__(self, irisnative, global_name, *subscripts):
        self._irisnative = irisnative
        self._global_name = global_name
        self._subscripts = subscripts
        self._start_from = None
        self._reversed = False
        self._view_type = intersystems_iris.IRISIterator.VIEW_ITEMS
        self._at_end = False

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        if self._at_end:
            raise StopIteration
        key_value = self._irisnative._nextNode(self._reversed, self._global_name, *self._subscripts, self._start_from)
        # check if we are at end
        returned_value = key_value[0]
        if returned_value == None or len(returned_value) == 0:
            self._at_end = True
            raise StopIteration
        self._start_from = returned_value
        # return value
        if self._view_type == intersystems_iris.IRISIterator.VIEW_SUBSCRIPTS:
            return key_value[0]
        elif self._view_type == intersystems_iris.IRISIterator.VIEW_VALUES:
            return key_value[1]
        elif self._view_type == intersystems_iris.IRISIterator.VIEW_ITEMS:
            return key_value
        else:
            raise TypeError("Unrecognized view type")

    def startFrom(self, subscript):
        '''
Set the starting position and return the iterator.

startFrom(subscript)

Set the starting position to the specified subscript. After calling this
method, use next() to advance the iterator to the next defined sub-node
in alphabetic collating sequence. For example, for the following global
and iterator:
    ^gbl("a")=11
    ^gbl("b")=22
    ^gbl("e")=55
    itr=iris.iterator("^gbl")
The starting position may be a valid sub-node, in which case the next sub-node will be the next valid one.
    itr.startFrom("a")
    for sub, val in itr:
        print(sub,"->",val)
        // prints: b -> 22; e -> 55
The starting position may also be an invalid sub-node, in which case the next sub-node will be the first valid one in alphabetic collating sequence after the given subscript.
    itr.startFrom("c")
    for sub, val in itr:
        print(sub,"->",val)
        // prints: e -> 55
Calling this method with None as the argument is the same as using the default starting position, which is just before the first node, or just after the last node, depending on the iterator direction.

Parameters
----------
subscript : a single subscript indicating a starting position

Returns
-------
The same iterator object.
'''
        self._start_from = subscript
        return self

    def reversed(self):
        '''
Reverse the direction and return the iterator.

reversed()

Returns
-------
The same iterator object.
'''
        self._reversed = not self._reversed
        return self

    def subscripts(self):
        ''''
Set the iterator to return subscripts only and return the iterator.

subscripts()

Returns
-------
The same iterator object.
'''
        self._view_type = intersystems_iris.IRISIterator.VIEW_SUBSCRIPTS
        return self

    def values(self):
        '''
Set the iterator to return values only and return the iterator.

values()

Returns
-------
The same iterator object.
'''
        self._view_type = intersystems_iris.IRISIterator.VIEW_VALUES
        return self

    def items(self):
        '''
Set the iterator to return subscript and value tuples, and return the iterator.

items()

Returns
-------
The same iterator object.
'''
        self._view_type = intersystems_iris.IRISIterator.VIEW_ITEMS
        return self
