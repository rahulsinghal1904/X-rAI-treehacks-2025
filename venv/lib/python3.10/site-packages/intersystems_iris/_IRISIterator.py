import collections.abc
import intersystems_iris._IRISGlobalNode

class _IRISIterator(collections.abc.Iterator, collections.abc.Iterable):
    '''
This class implements the iterator of IRISGlobalNode iterable. It can be used to iterate over the contents of IRISGlobalNode which behaves like a virtual dictionary representing the immediate children of a global node.
'''

    VIEW_SUBSCRIPTS = 1
    VIEW_VALUES = 2
    VIEW_ITEMS = 3
    VIEW_NODES = 4
    VIEW_NODEITEMS = 5

    def __init__(self, view):
        self._irisnative = view._node._irisnative
        self._global_name = view._node._global_name
        self._subscripts = view._node._subscripts
        self._start = view._node._start._iterator_copy()
        self._stop = view._node._stop._iterator_copy()
        self._reversed = view._node._reversed
        self._view_type = view._view_type
        self._at_end = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._at_end:
            raise StopIteration
        # deal with inclusiveness or call _nextNode
        if self._start._inclusive:
            dollar_data = self._irisnative.isDefined(self._global_name, *self._subscripts, self._start._key)
            if dollar_data == 0:
                key_value = self._nextNode()
            elif dollar_data == 10:
                if self._has_passed_stop(self._start._key):
                    self._at_end = True
                    raise StopIteration
                key_value = (self._start._key, None)
            else:
                if self._has_passed_stop(self._start._key):
                    self._at_end = True
                    raise StopIteration
                value = self._irisnative.get(self._global_name, *self._subscripts, self._start._key)
                key_value = (self._start._key, value)
        else:
            key_value = self._nextNode()
        # check if we are at end
        returned_key = key_value[0]
        if returned_key == None or len(returned_key) == 0:
            self._at_end = True
            raise StopIteration
        # save advancing pointer
        self._start = intersystems_iris._IRISGlobalNode._point(returned_key, False)
        # return value
        if self._view_type == intersystems_iris.IRISIterator.VIEW_SUBSCRIPTS:
            return key_value[0]
        elif self._view_type == intersystems_iris.IRISIterator.VIEW_VALUES:
            return key_value[1]
        elif self._view_type == intersystems_iris.IRISIterator.VIEW_ITEMS:
            return key_value
        elif self._view_type == intersystems_iris.IRISIterator.VIEW_NODES:
            return self._irisnative.node(self._global_name, *self._subscripts, key_value[1])
        elif self._view_type == intersystems_iris.IRISIterator.VIEW_NODEITEMS:
            return (key_value[0], self._irisnative.node(self._global_name, *self._subscripts, key_value[1]))
        else:
            raise TypeError("Unrecognized view type")

    def _nextNode(self):
        if self._stop._key is None:
            return self._irisnative._nextNode(self._reversed, self._global_name, *self._subscripts, self._start._key)
        SUPPORT_STOP_ON = self._irisnative._connection._connection_info.protocol_version >= 63
        if SUPPORT_STOP_ON:
            key_value = self._irisnative._nextNodeWithStop(self._reversed, self._stop._key, self._global_name, *self._subscripts, self._start._key)
            if not self._stop._inclusive and key_value[0] == self._stop._key:
                self._at_end = True
                raise StopIteration
            return key_value
        else:
            key_value = self._irisnative._nextNode(self._reversed, self._global_name, *self._subscripts, self._start._key)
            if self._has_passed_stop(key_value[0]):
                self._at_end = True
                raise StopIteration
            return key_value

    def _has_passed_stop(self, current_value):
        if self._stop._key is None:
            return False
        if self._reversed:
            if self._stop._inclusive:
                if intersystems_iris.IRISGlobalNode._sort_order_by_key(self._irisnative, self._stop._key, current_value, self._global_name, *self._subscripts) < 0:
                    return True
            else:
                if intersystems_iris.IRISGlobalNode._sort_order_by_key(self._irisnative, self._stop._key, current_value, self._global_name, *self._subscripts) <= 0:
                    return True
        else:
            if self._stop._inclusive:
                if intersystems_iris.IRISGlobalNode._sort_order_by_key(self._irisnative, self._stop._key, current_value, self._global_name, *self._subscripts) > 0:
                    return True
            else:
                if intersystems_iris.IRISGlobalNode._sort_order_by_key(self._irisnative, self._stop._key, current_value, self._global_name, *self._subscripts) >= 0:
                    return True
        return False

    def _slice_state(self):
        '''
This is an internal method intended to be used for debugging purposes only.

Returns a formatted string that describes the state of slicing in the IRISIterator object.

slice_state()

IRISGlobalNode object can be sliced to limit the subscript traversing range.

node[start:stop:step]

This results in a new IRISGlobalNode object with the subsript range limited to from start (inclusive) to stop (exclusive).
step can be 1 or -1, meaning traversing in forward direction or in reversed direction.

The formatted string uses the standard mathematical notation of bracketing.
Square brackets, [], are used to denote closed intervals with inclusive endpoints.
Parentheses, (), are used to denote open intervals with exclusive endpoints
For example, "[ 3 >>> 7 )" means from 3 to 7, forward direction, inclusive of 3 but exclusive of 7. This is what you will get with a simple slicing of node[3:7]
"[ 1 <<< 9 ]" means from 9 to 1, in reversed direction, inclusive on both ends.

Return Value
------------
Returns slice state.
        '''
        if self._reversed:
            left_bracket = "[ " if self._stop._inclusive else "( "
            left_key = "None" if self._stop._key == None else ("'"+self._stop._key.replace("'","\\'")+"'")
            arrows = " <<< "
            right_key = "None" if self._start._key == None else ("'"+self._start._key.replace("'","\\'")+"'")
            right_bracket = " ]" if self._start._inclusive else " )"
        else:
            left_bracket = "[ " if self._start._inclusive else "( "
            left_key = "None" if self._start._key == None else ("'"+self._start._key.replace("'","\\'")+"'")
            arrows = " >>> "
            right_key = "None" if self._stop._key == None else ("'"+self._stop._key.replace("'","\\'")+"'")
            right_bracket = " ]" if self._stop._inclusive else " )"
        return left_bracket + left_key + arrows + right_key + right_bracket
