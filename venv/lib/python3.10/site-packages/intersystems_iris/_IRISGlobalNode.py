import collections.abc
import intersystems_iris._IRISIterator
import intersystems_iris._IRISGlobalNodeView

class BEFOREFIRST(object):
    pass

class AFTERLAST(object):
    pass

class _point(object):

    def __init__(self, key, inclusive):
        self._key = key
        self._inclusive = inclusive

    def _iterator_copy(self):
        if self.is_marker():
            return _point(None, self._inclusive)
        else:
            return _point(self._key, self._inclusive)

    def is_marker(self):
        return isinstance(self._key,BEFOREFIRST) or isinstance(self._key,AFTERLAST)

class _IRISGlobalNode(collections.abc.Container, collections.abc.Reversible, collections.abc.Iterable):
    '''
IRISGlobalNode provides an iterable interface that behaves like a virtual dictionary representing the immediate children of a global node.

IRISGlobalNode is iterable:
IRISGlobalNode supports iterable interface using for-loop. For example:
    for x in node:
        print(x)

IRISGlobalNode supports views:
Methods keys(), subscripts(), values(), items(), nodes(), and nodeitems() return view objects of IRISGlobalNodeView.
They provide specific views on the IRISGlobalNode entries which can be iterated over to yield their respective data, and support membership tests.

For example, the items() method returns a view object containing list of subscript-value pairs in the IRISGlobalNode.
    for x in node.items():
        print(x)

IRISGlobalNode is sliceable:
Through standard Python slicing syntax, IRISGlobalNode can be iterated over a more restricted ranges of subscripts.
    node[start:stop:step]
This results in a new IRISGlobalNode object with the subsript range limited to from start (inclusive) to stop (exclusive). step can be 1 or -1, meaning traversing in forward direction or in reversed direction.

IRISGlobalNode is reversable:
If the "step" variable is -1 in the slicing syntax, the reaversing of the IRISGlobalNode will go backwards - reversed from the standard order.
For example:
    for x in node[8:2:-1]: print(x)
will traverse the subscripts from 8 (inclusive) to 2 (exclusive).

IRISGlobalNode is indexable and supports membership tests
node[x] will return the value of the global node with subscript x. "x in node" returns a boolean.
'''

    def __init__(self, irisnative, global_name, *subscripts):
        self._irisnative = irisnative
        self._global_name = global_name
        self._subscripts = subscripts
        self._start = _point(BEFOREFIRST(), False)
        self._stop = _point(AFTERLAST(), False)
        self._reversed = False

    def __iter__(self):
        return intersystems_iris.IRISIterator(intersystems_iris.IRISGlobalNodeView(self, intersystems_iris.IRISIterator.VIEW_SUBSCRIPTS))

    def __contains__(self, subscript):
        return self._irisnative.isDefined(self._global_name, *self._subscripts, subscript) != 0

    def __getitem__(self, subscript):
        if isinstance(subscript, slice):
            newnode = self._clone()
            step = 1 if subscript.step is None else subscript.step
            if not isinstance(step, int):
                raise TypeError("slice indices must be integers or None")
            if step != 1 and step != -1:
                raise ValueError("slice step can only be 1, -1 or None")
            if step > 0:
                if subscript.start is not None:
                    newnode._start = self._max_start(self._start, _point(subscript.start, True), newnode._reversed)
                else:
                    newnode._start = self._start
                if subscript.stop is not None:
                    newnode._stop = self._min_stop(self._stop, _point(subscript.stop, False), newnode._reversed)
                else:
                    newnode._stop = self._stop
            else:
                newnode._reversed = not self._reversed
                if subscript.start is not None:
                    newnode._start = self._max_start(self._stop, _point(subscript.start, True), newnode._reversed)
                else:
                    newnode._start = self._stop
                if subscript.stop is not None:
                    newnode._stop = self._min_stop(self._start, _point(subscript.stop, False), newnode._reversed)
                else:
                    newnode._stop = self._start
            return newnode
        else:
            iris_value = self._irisnative.get(self._global_name, *self._subscripts, subscript)
            if iris_value == None:
                raise KeyError("<UNDEFINED>")
            return iris_value

    def __setitem__(self, subscript, value):
        self._irisnative.set(value, self._global_name, *self._subscripts, subscript)

    def __reversed__(self):
        newnode = self._clone()
        newnode._reversed = not self._reversed
        newnode._start = self._stop
        newnode._stop = self._start
        return newnode.__iter__()

    def __delitem__(self, subscript):
        self._irisnative.kill(self._global_name, *self._subscripts, subscript)

    def get(self, subscript, default_value):
        '''
Returns the value of a given subscript. Returns the default_value if the node is UNDEFINED.
'''
        iris_value = self._irisnative.get(self._global_name, *self._subscripts, subscript)
        return default_value if iris_value == None else iris_value

    def node(self, subscript):
        '''
Returns IRISGlobalNode object representing the subnode at a given subscript.
'''
        return self._irisnative.node(self._global_name, *self._subscripts, subscript)

    def keys(self):
        '''
Returns an IRISGlobalNodeView object of the current IRISGlobalNode which can be iterated over to yield only subscripts.
'''
        return intersystems_iris.IRISGlobalNodeView(self, intersystems_iris.IRISIterator.VIEW_SUBSCRIPTS)

    def subscripts(self):
        '''
Returns an IRISGlobalNodeView object of the current IRISGlobalNode which can be iterated over to yield only subscripts.
'''
        return intersystems_iris.IRISGlobalNodeView(self, intersystems_iris.IRISIterator.VIEW_SUBSCRIPTS)

    def values(self):
        '''
Returns an IRISGlobalNodeView object of the current IRISGlobalNode which can be iterated over to yield only values.
'''
        return intersystems_iris.IRISGlobalNodeView(self, intersystems_iris.IRISIterator.VIEW_VALUES)

    def items(self):
        '''
Returns an IRISGlobalNodeView object of the current IRISGlobalNode which can be iterated over to yield subscript-value tuples.
'''
        return intersystems_iris.IRISGlobalNodeView(self, intersystems_iris.IRISIterator.VIEW_ITEMS)

    def nodes(self):
        '''
Returns an IRISGlobalNodeView object of the current IRISGlobalNode which can be iterated over to yield subnodes.
'''
        return intersystems_iris.IRISGlobalNodeView(self, intersystems_iris.IRISIterator.VIEW_NODES)

    def nodesitems(self):
        '''
Returns an IRISGlobalNodeView object of the current IRISGlobalNode which can be iterated over to yield subscript-subnode tuples.
'''
        return intersystems_iris.IRISGlobalNodeView(self, intersystems_iris.IRISIterator.VIEW_NODEITEMS)

    def _clone(self):
        newnode = intersystems_iris.IRISGlobalNode(self._irisnative, self._global_name, *self._subscripts)
        newnode._start = self._start
        newnode._stop = self._stop
        newnode._reversed = self._reversed
        return newnode

    def _min_stop(self, X, Y, reversed):
        sort_order = self._sort_order(self._irisnative, X, Y, self._global_name, *self._subscripts)
        if sort_order > 0:
            return Y if reversed else X
        elif sort_order < 0:
            return X if reversed else Y
        else:
            return Y if X._inclusive or X._inclusive == Y._inclusive else X

    def _max_start(self, X, Y, reversed):
        sort_order = self._sort_order(self._irisnative, X, Y, self._global_name, *self._subscripts)
        if sort_order > 0:
            return X if reversed else Y
        elif sort_order < 0:
            return Y if reversed else X
        else:
            return Y if X._inclusive or X._inclusive == Y._inclusive else X

    @staticmethod
    def _sort_order(irisnative, X, Y, global_name, *subscripts):
        if isinstance(X._key, BEFOREFIRST) and isinstance(Y._key, BEFOREFIRST):
            return 0
        if isinstance(X._key, AFTERLAST) and isinstance(Y._key, AFTERLAST):
            return 0
        if isinstance(X._key, BEFOREFIRST):
            reference_Y = intersystems_iris.IRISReference(Y._key, str)
            irisnative._sortOrder(reference_Y, None, global_name, *subscripts)
            Y._key = reference_Y.get_value()
            return 1
        if isinstance(X._key, AFTERLAST):
            reference_Y = intersystems_iris.IRISReference(Y._key, str)
            irisnative._sortOrder(reference_Y, None, global_name, *subscripts)
            Y._key = reference_Y.get_value()
            return -1
        if isinstance(Y._key, BEFOREFIRST):
            reference_X = intersystems_iris.IRISReference(X._key, str)
            irisnative._sortOrder(reference_X, None, global_name, *subscripts)
            X._key = reference_X.get_value()
            return -1
        if isinstance(Y._key, AFTERLAST):
            reference_X = intersystems_iris.IRISReference(X._key, str)
            irisnative._sortOrder(reference_X, None, global_name, *subscripts)
            X._key = reference_X.get_value()
            return 1
        reference_X = intersystems_iris.IRISReference(X._key, str)
        reference_Y = intersystems_iris.IRISReference(Y._key, str)
        sort_order = irisnative._sortOrder(reference_X, reference_Y, global_name, *subscripts)
        X._key = reference_X.get_value()
        Y._key = reference_Y.get_value()
        return sort_order

    @classmethod
    def _sort_order_by_key(cls, irisnative, keyX, keyY, global_name, *subscripts):
        return cls._sort_order(irisnative, _point(keyX, None), _point(keyY, None), global_name, *subscripts)

    @staticmethod
    def _point(key, inclusive):
        return _point(key, inclusive)

    def _slice_state(self):
        '''
This is an internal method intended to be used for debugging purposes only.

Returns a formatted string that describes the state of slicing in the IRISGlobalNode object.

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
            left_key = "None" if self._stop.is_marker() else ("'"+self._stop._key.replace("'","\\'")+"'")
            arrows = " <<< "
            right_key = "None" if self._start.is_marker() else ("'"+self._start._key.replace("'","\\'")+"'")
            right_bracket = " ]" if self._start._inclusive else " )"
        else:
            left_bracket = "[ " if self._start._inclusive else "( "
            left_key = "None" if self._start.is_marker() else ("'"+self._start._key.replace("'","\\'")+"'")
            arrows = " >>> "
            right_key = "None" if self._stop.is_marker() else ("'"+self._stop._key.replace("'","\\'")+"'")
            right_bracket = " ]" if self._stop._inclusive else " )"
        return left_bracket + left_key + arrows + right_key + right_bracket


