import collections.abc
import intersystems_iris._IRISIterator

class _IRISGlobalNodeView(collections.abc.MappingView, collections.abc.Reversible):
    '''
IRISGlobalNodeView class implements view objects for IRISGlobalNode which can be iterated over to yield their respective data.
'''

    def __init__(self, node, view_type):
        self._node = node
        self._view_type = view_type

    def __iter__(self):
        return intersystems_iris.IRISIterator(self)

    def __reversed__(self):
        iter = intersystems_iris.IRISIterator(self)
        iter._reversed = not iter._reversed
        temp = iter._start
        iter._start = iter._stop
        iter._stop = temp
        return iter

    def __len__(self):
        raise TypeError("object of type 'IRISGlobalNodeView' has no len()")