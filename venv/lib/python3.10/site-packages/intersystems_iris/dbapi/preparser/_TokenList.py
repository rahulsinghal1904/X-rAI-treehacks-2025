import abc
import intersystems_iris.dbapi.preparser._Token

class ITokenEnumerator(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def Reset(self):
        pass

    @abc.abstractmethod
    def Current(self):
        pass

    @abc.abstractmethod
    def MoveNext(self):
        pass

    @abc.abstractmethod
    def MovePrevious(self):
        pass

    @abc.abstractmethod
    def Clone(self):
        pass

    @abc.abstractmethod
    def Count(self):
        pass

class TokenListNode(object):
    def __init__(self, Value, Owner):
        if not isinstance(Value, intersystems_iris.dbapi.preparser._Token._Token):
            raise TypeError("Value must be a _Token")
        
        self.m_Previous = None
        self.m_Next = None
        self.m_Value = Value

    def GetValue(self):
        return self.m_Value

    def SetValue(self, value):
        if not isinstance(value, intersystems_iris.dbapi.preparser._Token._Token):
            raise TypeError("value must be a _Token")

        self.m_Value = value

    def Previous(self):
        return self.m_Previous

    def Next(self):
        return self.m_Next

    def SetPrevious(self, node):
        if not isinstance(node, TokenListNode):
            raise TypeError("node must be a TokenListNode")

        self.m_Previous = node

    def SetNext(self, node):
        if not isinstance(node, TokenListNode):
            raise TypeError("node must be a TokenListNode")

        self.m_Next = node


class _TokenList(object): 
    def __init__(self):
        self.m_Count = 0
        self.m_Head = None
        self.m_Tail = None

    def Count(self):
        return self.m_Count

    def GetToken(self, index):
        try:
            index = int(index)
        except (TypeError, ValueError):
            raise ValueError("index must be an integer")

        if index < 0 or index > (self.Count() - 1):
            raise IndexError("Index " + index + " is out of range")
            
        node = self.First()
        for i in range(index):
            node = node.Next()
        return node.GetValue()

    def SetToken(self, index, value):
        try:
            index = int(index)
        except (TypeError, ValueError):
            raise ValueError("index must be an integer")

        if index < 0 or index > (self.Count() - 1):
            raise IndexError("Index " + index + " is out of range")

        node = self.First()
        i = 0
        for i in range(index):
            node = node.Next()
        node.SetValue(value)

    def GetEnumerator(self):
        return LinkedListEnumerator(self)

    def __iter__(self):
        return LinkedListEnumerator(self)

    def Append(self, Value):
        node = TokenListNode(Value, self)
        if self.m_Head == None:
            self.m_Head = node
        else:
            self.m_Tail.SetNext(node)
            node.SetPrevious(self.m_Tail)
        self.m_Tail = node
        self.m_Count += 1
        return node

    def Prepend(self, Value):
        node = TokenListNode(Value, self)
        if self.m_Tail == None:
            self.m_Tail = node
        else:
            self.m_Head.SetPrevious(node)
            node.SetNext(self.m_Head)
        self.m_Head = node
        self.m_Count += 1
        return node

    def InsertBefore(self, Before, Value):
        if not isinstance(Before, TokenListNode):
            raise TypeError("Before must be a TokenListNode")

        node = TokenListNode(Value, self)
        prev = Before.Previous()
        node.SetNext(Before)
        node.SetPrevious(prev)
        Before.SetPrevious(node)
        if prev != None:
            prev.SetNext(node)
        else:
            self.m_Head = node
        self.m_Count += 1
        return node

    def InsertAfter(self, After, Value):
        if not isinstance(After, TokenListNode):
            raise TypeError("Before must be a TokenListNode")

        node = TokenListNode(Value, self)
        next = After.Next()
        node.SetNext(next)
        node.SetPrevious(After)
        After.SetNext(node)
        if next != None:
            next.SetPrevious(node)
        else:
            self.m_Tail = node
        self.m_Count += 1
        return node

    def Remove(self, Node):
        if not isinstance(Node, TokenListNode):
            raise TypeError("Node must be a TokenListNode")
        if self.m_Head == None:
            raise TypeError("Cannot remove Node from empty list")
            
        prev = Node.Previous()
        next = Node.Next()
        if Node == self.m_Head and Node == self.m_Tail:
            self.m_Head = self.m_Tail = None
        elif Node == self.m_Head:
            self.m_Head = next
            self.m_Head.SetPrevious(None)
        elif Node == self.m_Tail:
            self.m_Tail = prev
            self.m_Tail.SetNext(None)
        else:
            #  the node is in the middle of the list
            next.SetPrevious(prev)
            prev.SetNext(next)
        self.m_Count -= 1

    def First(self):
        return self.m_Head

    def Last(self):
        return self.m_Tail


class LinkedListEnumerator(ITokenEnumerator):
    def Clone(self):
        return LinkedListEnumerator(self)

    def __init__(self, arg):
        if isinstance(arg, _TokenList):
            super(LinkedListEnumerator, self).__init__()
            self.m_List = arg
            self.m_Current = None
            self.m_bEOF = False
        elif isinstance(arg, LinkedListEnumerator):
            super(LinkedListEnumerator, self).__init__()
            self.m_List = arg.m_List
            self.m_Current = arg.m_Current
            self.m_bEOF = arg.m_bEOF
        else:
            raise TypeError("arg must be a _TokenList or LinkedListEnumerator")

    def Count(self):
        return self.m_List.Count()

    def Current(self):
        return self.m_Current.GetValue()

    def MoveNext(self):
        if self.m_bEOF:
            return False
        elif self.m_Current == None:
            self.m_Current = self.m_List.First()
        else:
            self.m_Current = self.m_Current.Next()
        self.m_bEOF = (self.m_Current == None)
        return not self.m_bEOF

    def MovePrevious(self):
        if self.m_bEOF:
            return False
        elif self.m_Current == None:
            self.m_Current = self.m_List.Last()
        else:
            self.m_Current = self.m_Current.Previous()
        self.m_bEOF = (self.m_Current == None)
        return not self.m_bEOF

    def Reset(self):
        self.m_Current = None
        self.m_bEOF = False

    def __iter__(self):
        self.m_Current = None
        self.m_bEOF = False
        return self

    def __next__(self):
        if self.MoveNext():
            return self.Current()
        raise StopIteration
