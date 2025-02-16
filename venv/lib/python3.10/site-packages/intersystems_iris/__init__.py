'''
IRIS Native API for Python.

This module provides highly efficient and lightweight access to IRIS,
including the Global Module and object oriented programming environment.
'''
from . import _BufferReader
from . import _BufferWriter
from . import _ConnectionInformation
from . import _ConnectionParameters
from . import _Constant
from . import _Device
from . import _DBList
from . import _GatewayContext
from . import _GatewayException
from . import _GatewayUtility
from . import _InStream
from . import _IRIS
from . import _IRISConnection
from . import _IRISGlobalNode
from . import _IRISGlobalNodeView
from . import _IRISIterator
from . import _IRISList
from . import _IRISObject
from . import _IRISOREF
from . import _IRISReference
from . import _LegacyIterator
from . import _ListItem
from . import _ListReader
from . import _ListWriter
from . import _LogFileStream
from . import _MessageHeader
from . import _OutStream
from . import _PrintStream
from . import _PythonGateway

from intersystems_iris._IRISNative import connect  # noqa
from intersystems_iris._IRISNative import createConnection  # noqa
from intersystems_iris._IRISNative import createIRIS  # noqa


class GatewayContext(_GatewayContext._GatewayContext):
    pass


class IRIS(_IRIS._IRIS):
    pass


class IRISConnection(_IRISConnection._IRISConnection):
    pass


class IRISGlobalNode(_IRISGlobalNode._IRISGlobalNode):
    pass


class IRISGlobalNodeView(_IRISGlobalNodeView._IRISGlobalNodeView):
    pass


class IRISIterator(_IRISIterator._IRISIterator):
    pass


class IRISList(_IRISList._IRISList):
    pass


class IRISObject(_IRISObject._IRISObject):
    pass


class IRISReference(_IRISReference._IRISReference):
    pass


class LegacyIterator(_LegacyIterator._LegacyIterator):
    pass
