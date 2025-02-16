import datetime
import decimal
import enum
import intersystems_iris.dbapi._Descriptor
from intersystems_iris._IRISOREF import _IRISOREF


class ParameterMode(enum.IntEnum):
    UNKNOWN = 0
    INPUT = 1
    INPUT_OUTPUT = 2
    UNUSED = 3
    OUTPUT = 4
    REPLACED_LITERAL = 5
    DEFAULT_PARAMETER = 6
    RETURN_VALUE = 7


class _Parameter(intersystems_iris.dbapi._Descriptor._Descriptor):
    def __init__(
        self,
        value=None,
        mode=ParameterMode.UNKNOWN,
        paramType="?",
        name="",
        execParam=False,
        bound=False,
        type=0,
        precision=0,
        scale=None,
        nullable=0,
        slotPosition=None,
    ):
        if not isinstance(mode, ParameterMode):
            raise TypeError("mode must be a ParameterMode")
        paramType = str(paramType)
        if len(paramType) > 1:
            raise ValueError("paramType must be a single character")
        name = str(name)
        execParam = bool(execParam)
        bound = bool(bound)
        if slotPosition is not None:
            try:
                slotPosition = int(slotPosition)
            except (TypeError, ValueError):
                raise TypeError("slotPosition must be an integer")
            if slotPosition < 0:
                raise ValueError("slotPosition must be positive")

        super().__init__(type, precision, scale, nullable)

        self.__value = value
        self.mode = mode
        self.__paramType = paramType
        self.name = name
        self.execParam = execParam
        self.__bound = bound
        self.slotPosition = slotPosition

        self._values = list()

        self.parsermatched = False
        self.matchedParameterList = None

    def __repr__(self) -> str:
        if self.mode not in [ParameterMode.UNKNOWN, ParameterMode.INPUT]:
            return f"<{self.mode.name}>{repr(self.value)}"
        else:
            return f"<{self.mode.name}>"

    def Clone(self, value=None):
        clone = _Parameter(
            value or self.value,
            self.mode,
            self.paramType,
            self.name,
            self.execParam,
            self.bound,
            self.type,
            self.precision,
            self.scale,
            self.nullable,
            self.slotPosition,
        )
        clone.cloneMe(self)

        clone.parsermatched = self.parsermatched
        clone.matchedParameterList = self.matchedParameterList

        return clone

    @property
    def bound(self):
        return self.__bound

    @property
    def paramType(self):
        return self.__paramType

    @property
    def value(self):
        _set_switcher = {
            type(None): lambda v: None,
            # str: lambda v : v or b'\x00',
            datetime.time: lambda v: v.strftime("%H:%M:%S.%f"),
            datetime.date: lambda v: v.strftime("%Y-%m-%d"),
            datetime.datetime: lambda v: v.strftime("%Y-%m-%d %H:%M:%S.%f"),
            bytes: lambda v: v,
            bytearray: lambda v: v,
            bool: lambda v: 1 if v else 0,
            int: lambda v: v,
            float: lambda v: v,
            decimal.Decimal: lambda v: v,
            _IRISOREF: lambda v: str(v),
        }
        func = None
        if issubclass(type(self.__value), enum.Enum):
            value = self.__value.value
        elif type(self.__value) in _set_switcher:
            func = _set_switcher[type(self.__value)]
            value = func(self.__value)
        else:
            value = str(self.__value)
        if self.mode == ParameterMode.REPLACED_LITERAL:
            if isinstance(value, str) and value.isdigit() and str(int(value)) == value:
                value = int(value)
        return value

    def _copy_cached_info(self, desc, copy_replaced):
        self.type = desc.type
        self.precision = desc.precision
        self.scale = desc.scale
        self.nullable = desc.nullable
        self.name = desc.name
        if (
            (self.mode != ParameterMode.REPLACED_LITERAL)
            and (desc.mode != ParameterMode.REPLACED_LITERAL)
            and (desc.mode != ParameterMode.UNKNOWN)
        ):
            self.mode = desc.mode

        if not copy_replaced:
            return

        if desc.mode == ParameterMode.REPLACED_LITERAL:
            self.mode = ParameterMode.REPLACED_LITERAL
            self.__value = desc.value
            self._values = list()
            if len(desc._values) > 0:
                self._values.append(desc._values[0])
        return

    def _bind(self, value, parameter_sets):
        if isinstance(value, list):
            self._values = value
        else:
            if parameter_sets == 0:
                self._values = []
            size = len(self._values)
            if parameter_sets + 1 < size or size < parameter_sets:
                raise Exception("Not all parameters bound for this set")
            elif parameter_sets == size:
                self._values.append(value)
            else:
                # Rebinding
                self._values[size - 1] = value
        self.__bound = True
        if self.mode == ParameterMode.OUTPUT or self.mode == ParameterMode.INPUT_OUTPUT:
            self.mode = ParameterMode.INPUT_OUTPUT
        else:
            self.mode = ParameterMode.INPUT
