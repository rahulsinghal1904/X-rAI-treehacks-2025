import enum

class SQLType(enum.IntEnum):
    BIGINT = -5
    BINARY = -2
    BIT = -7
    CHAR = 1
    DECIMAL = 3
    DOUBLE = 8
    FLOAT = 6
    GUID = -11
    INTEGER = 4
    LONGVARBINARY = -4
    LONGVARCHAR = -1
    NUMERIC = 2
    REAL = 7
    SMALLINT = 5
    DATE = 9
    TIME = 10
    TIMESTAMP = 11
    TINYINT = -6
    TYPE_DATE = 91
    TYPE_TIME = 92
    TYPE_TIMESTAMP = 93
    VARBINARY = -3
    VARCHAR = 12
    WCHAR = -8
    WLONGVARCHAR = -10
    WVARCHAR = -9
    DATE_HOROLOG = 1091
    TIME_HOROLOG = 1092
    TIMESTAMP_POSIX = 1093