import uuid
from datetime import datetime, date, time, timezone
from collections import namedtuple
from ._SQLType import SQLType
from .._DBList import _DBList
from .._ListItem import _ListItem
from ._IRISStream import IRISStream, IRISBinaryStream
from ._Column import _Column


def from_timestamp_posix(posix):
    time = int(posix)
    if time > 0:
        time ^= 0x1000000000000000
    else:
        time |= 0xF000000000000000

    time /= 1000000

    value = datetime.fromtimestamp(time, timezone.utc).replace(tzinfo=None)
    return value


class _ResultSetRow:
    _locale = "latin-1"
    _connection = None

    def __init__(
        self,
        connection,
        columns=None,
        rowcount=0,
    ):
        self._connection = connection
        # index from user-inputted columns to columns received from server
        self.col_index = []
        self._col_type = []
        self._columns = columns
        if columns != None:
            for column in columns:
                self.col_index.append(column.slotPosition - 1)
                self._col_type.append(column.type)

            self._name_dict = {}
            for columnIndex, column in enumerate(columns):
                key = column.name.lower()
                if key in self._name_dict:
                    self._name_dict[key].append(columnIndex)
                else:
                    self._name_dict[key] = [columnIndex]

            self._name_dict_keys = list(self._name_dict.keys())
            self._name_dict_values = list(self._name_dict.values())

        if rowcount != 0:
            self._fast_select = True
            self.colCount = rowcount
            self._fast_first_iter = True
        else:
            self._fast_select = False
            self.colCount = len(columns) if columns != None else 0

        # number of columns received from server, aka number of items per row
        # self.colCount = rowcount

        # list of _ListItems corresponding to the various entries in the row, plus the offset of next row's first _ListItem
        self.rowItems = None

        self._locale = connection._connection_info._locale

        self._new_buffer = True

        # list of data offsets corresponding to each _ListItem in the row
        self._offsets = [0] * self.colCount

    class DataRow:
        _types = {
            SQLType.GUID: uuid.UUID,
            SQLType.BIGINT: int,
            SQLType.BINARY: bytes,
            SQLType.BIT: None,
            SQLType.CHAR: None,
            SQLType.DECIMAL: None,
            SQLType.DOUBLE: None,
            SQLType.FLOAT: None,
            SQLType.GUID: None,
            SQLType.INTEGER: int,
            SQLType.LONGVARBINARY: IRISBinaryStream,
            SQLType.LONGVARCHAR: IRISStream,
            SQLType.NUMERIC: None,
            SQLType.REAL: None,
            SQLType.SMALLINT: None,
            SQLType.DATE: None,
            SQLType.TIME: None,
            SQLType.TIMESTAMP: None,
            SQLType.TINYINT: None,
            SQLType.TYPE_DATE: None,
            SQLType.TYPE_TIME: None,
            SQLType.TYPE_TIMESTAMP: None,
            SQLType.VARBINARY: bytes,
            SQLType.VARCHAR: str,
            SQLType.WCHAR: None,
            SQLType.WLONGVARCHAR: None,
            SQLType.WVARCHAR: None,
            SQLType.DATE_HOROLOG: None,
            SQLType.TIME_HOROLOG: None,
            SQLType.TIMESTAMP_POSIX: None,
        }

        def __init__(self, rsrow):
            self._offsets = []
            for i in range(len(rsrow.rowItems) - 1):
                self._offsets.append(rsrow.rowItems[i])

            self._connection = rsrow._connection
            self._col_type = []
            self._columns = rsrow._columns
            self._name_dict = []
            self._name_dict_keys = []
            self._name_dict_values = []
            if hasattr(rsrow, "_name_dict"):
                self._col_type = rsrow._col_type
                self._name_dict = rsrow._name_dict
                self._name_dict_keys = rsrow._name_dict_keys
                self._name_dict_values = rsrow._name_dict_values

            self._list_item = rsrow._last_list_item
            self._locale = rsrow._locale

        def __getattr__(self, key):
            return self.__getitem__(key)

        def get(self):
            return self[:]

        def as_tuple(self):
            row = namedtuple("Row", [col.name for col in self._columns], rename=True)
            values = self[:]
            return row(*values)

        def __getitem__(self, key):
            if isinstance(key, str):
                key = key.lower()
                if key not in self._name_dict_keys:
                    raise KeyError("Column '" + key + "' does not exist")
                return self[self._name_dict[key][0] + 1]
            elif isinstance(key, int):
                if key < 0 or key > sum(len(item) for item in self._name_dict_values):
                    raise ValueError("Column index " + str(key) + " is out of range")
                if key == 0:
                    return self.__getitem__(slice(None, None, None))
                key = key - 1

                for i, list in enumerate(self._name_dict_values):
                    if key in list:
                        idx = i
                        break
                    else:
                        continue
                name = self._name_dict_keys[idx]

                self._list_item.next_offset = self._offsets[key]
                _DBList._get_list_element(self._list_item)
                item = _DBList._get(self._list_item, self._locale)
                _column: _Column = self._columns[idx]
                ctype = _column.type
                value_type = self._types[ctype] if ctype in self._types else None
                try:
                    if ctype == SQLType.DATE_HOROLOG:
                        HOROLOG_ORDINAL = date(1840, 12, 31).toordinal()
                        if item:
                            item = date.fromordinal(HOROLOG_ORDINAL + item)
                    if ctype == SQLType.TIMESTAMP and item:
                        item = item + '.000' if '.' not in item else item
                        item = datetime.strptime(item, '%Y-%m-%d %H:%M:%S.%f')
                    if ctype == SQLType.TIME_HOROLOG:
                        if item:
                            item = time(item // 3600, item % 3600 // 60, item % 3600 % 60)
                    if ctype == SQLType.GUID:
                        item = uuid.UUID(item)
                    if ctype == SQLType.TIMESTAMP_POSIX and item:
                        item = from_timestamp_posix(item)

                    if _column.tableName == "None" and _column.schema == "None":
                        # Ignore for anonymous tables
                        pass
                    elif item is None:
                        pass
                    elif value_type is bytes:
                        item = bytes(map(ord, item))
                    elif value_type and issubclass(value_type, IRISStream):
                        stream = value_type(self._connection, item)
                        item = stream.fetch()
                    elif value_type is not None and not isinstance(item, value_type):
                        item = value_type(item)
                except Exception:
                    pass

                setattr(self, name, item)
                return item
            elif isinstance(key, slice):
                list = []
                if key.start is None:
                    if key.stop is None:
                        for i in range(len(self._offsets))[key]:
                            list.append(self[i + 1])
                    else:
                        key2 = slice(None, key.stop - 1, key.step)
                        for i in range(len(self._offsets))[key2]:
                            list.append(self[i + 1])
                else:
                    if key.stop is None:
                        for i in range(len(self._offsets) + 1)[key]:
                            list.append(self[i])
                    else:
                        for i in range(len(self._offsets) + 1)[key]:
                            list.append(self[i])
                return list
            else:
                raise TypeError("List indices must be strings, integers, or slices, not " + type(key).__name__)

        def __len__(self):
            return len(self._offsets)

        class DataRowIterator:
            def __init__(self, data_row):
                self._data_row = data_row
                self._counter = 1

            def __iter__(self):
                return self

            def __next__(self):
                if self._counter > len(self._data_row):
                    raise StopIteration()
                next = self._data_row[self._counter]
                self._counter += 1
                return next

        def __iter__(self):
            return self.DataRowIterator(self)

    class DataRowFastSelect(DataRow):
        def __init__(self, rsrow, first_offset, length, buffer):
            super().__init__(rsrow)
            self._start_offset = first_offset
            self._length = length
            self._buffer = buffer
            self._rsrow = rsrow

        def __getitem__(self, key):
            if len(self._offsets) == 0:
                self.__len__()
            return super().__getitem__(key)

        def __len__(self):
            self._offsets = self._rsrow.indexRowFastSelect(self._start_offset, self._length, self._buffer)
            if not self._offsets:
                self._offsets = []
            return len(self._offsets)

    def indexRow(self, list_item):
        self._last_list_item = list_item
        rowItems = [0] * (self.colCount + 1)
        # buffer is reset by a read_message
        buffer = list_item.buffer
        length = list_item.list_buffer_end
        self._first_offset = list_item.next_offset

        if self._new_buffer:
            # First row after creation of RsRow
            # This is for cases where metadata is mixed with data (MRS)
            prev_offset = self._first_offset
            self._first_offset = 0
        else:
            prev_offset = self.rowItems[-1]

        for i in range(self.colCount + 1):
            try:
                if prev_offset > length:
                    raise Exception("Offset out of range")
                if prev_offset == length:
                    if i != 0:
                        if self._new_buffer and self.rowItems != None:
                            self.rowItems[-1] = 0
                    else:
                        if self.rowItems != None:
                            self.rowItems[-1] = 0
                    return False
                if i == 0:
                    rowItems[i] = prev_offset
                    continue
                curr_offset = _DBList._get_data_offset(buffer, prev_offset)
                rowItems[i] = curr_offset
                prev_offset = curr_offset
            except IndexError:
                raise IndexError(
                    "Row incomplete: " + str(self.colCount) + " items expected, but " + str(i) + " were found"
                )
        self.update(rowItems)
        return True

    def indexRowFastSelect(self, prev_offset, length, buffer):
        rowItems = [-1] * (self.colCount + 1)
        for i in range(self.colCount + 1):
            try:
                if prev_offset > length:
                    raise Exception("Offset out of range")
                if prev_offset == length:
                    if i != 0:
                        if self._fast_select:
                            # Fill in the remaining entries with -1
                            j = i - 1
                            for idx in range(len(rowItems) - i + 1):
                                rowItems[j] = -1
                                j += 1
                            return self.update(rowItems)
                        else:
                            if self._new_buffer and self.rowItems != None:
                                self.rowItems[-1] = 0
                    else:
                        if self.rowItems != None:
                            self.rowItems[-1] = 0
                    return
                if i == 0:
                    rowItems[i] = prev_offset
                    continue
                curr_offset = _DBList._get_data_offset(buffer, prev_offset)
                rowItems[i] = curr_offset
                prev_offset = curr_offset
            except IndexError:
                raise IndexError(
                    "Row incomplete: " + str(self.colCount) + " items expected, but " + str(i) + " were found"
                )
        return self.update(rowItems)

    def update(self, rowItems):
        if self._fast_select:
            colIndexOffsets = [0] * (len(self.col_index) + 1)
            for idx, i in enumerate(self.col_index):
                colIndexOffsets[idx] = rowItems[i]
            colIndexOffsets[-1] = self._last_list_item.next_offset
            self.rowItems = colIndexOffsets
            return self.rowItems[: self.colCount]
        self.rowItems = rowItems
        self._offsets = self.DataRow(self)
        self._new_buffer = False

    def cloneListItem(self, list_item):
        clone = _ListItem(list_item.buffer)
        clone.is_null = list_item.is_null
        clone.is_undefined = list_item.is_undefined
        clone.type = list_item.type
        clone.data_offset = list_item.data_offset
        clone.data_length = list_item.data_length
        clone.next_offset = list_item.next_offset
        clone.by_reference = list_item.by_reference
        return clone

    def get(self):
        return self._offsets.get()
