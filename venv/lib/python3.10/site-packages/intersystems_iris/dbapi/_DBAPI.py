from typing import Union
import struct
import copy
import enum
import copy
import uuid
import decimal
import intersystems_iris
from collections import namedtuple
from . import _Message
import intersystems_iris.dbapi._Parameter
import intersystems_iris.dbapi._Column
from ._ResultSetRow import _ResultSetRow
import intersystems_iris.dbapi._ParameterCollection
import intersystems_iris.dbapi.preparser._PreParser
from intersystems_iris.dbapi._Parameter import ParameterMode
from intersystems_iris.dbapi.preparser._PreParser import StatementType, MultiValuesInsert
from intersystems_iris._IRISConnection import Feature
from intersystems_iris._InStream import _InStream
from intersystems_iris.dbapi._IRISStream import (
    IRISStream,
    IRISBinaryStream,
)
from intersystems_iris._IRISOREF import _IRISOREF
from ._SQLType import SQLType

from .._IRISNative import connect as native_connect
from .._IRISEmbedded import _IRISEmbedded
from intersystems_iris._IRISConnection import _IRISConnection


def NotImplementedErrorDBAPI(msg=None):
    import traceback

    if msg is None:
        traceback.print_stack()
        msg = "Coming soon to an IRIS DB API near you!"
    return NotImplementedError(msg)


def embedded_connect(*args, hostname=None, port=None, namespace=None, username=None, password=None, **kw):
    connection = _IRISEmbedded()
    connection.connect(hostname, port, namespace, username, password, **kw)
    return connection


def connect(*args, embedded=False, hostname=None, port=None, namespace=None, username=None, password=None, **kw) -> Union[_IRISConnection, _IRISEmbedded]:
    try:
        if not embedded:
            return native_connect(
                *args, hostname=hostname, port=port, namespace=namespace, username=username, password=password, **kw
            )
        else:
            return embedded_connect(
                *args, hostname=hostname, port=port, namespace=namespace, username=username, password=password, **kw
            )
    except Exception as e:
        raise OperationalError(e)


class ServerReturnType(enum.IntEnum):
    NO_RETURN_VALUE = 0
    IGNORE_RETURN_VALUE = 1
    HAS_RETURN_VALUE = 2
    NULL_RETURN_VALUE = 3


class CursorType(enum.IntEnum):
    DEFAULT = 0
    PREPARED = 1
    CALLABLE = 2


# api globals, methods, classes, etc.
# globals
apilevel = "2.0"
threadsafety = 0
paramstyle = "qmark"


class _BaseCursor:
    embedded = False

    def __init__(self, connection):
        self._connection = connection

        self.statement = None
        self._parsed_statement = None

        self._columns = None
        self._rowcount = -1
        self.arraysize = 1

        self._result_set = None

        self._rsrow = None
        self._rownumber = 0
        self._cursor_ptr = 0
        self._scroll_flag = False

        self.maxRowItemCount = 0

        self._is_batch_update = False

        self._exec_params = None
        self._params = intersystems_iris.dbapi._ParameterCollection._ParameterCollection()
        self._multiple_result_sets = False
        self._mrs_done = False
        self._has_return_value = 0
        self._cursor_type = 0
        self._parameter_list_mismatch_exception = False
        self._fetch_done = False
        self._output_parameter_list = None

        self._sqlcode = None
        self._lastrowid = None

        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._connection.rollback()
        self.close()

    def __iter__(self):
        if self._result_set == None:
            raise InterfaceError(
                "Either execute has not yet been called, or the previous call of execute did not return a result set"
            )
        return self

    def __next__(self):
        row = self.fetchone()
        if row:
            return row
        raise StopIteration

    # non-api methods and classes
    def isClosed(self):
        return self._closed

    def setinputsizes(self, sizes):
        raise NotImplementedErrorDBAPI()

    def setoutputsize(size, column=None):
        raise NotImplementedErrorDBAPI()

    @property
    def sqlcode(self):
        if self._closed:
            raise InterfaceError("Cursor is closed")
        return 0 if self._sqlcode is None else self._sqlcode

    def close(self):
        if self._closed:
            return
        self._columns = None
        self._rowcount = -1
        self.arraysize = 0

        self._connection = None
        self._in_message = None
        self._out_message = None

        self._result_set = None

        if self._rsrow != None:
            self._rsrow = None
        self._cursor_ptr = 0
        self._scroll_flag = False
        self._warehouse = []
        self._warehouse_dict = {}
        self._last_row_in_warehouse_dict = -1
        self._warehouse_dict_keys = []

        self._params = None
        self._parsed_statement = None
        self._cursor_type = CursorType.DEFAULT
        self._statementType = StatementType.UPDATE  # default
        self._paramInfo = None
        self.statement = None
        self.statementFeatureOption = Feature.optionNone
        self._statement_id = None
        self._sqlcode = None
        self._current_wire = None
        self._result_set = []
        self._rs_index = -1

        self._parameter_sets = 0
        self._exec_params = None
        self._is_batch_update = False
        self._multiple_result_sets = False
        self._mrs_done = False
        self._fetch_done = False
        self._parameter_list_mismatch_exception = False
        if self._output_parameter_list != None:
            self._output_parameter_list._clear_list()

        self._closed = True

    def _cleanup(self):
        if self._rsrow != None:
            self._rsrow = None
        if self._params != None:
            self._params._clear()
        self._multiple_result_sets = False
        self._mrs_done = False
        self._fetch_done = False
        self._parameter_list_mismatch_exception = False
        self._parameter_sets = 0
        self._rowcount = -1
        self._exec_params = None
        self._statementType = StatementType.UPDATE
        self.statementFeatureOption = 0
        self.maxRowItemCount = 0
        self._is_batch_update = False

    def _is_alive(self):
        if self._closed:
            raise InterfaceError("Cursor is closed")
        if self._connection == None or self._connection.isClosed():
            raise InterfaceError("Connection not open")

    def direct_execute(self, operation, *params):
        self._is_alive()

        self.statement = operation
        if len(params) == 1 and (isinstance(params[0], tuple) or isinstance(params[0], list)):
            self.params = params[0]
        else:
            self.params = params
        self._params.set_input_params(self.params)

        self._cursor_type = CursorType.DEFAULT
        self._cleanup()
        self._preparse()

        self._execute()
        return self._rowcount

    def execute(self, operation, params=()):
        self._is_alive()

        self.statement = operation
        if params and not isinstance(params, list) and not isinstance(params, tuple):
            params = (params,)
        self.params = params if params is not None else ()
        self._params.set_input_params(self.params)

        self._cleanup()
        try:
            self._preparse()
        except MultiValuesInsert as ex:
            # convert to executemany
            params = params or ex.params
            params_count = int(len(params) / ex.rows)
            new_params = [params[i : i + params_count] for i in range(0, len(params), params_count)]
            return self.executemany(ex.query, new_params)
        except Exception:
            raise

        if self._statementType == StatementType.UPDATE:
            self._cursor_type = CursorType.PREPARED
            self._prepare()
        else:
            self._cursor_type = CursorType.DEFAULT

        self._execute()
        return self._rowcount

    def add_batch(self):
        self._is_alive()

        if self._params._array_bound:
            if len(self._params._params_list) > 0:
                cnt = 0
                first = True
                for i in range(self._params._user_parameters_size):
                    i = i + 1
                    cnt = len(self._params._get_user_param(i)._values)
                    if cnt > 1:
                        if first:
                            self._parameter_sets = cnt
                            first = False
                        elif self._parameter_sets != cnt:
                            raise Exception(
                                "Unmatched columnwise parameter values: "
                                + str(self._parameter_sets)
                                + " rows expected, but found only "
                                + str(cnt)
                                + " in "
                                + str(i)
                                + " parameter!"
                            )
                if self._parameter_sets > 1:
                    return
        self._parameter_sets = self._parameter_sets + 1

        for param in self._params._params_list:
            if param.mode != ParameterMode.REPLACED_LITERAL:
                if len(param._values) != self._parameter_sets:
                    if self._parameter_sets != 1:
                        if len(param._values) + 1 == self._parameter_sets:
                            param._values.append(param._values[len(param._values) - 1])
                            continue

    def executemany(self, operation, seq_of_params):
        self._is_alive()
        self._rowcount = 0

        if not isinstance(seq_of_params, tuple) and not isinstance(seq_of_params, list):
            seq_of_params = tuple(seq_of_params)

        self.statement = operation
        self.params = copy.deepcopy(seq_of_params)
        self._params.set_input_params(self.params)

        self._cursor_type = CursorType.PREPARED
        self._cleanup()
        self._is_batch_update = True

        self._preparse()
        self._prepare()

        for row_num, param_row in enumerate(seq_of_params):
            self.add_batch()

        if self._parameter_sets == 0:
            for param in self._params._params_list:
                if param.value == "?":
                    raise ValueError("Missing value")
            self._prepared_update_execute()  # similar to executing a statement w/ literals
            return

        for param in self._params._params_list:
            mode = param.mode
            if mode == ParameterMode.INPUT_OUTPUT or mode == ParameterMode.OUTPUT:
                raise ValueError("INOUT/OUT parameters not permitted")

        self._prepared_update_execute()

        return self._rowcount

    def _process_sqlcode(self, sqlcode, message=None):
        self._sqlcode = sqlcode
        if sqlcode in [0, 100]:
            return
        if abs(sqlcode) in [108, 119, 121, 122]:
            raise IntegrityError(message)
        if abs(sqlcode) in [1, 12]:
            raise OperationalError(message)
        raise DatabaseError(message)

    def _preparse(self):
        csql = self._connection._pre_preparse_cache.get(self.statement)
        if csql is not None:
            self._has_return_value = csql._has_return_value
            self._params = copy.deepcopy(csql._params)
            self._params.set_input_params(self.params)
            self._parsed_statement = csql._parsed_statement
            self._statementType = csql._statementType
            self._paramInfo = csql._paramInfo
            return

        count = 0
        for i, item in enumerate(self.params):
            if isinstance(item, list) or isinstance(item, tuple):
                if not self._is_batch_update:
                    raise TypeError("Unsupported argument type: " + str(type(item)))
                for ele in item:
                    if not intersystems_iris._DBList._DBList._set_switcher.get(type(ele), None) and not issubclass(
                        type(ele), enum.Enum
                    ):
                        raise TypeError("Unsupported argument type: " + str(type(ele)))
            elif intersystems_iris._DBList._DBList._set_switcher.get(type(item), None) is None:
                item = str(item)
                # raise TypeError("Unsupported argument type: " + str(type(item)))
            if i == 0:
                count = len(item) if isinstance(item, list) or isinstance(item, tuple) else 1
            else:
                curr_count = len(item) if isinstance(item, list) or isinstance(item, tuple) else 1
                if count != curr_count:
                    raise Exception("Parameter count does not match")

        parser = intersystems_iris.dbapi.preparser._PreParser._PreParser(
            self._connection._connection_info._delimited_ids, embedded=self.embedded
        )
        try:
            pOut = parser.PreParse(self.statement, self._params)
        except MultiValuesInsert:
            raise
        except Exception as e:
            raise InterfaceError("Error parsing statement '" + self.statement + "':\n" + str(e))

        if len(self.params) > 0:
            item = self.params[0]
            if (isinstance(item, list) or isinstance(item, tuple)) and not self._is_batch_update:
                raise TypeError("Unsupported argument type: " + str(type(item)))

        self._parsed_statement = pOut.sResult
        self._statementType = pOut.p_eStmtType
        self._paramInfo = parser.m_ParamInfo

        if self._statementType == StatementType.CALL:
            self._has_return_value = ServerReturnType.NO_RETURN_VALUE
        elif self._statementType == StatementType.CALLWITHRESULT:
            self._has_return_value = ServerReturnType.HAS_RETURN_VALUE

        self._update_parameters()
        self._connection._add_pre_preparse_cache(self.statement, self)

    def _prepare(self):
        notDDL = bool(
            self._statementType != StatementType.DDL_ALTER_DROP and self._statementType != StatementType.DDL_OTHER
        )

        if notDDL and self._get_cached_info():
            return
        else:
            self._prepare_new()

    def _update_parameters(self):
        count = self._paramInfo._list_data[0]  # self._paramInfo.count()
        if count == 0:
            return

        temp_list_data = self._paramInfo._list_data[1:]
        param_info_count = int(len(temp_list_data) / 2)
        if self._is_batch_update:
            unknown_count = replaced_count = 0
            for item in temp_list_data:
                if item == "c":
                    replaced_count = replaced_count + 1
                elif item == "?":
                    unknown_count = unknown_count + 1

            if len(self.params) > 0:
                item = self.params[0]
                param_count = len(item) if isinstance(item, list) or isinstance(item, tuple) else len(self.params)
                if param_count != unknown_count:
                    raise Exception(f"Parameter mismatch: {param_count}/{unknown_count}")
        else:
            if self._cursor_type == CursorType.CALLABLE:
                i = 0
                for param in self._params._params_list:
                    if param.mode == ParameterMode.RETURN_VALUE:
                        continue
                    else:
                        if len(self._params._params_list) == len(self.params) and self.params[i] == None:
                            param.mode = ParameterMode.OUTPUT
                        if len(self._params._params_list) > len(self.params):
                            if i >= len(self.params):
                                param.mode = ParameterMode.OUTPUT
                            else:
                                if self.params[i] == None:
                                    param.mode = ParameterMode.OUTPUT
                        i += 1
                return

            if len(temp_list_data) > 0:
                if count != param_info_count:
                    raise Exception("Parameter mismatch")

                unknown_count = replaced_count = 0
                for item in temp_list_data:
                    if item == "c":
                        replaced_count = replaced_count + 1
                    elif item == "?":
                        unknown_count = unknown_count + 1

                if unknown_count != len(self.params):
                    raise Exception(
                        f"Incorrect number of parameters: {unknown_count}/{replaced_count}/{len(self.params)}"
                    )

    def _is_not_default_or_replaced(self, param):
        mode = param.mode
        if (
            mode != ParameterMode.REPLACED_LITERAL
            and mode != ParameterMode.DEFAULT_PARAMETER
            and mode != ParameterMode.INPUT
        ):
            raise Exception("Parameters not allowed in Cursor class")

    def _validate_parameters(self):
        if self._parameter_list_mismatch_exception and not self._params._has_bound_by_param_name:
            raise Exception("Parameter list mismatch")
        for param in self._params._params_list:
            self._is_not_default_or_replaced(param)

    def _validate_prepared_parameters(self):
        if self._parameter_list_mismatch_exception and not self._params._has_bound_by_param_name:
            raise Exception("Parameter list mismatch")
        i = 0
        if (
            self._has_return_value == ServerReturnType.IGNORE_RETURN_VALUE
            or self._has_return_value == ServerReturnType.NULL_RETURN_VALUE
        ):
            i = 1
        for param in self._params._params_list:
            if i == 1:
                i = 0
                continue
            if param.mode == ParameterMode.UNKNOWN:
                if self._params._has_named_parameters():
                    param.mode = ParameterMode.DEFAULT_PARAMETER
                else:
                    raise Exception("Not all parameters bound/registered")

    def _execute(self):
        if self._closed:
            raise InterfaceError("Cursor is closed")
        if self._connection == None or self._connection.isClosed():
            raise InterfaceError("Connection not open")

        exec_switcher = {
            StatementType.QUERY: self._execute_query,
            StatementType.CALL: self._execute_update,
            StatementType.STMT_USE: self._execute_update,
            StatementType.UPDATE: self._execute_update,
            StatementType.DDL_OTHER: self._execute_update,
            StatementType.DDL_ALTER_DROP: self._execute_update,
        }
        exec_func = exec_switcher.get(self._statementType, None)
        if exec_func is None:
            raise NotImplementedErrorDBAPI(f"StatementType {self._statementType.name} not implemented")
        else:
            return exec_func()

    def _prepare_stored_procedure(self):
        if self._get_cached_info():
            self._prepared_stored_procedure_execute()
        pass

    def _execute_stored_procedure(self):
        if self._cursor_type == CursorType.DEFAULT:
            if self._get_cached_info():
                # found in client side cache - send SQ message
                self._stored_procedure_update()
            else:
                # not found in client side cache - send DS message
                self._send_direct_stored_procedure_request()
        else:
            self._stored_procedure_update()

    def _execute_query(self):
        self._fetch_done = False
        if self._cursor_type == CursorType.DEFAULT:
            if self._statementType not in [StatementType.QUERY, StatementType.CALL, StatementType.CALLWITHRESULT]:
                raise Exception("Not a query")

            if self._exec_params != None:
                self._prepare_stored_procedure()
                self._bind_exec_params()
                if self._statementType in [StatementType.DIRECT_CALL_UPDATE, StatementType.PREPARED_CALL_UPDATE]:
                    raise Exception("Not a query")
                elif self._statementType in [StatementType.DIRECT_CALL_QUERY, StatementType.PREPARED_CALL_QUERY]:
                    self._stored_procedure_query()
                return

            self._validate_parameters()

            if self._statementType in [StatementType.CALL, StatementType.CALLWITHRESULT]:
                self._execute_stored_procedure()
                if not (self._statementType in [StatementType.DIRECT_CALL_QUERY, StatementType.PREPARED_CALL_QUERY]):
                    raise Exception("Not a query")
                return

            if self._get_cached_info():
                # found in client side cache - send PQ message
                self._prepared_query_execute()
            else:
                # not found in client side cache - send DQ message
                self._send_direct_query_request()
        else:
            if (
                self._statementType != StatementType.QUERY
                and self._statementType != StatementType.PREPARED_CALL_QUERY
                and self._statementType != StatementType.DIRECT_CALL_QUERY
            ):
                raise Exception("Not a query")

            if self._exec_params != None:
                self._bind_exec_params()
            self._validate_prepared_parameters()

            if self.statementFeatureOption & Feature.optionFastSelect == Feature.optionFastSelect:
                self._rsrow = _ResultSetRow(self._connection, self._columns, self.maxRowItemCount)
            else:
                self._rsrow = _ResultSetRow(self._connection, self._columns, 0)

            if self._cursor_type == CursorType.CALLABLE or self._statementType == StatementType.PREPARED_CALL_QUERY:
                self._stored_procedure_query()
                return
            self._prepared_query_execute()

    def _execute_update(self):
        if self._cursor_type == CursorType.DEFAULT:
            if self._statementType == StatementType.QUERY:
                raise Exception("Not an update")

            if self._exec_params != None:
                self._prepare_stored_procedure()
                self._bind_exec_params()
                if self._statementType in [StatementType.DIRECT_CALL_UPDATE, StatementType.PREPARED_CALL_UPDATE]:
                    self._stored_procedure_query()
                elif self._statementType in [StatementType.DIRECT_CALL_QUERY, StatementType.PREPARED_CALL_QUERY]:
                    raise Exception("Not an update")
                return

            self._validate_parameters()

            if self._statementType == StatementType.CALL:
                self._execute_stored_procedure()
                if self._statementType == StatementType.DIRECT_CALL_QUERY:
                    raise Exception("Not an update")
                return

            notDDL = bool(
                self._statementType != StatementType.DDL_ALTER_DROP and self._statementType != StatementType.DDL_OTHER
            )

            if notDDL and self._get_cached_info():
                # found in client side cache - send PU message
                self._prepared_update_execute()
            else:
                # not found in client side cache - send DU message
                self._send_direct_update_request()
        else:
            if self._statementType == StatementType.QUERY or self._statementType == StatementType.PREPARED_CALL_QUERY:
                raise Exception("Not an update")

            if self._exec_params != None:
                self._bind_exec_params()
            self._validate_prepared_parameters()

            if (
                self._cursor_type == CursorType.CALLABLE
                or self._statementType == StatementType.PREPARED_CALL_UPDATE
                or self._statementType == StatementType.DIRECT_CALL_UPDATE
            ):
                self._stored_procedure_update()
                return

            self._prepared_update_execute()

    def _query404(self):
        with self._connection._lock:
            self._validate_parameters()
            self._send_direct_query_request()

    def _update404(self):
        with self._connection._lock:
            # self._reset_cached_info()
            self._validate_parameters()
            # self._prepare()
            # self._validate_prepared_parameters()
            # self._prepared_update_execute()
            self._send_direct_update_request()

    # api properties and methods
    @property
    def description(self):
        if self._statementType is StatementType.UPDATE:
            return None

        if self._columns is None:
            return None

        Column = namedtuple(
            "Column",
            [
                "name",
                "type_code",
                "display_size",
                "internal_size",
                "precision",
                "scale",
                "null_ok",
            ],
        )

        sequence = []
        for column in self._columns:
            sequence.append(
                Column(
                    column.name,
                    column.type,
                    None,
                    None,
                    column.precision,
                    column.scale,
                    column.nullable,
                )
            )
        return tuple(sequence)

    # currently doesn't work for queries
    @property
    def rowcount(self):
        return self._rowcount


# Cursor class
class Cursor(_BaseCursor):
    def __init__(self, connection):
        super().__init__(connection)

        self._columns = None
        self._rowcount = -1
        self.arraysize = 1

        self._in_message = intersystems_iris._InStream._InStream(connection)
        self._out_message = intersystems_iris._OutStream._OutStream(connection)
        self.statementFeatureOption = 0

        self._result_set = None

        self._rsrow = None
        self._rownumber = 0
        self._cursor_ptr = 0
        self._scroll_flag = False
        self._warehouse = list()

        self._warehouse_dict = dict()
        self._last_row_in_warehouse_dict = -1
        self._warehouse_dict_keys = list()

        self.maxRowItemCount = 0

        self._parameter_sets = 0
        self._exec_params = None
        self._params = intersystems_iris.dbapi._ParameterCollection._ParameterCollection()
        self._is_batch_update = False

        self._exec_params = None
        self._multiple_result_sets = False
        self._mrs_done = False
        self._has_return_value = 0
        self._cursor_type = 0
        self._parameter_list_mismatch_exception = False
        self._fetch_done = False
        self._output_parameter_list = None

        self._lastrowid = None

        self._closed = False

    def _process_sqlcode(self, sqlcode, message=None):
        if sqlcode in [0, 100]:
            return
        super()._process_sqlcode(sqlcode, self._get_error_info(sqlcode))

    def _get_cached_info(self):
        if not self._connection._preparedCache or not hasattr(self._connection._preparedCache, "__iter__"):
            return False
        if self._parsed_statement in self._connection._preparedCache:
            self._prepare_cached(self._connection._preparedCache[self._parsed_statement])
            return True
        else:
            return False

    def _reset_cached_info(self):
        if not self._connection._preparedCache or not hasattr(self._connection._preparedCache, "__iter__"):
            return
        if self._parsed_statement in self._connection._preparedCache:
            del self._connection._preparedCache[self._parsed_statement]

    def _prepare_cached(self, cached_statement):
        self._statement_id = cached_statement.statement_id

        if self._statementType == StatementType.CALL or self._statementType == StatementType.CALLWITHRESULT:
            if len(self._params._params_list) != len(cached_statement._params._params_list):
                if (
                    self._statementType == StatementType.CALL
                    and self._has_return_value == 0
                    and cached_statement._has_return_value == 1
                    and len(self._params._params_list) + 1 == len(cached_statement._params._params_list)
                ):
                    self._params._params_list.insert(
                        0, intersystems_iris.dbapi._Parameter._Parameter(None, ParameterMode.OUTPUT, "c")
                    )
                else:
                    if len(self._params._params_list) == 0 or len(self._params._params_list) == 1:
                        self._params._clear()
                    else:
                        return False

            if cached_statement._statementType == StatementType.QUERY:
                self._statementType = StatementType.PREPARED_CALL_QUERY
            else:
                if cached_statement._statementType == StatementType.UPDATE:
                    self._statementType = StatementType.PREPARED_CALL_UPDATE
                else:
                    self._statementType = cached_statement._statementType

            self._has_return_value = cached_statement._has_return_value
            self._multiple_result_sets = cached_statement.multiple_result_sets
            self._mrs_done = False

            if not self._multiple_result_sets and (
                self._statementType
                in [StatementType.QUERY, StatementType.PREPARED_CALL_QUERY, StatementType.DIRECT_CALL_QUERY]
            ):
                self._columns = []
                for column in cached_statement.columns:
                    self._columns.append(column.Clone())

                if self.statementFeatureOption & Feature.optionFastSelect == Feature.optionFastSelect:
                    self._rsrow = _ResultSetRow(self._connection, self._columns, cached_statement.maxRowItemCount)
                else:
                    self._rsrow = _ResultSetRow(self._connection, self._columns, 0)

        else:
            if self._statementType == StatementType.QUERY:
                self._columns = []
                for column in cached_statement.columns:
                    self._columns.append(column.Clone())

                if self.statementFeatureOption & Feature.optionFastSelect == Feature.optionFastSelect:
                    self._rsrow = _ResultSetRow(self._connection, self._columns, cached_statement.maxRowItemCount)
                else:
                    self._rsrow = _ResultSetRow(self._connection, self._columns, 0)

        if hasattr(cached_statement, "statementFeatureOption"):
            self.statementFeatureOption = cached_statement.statementFeatureOption
        if hasattr(cached_statement, "maxRowItemCount"):
            self.maxRowItemCount = cached_statement.maxRowItemCount
        if hasattr(cached_statement, "_params"):
            self._params._update_param_info(cached_statement._params)

    def _prepare_new(self):
        # send PP message
        with self._connection._lock:
            # message header
            self._statement_id = self._connection._get_new_statement_id()
            self._out_message.wire._write_header(_Message.PREPARE)
            intersystems_iris._MessageHeader._MessageHeader._set_statement_id(
                self._out_message.wire.buffer, self._statement_id
            )

            # message body
            self._out_message.wire._set(1)  # number of statement chunks
            self._out_message.wire._set(self._parsed_statement)  # statement itself
            self._out_message.wire._set_raw_bytes(self._paramInfo.getBuffer())  # paramInfo (from _PreParser)

            # send message
            sequence_number = self._connection._get_new_sequence_number()
            self._out_message._send(sequence_number)

            # retrieve response
            self._in_message._read_message_sql(sequence_number, self._statement_id, 0, [0])
            sqlcode = self._in_message.wire.header._get_function_code()
            if sqlcode not in [0, 100]:
                raise DatabaseError(self._get_error_info(sqlcode))

        # process metadata
        try:
            if self._connection._isFastOption():
                self._check_statement_feature(self._in_message.wire)
            else:
                self.statementFeatureOption = Feature.optionNone

            if self._statementType == StatementType.QUERY:
                self._get_column_info(self._in_message.wire)
            else:
                self._columns = None

            addToCache = self._get_parameter_info(self._in_message.wire)
            if addToCache:
                self._cache_prepared_statement()
        except IndexError:
            raise DatabaseError("Server response message terminated prematurely")
        except TypeError:
            raise DatabaseError("Unexpected server response message format")

    def _get_error_info(self, sqlcode):
        with self._connection._lock:
            self._out_message.wire._write_header(_Message.GET_SERVER_ERROR)
            self._out_message.wire._set(sqlcode)

            sequence_number = self._connection._get_new_sequence_number()
            self._out_message._send(sequence_number)

            self._in_message._read_message_sql(sequence_number)
            return self._in_message.wire._get()

    def _check_statement_feature(self, wire):
        self.statementFeatureOption = wire._get()
        if (
            self.statementFeatureOption == Feature.optionFastSelect
            or self.statementFeatureOption == Feature.optionFastInsert
        ):
            self.maxRowItemCount = wire._get()
        else:
            self.maxRowItemCount = 0

    def _get_column_info(self, wire):
        self._columns = []
        count = wire._get()
        for i in range(count):
            name = wire._get()
            type = wire._get()
            precision = wire._get()
            scale = wire._get()
            nullable = wire._get()
            label = wire._get()
            tableName = wire._get()
            schema = wire._get()
            catalog = wire._get()
            if catalog == 0:
                catalog = None
            additionalData = wire._get().encode()
            slotPosition = (
                wire._get()
                if self.statementFeatureOption & Feature.optionFastSelect == Feature.optionFastSelect
                else i + 1
            )
            self._columns.append(
                intersystems_iris.dbapi._Column._Column(
                    name,
                    type,
                    precision,
                    scale,
                    nullable,
                    label,
                    tableName,
                    schema,
                    catalog,
                    additionalData,
                    slotPosition,
                )
            )

    def _get_parameter_info(self, wire):
        count = wire._get()
        if count != len(self._params._params_list):
            raise Exception("Invalid number of parameters")
        self._read_parameter_data(wire, count, False)

        addToCache = bool(wire._get())
        return addToCache

    def _read_parameter_data(self, wire, count, is_stored_procedure):
        if count != len(self._params._params_list):
            raise Exception("Invalid number of parameters")
        with self._connection._lock:
            r = 0
            user_param_count = 0
            if self._has_return_value == ServerReturnType.NULL_RETURN_VALUE:
                r += 1
            if self._has_return_value == ServerReturnType.IGNORE_RETURN_VALUE:
                user_param_count -= 1

            for i in range(count):
                param = self._params._params_list[i + r]
                param.type = wire._get()
                param.precision = wire._get()
                param.scale = wire._get()
                param.nullable = wire._get()

                if self.statementFeatureOption & Feature.optionFastInsert == Feature.optionFastInsert:
                    param.slotPosition = wire._get()
                    self.rowOfDefaultValues = wire._get()  # needs to be processed
                else:
                    param.slotPosition = i + r

                if is_stored_procedure:
                    param.name = wire._get()
                    wire._get()

            if is_stored_procedure:
                self._params._update_names()

    def _execute_update(self):
        super()._execute_update()

        if self._parameter_sets == 0 and not self._multiple_result_sets:
            self._rowcount = self._in_message.wire._get()

    class prepared_statement(intersystems_iris._IRISConnection.CachedSQL):
        def __init__(self, cursor):
            if not isinstance(cursor, Cursor):
                raise TypeError("cursor must be a Cursor")

            super().__init__(cursor)
            self.statement = cursor._parsed_statement
            self.statement_id = cursor._statement_id

            if cursor._columns != None:
                self.columns = []
                for column in cursor._columns:
                    self.columns.append(column.Clone())

            if hasattr(cursor, "statementFeatureOption"):
                self.statementFeatureOption = cursor.statementFeatureOption
            if hasattr(cursor, "maxRowItemCount"):
                self.maxRowItemCount = cursor.maxRowItemCount

            self.multiple_result_sets = cursor._multiple_result_sets

    def _cache_prepared_statement(self):
        self._connection._cache_prepared_statement(self.prepared_statement(self))

    def _write_parameters(self):
        sets = self._parameter_sets or 1

        self._out_message.wire._set(sets)  # nParamSets
        self._out_message.wire._set(len(self._params._params_list))  # nParams
        for i in range(sets):
            param_index = 0
            param_counter = i
            for param in self._params._params_list:
                mode = param.mode
                if mode == ParameterMode.REPLACED_LITERAL:
                    self._out_message.wire._set_parameter(param)
                elif not mode == ParameterMode.INPUT:
                    temp_param = intersystems_iris.dbapi._Parameter._Parameter(param._values[i])
                    self._out_message.wire._set_parameter(temp_param)
                elif len(self.params) > 0:
                    item = self.params[param_counter]
                    if isinstance(item, list) or isinstance(item, tuple):
                        value = item[param_index]
                        param_index = param_index + 1
                    else:
                        value = item
                        param_counter = param_counter + 1
                    temp_param = intersystems_iris.dbapi._Parameter._Parameter(value)
                    self._out_message.wire._set_parameter(temp_param)
                else:
                    raise Exception("Missing value")

    def _prepared_query_execute(self):
        # send PQ message
        with self._connection._lock:
            # message header
            self._out_message.wire._write_header(_Message.PREPARED_QUERY_EXECUTE)
            intersystems_iris._MessageHeader._MessageHeader._set_statement_id(
                self._out_message.wire.buffer, self._statement_id
            )

            # message body
            self._write_parameters()
            self._out_message.wire._set(0)  # query timeout
            self._out_message.wire._set(0)  # maxRows (0 = all rows)

            # send
            sequence_number = self._connection._get_new_sequence_number()
            self._out_message._send(sequence_number)

            # retrieve response
            self._in_message._read_message_sql(sequence_number, self._statement_id, _InStream.FETCH_DATA, [404, 100])
            self._sqlcode = self._in_message.wire.header._get_function_code()
            self._handle_error_504(self._sqlcode)
            if self._sqlcode == 404:
                return
            self._process_sqlcode(self._sqlcode)

        self._current_wire = self._in_message.wire
        self._result_set = [self._current_wire]
        self._rs_index = 0

        self._rowcount = -1

    def _send_direct_stored_procedure_request(self):
        # self._has_return_value = ServerReturnType.NO_RETURN_VALUE
        # send DS message
        with self._connection._lock:
            # message header
            self._statement_id = self._connection._get_new_statement_id()
            self._out_message.wire._write_header(_Message.DIRECT_STORED_PROCEDURE)
            intersystems_iris._MessageHeader._MessageHeader._set_statement_id(
                self._out_message.wire.buffer, self._statement_id
            )

            # message body
            self._out_message.wire._set(self._parsed_statement)  # statement itself
            self._out_message.wire._set(0)  # resultSetType != ResultSet.TYPE_SCROLL_INSENSITIVE
            self._out_message.wire._set(0)  # query timeout
            self._out_message.wire._set(0)  # maxRows (0 = all rows)
            self._write_stored_procedure_parameters()

            # send
            sequence_number = self._connection._get_new_sequence_number()
            self._out_message._send(sequence_number)

            try:
                # retrieve metadata
                self._in_message._read_message_sql(sequence_number, self._statement_id, 0, [100])
                sqlcode = self._in_message.wire.header._get_function_code()
                if sqlcode not in [0, 100]:
                    raise DatabaseError(self._get_error_info(sqlcode))

                self._process_stored_procedure_metadata(self._in_message.wire, True)
                if self._multiple_result_sets:
                    # todo
                    return

                self._cache_prepared_statement()

                if self._statementType in [
                    StatementType.UPDATE,
                    StatementType.DIRECT_CALL_UPDATE,
                    StatementType.PREPARED_CALL_UPDATE,
                ]:
                    return False

            except IndexError:
                raise DatabaseError("Server response message terminated prematurely")
            except TypeError:
                raise DatabaseError("Unexpected server response message format")

    def _prepared_stored_procedure_execute(self):
        # send SU message
        with self._connection._lock:
            # message header
            self._out_message.wire._write_header(_Message.PREPARED_UPDATE_EXECUTE)
            intersystems_iris._MessageHeader._MessageHeader._set_statement_id(
                self._out_message.wire.buffer, self._statement_id
            )

    def _send_direct_query_request(self):
        # send DQ message
        with self._connection._lock:
            # message header
            self._statement_id = self._connection._get_new_statement_id()
            self._out_message.wire._write_header(_Message.DIRECT_QUERY)
            intersystems_iris._MessageHeader._MessageHeader._set_statement_id(
                self._out_message.wire.buffer, self._statement_id
            )

            # message body
            self._out_message.wire._set(1)  # number of statement chunks
            self._out_message.wire._set(self._parsed_statement)  # statement itself
            self._out_message.wire._set_raw_bytes(self._paramInfo.getBuffer())  # paramInfo (from _PreParser)
            self._write_parameters()
            self._out_message.wire._set(0)  # query timeout
            self._out_message.wire._set(0)  # maxRows (0 = all rows)

            # send
            sequence_number = self._connection._get_new_sequence_number()
            self._out_message._send(sequence_number)

            try:
                # retrieve metadata
                self._in_message._read_message_sql(sequence_number, self._statement_id, 0, [100])
                sqlcode = self._in_message.wire.header._get_function_code()
                self._process_sqlcode(sqlcode)

                if self._connection._isFastOption():
                    self._check_statement_feature(self._in_message.wire)
                else:
                    self.statementFeatureOption = Feature.optionNone
                self._get_column_info(self._in_message.wire)
                self._get_parameter_info(self._in_message.wire)
                self._cache_prepared_statement()

                # retrieve data
                self._in_message._read_message_sql(sequence_number, self._statement_id, _InStream.FETCH_DATA, [100])
                self._sqlcode = self._in_message.wire.header._get_function_code()
                if self._sqlcode not in [0, 100]:
                    raise DatabaseError(self._get_error_info(self._sqlcode))

            except IndexError:
                raise DatabaseError("Server response message terminated prematurely")
            except TypeError:
                raise DatabaseError("Unexpected server response message format")

        self._current_wire = self._in_message.wire
        self._result_set = [self._current_wire]
        self._rs_index = 0

        self._rowcount = -1
        if self.statementFeatureOption & Feature.optionFastSelect == Feature.optionFastSelect:
            self._rsrow = _ResultSetRow(self._connection, self._columns, self.maxRowItemCount)
        else:
            self._rsrow = _ResultSetRow(self._connection, self._columns, 0)

    def _update_streams(self):
        sets = self._parameter_sets or 1
        self.params = list(self.params).copy()
        param_types = [param.type for param in self._params._params_list]
        if not self.params:
            return

        for i in range(sets):
            params = self._params.collect(i)
            for pi, param in enumerate(params):
                if param_types[pi] in (SQLType.LONGVARBINARY, SQLType.LONGVARCHAR) and param is not None:
                    stream_oref = self._send_stream(param_types[pi], param)
                    if isinstance(self.params[i], tuple):
                        self.params[i] = list(self.params[i])
                    if isinstance(self.params[i], list):
                        self.params[i][pi] = stream_oref
                    else:
                        self.params[pi] = stream_oref

    def _send_stream(self, param_type, value):
        if isinstance(value, _IRISOREF):
            return value
        if not isinstance(value, str) and not isinstance(value, bytes):
            raise Exception(f"Invalid value type for stream, got {type(value).__name__}, expected str or bytes")
        stream_oref = None
        offset = 0
        full_size = len(value)
        if full_size < 3 * 1024 * 1024:
            return value
        with self._connection._lock:
            while True:
                size = full_size
                if size == 0:
                    break
                size = 4096 if size > 4096 else size
                chunk = value[offset : offset + size]
                if not isinstance(chunk, bytes):
                    chunk = bytes(chunk, "utf-8")
                offset += size
                full_size -= size

                # message header
                code = (
                    _Message.STORE_BINARY_STREAM
                    if param_type == SQLType.LONGVARBINARY
                    else _Message.STORE_CHARACTER_STREAM
                )
                self._out_message.wire._write_header(code)
                intersystems_iris._MessageHeader._MessageHeader._set_statement_id(
                    self._out_message.wire.buffer, self._statement_id
                )
                # message body
                self._out_message.wire._set(stream_oref or "0")
                self._out_message.wire._set_raw_bytes(struct.pack("<i", size))
                self._out_message.wire._set_raw_bytes(chunk)

                # send
                sequence_number = self._connection._get_new_sequence_number()
                self._out_message._send(sequence_number)

                self._in_message._read_message_sql(sequence_number, self._statement_id, 0)
                stream_oref = self._sqlcode = self._in_message.wire._get()

        return _IRISOREF(stream_oref)

    def _prepared_update_execute(self):
        self._update_streams()

        # send PU message
        with self._connection._lock:
            # message header
            self._out_message.wire._write_header(_Message.PREPARED_UPDATE_EXECUTE)
            intersystems_iris._MessageHeader._MessageHeader._set_statement_id(
                self._out_message.wire.buffer, self._statement_id
            )

            # message body
            self._lastrowid = None
            self._out_message.wire._set(-1)  # autoGeneratedKeyColumn
            self._out_message.wire._set(0)  # statement timeout always 0 for non-queries
            self._write_parameters()

            # send
            sequence_number = self._connection._get_new_sequence_number()
            self._out_message._send(sequence_number)

            # retrieve response
            self._in_message._read_message_sql(sequence_number, self._statement_id, 0, [404, 100])
            self._sqlcode = self._in_message.wire.header._get_function_code()
            if self._sqlcode == 404:
                self._update404()
                return
            self._process_sqlcode(self._sqlcode)

    def _send_direct_update_request(self):
        # send DU message
        with self._connection._lock:
            self._statement_id = self._connection._get_new_statement_id()
            # message header
            self._out_message.wire._write_header(_Message.DIRECT_UPDATE)
            intersystems_iris._MessageHeader._MessageHeader._set_statement_id(
                self._out_message.wire.buffer, self._statement_id
            )

            # message body
            self._lastrowid = None
            self._out_message.wire._set(1)  # number of statement chunks
            self._out_message.wire._set(self._parsed_statement)  # statement itself
            self._out_message.wire._set_raw_bytes(self._paramInfo.getBuffer())  # paramInfo (from _PreParser)
            self._out_message.wire._set(-1)  # autoGeneratedKeyColumn
            self._out_message.wire._set(0)  # statement timeout always 0 for non-queries
            self._write_parameters()

            # send
            sequence_number = self._connection._get_new_sequence_number()
            self._out_message._send(sequence_number)

            try:
                # retrieve response
                self._in_message._read_message_sql(sequence_number, self._statement_id, 0, [100])
                self._sqlcode = self._in_message.wire.header._get_function_code()
                self._process_sqlcode(self._sqlcode)

                addToCache = self._get_parameter_info(self._in_message.wire)

                notDDL = bool(
                    self._statementType != StatementType.DDL_ALTER_DROP
                    and self._statementType != StatementType.DDL_OTHER
                )
                if notDDL and addToCache:
                    self._cache_prepared_statement()

            except IndexError:
                raise DatabaseError("Server response message terminated prematurely")
            except TypeError:
                raise DatabaseError("Unexpected server response message format")

    def _prepare_stored_procedure(self):
        with self._connection._lock:
            # message header
            self._statement_id = self._connection._get_new_statement_id()
            self._out_message.wire._write_header(_Message.PREPARE_STORED_PROCEDURE)
            intersystems_iris._MessageHeader._MessageHeader._set_statement_id(
                self._out_message.wire.buffer, self._statement_id
            )

            # message body
            # self._out_message.wire._set(1) # number of statement chunks
            self._out_message.wire._set(self._parsed_statement)  # statement itself

            # send message
            sequence_number = self._connection._get_new_sequence_number()
            self._out_message._send(sequence_number)

            # retrieve response
            self._in_message._read_message_sql(sequence_number, self._statement_id, 0, [0])
            sqlcode = self._in_message.wire.header._get_function_code()
            self._process_sqlcode(sqlcode)

            self._process_stored_procedure_metadata(self._in_message.wire, False)
            if self._multiple_result_sets:
                return
        self._cache_prepared_statement()
        return

    def _process_stored_procedure_metadata(self, wire, direct_execute):
        ret_stmt_type = wire._get()
        if ret_stmt_type < 0:
            if ret_stmt_type == -1:
                self._multiple_result_sets_metadata(wire, False, direct_execute)
            else:
                self._multiple_result_sets_metadata(wire, True, direct_execute)
            return
        if ret_stmt_type % 2 == StatementType.QUERY.value:
            self._get_column_info(wire)
        if not self._stored_procedure_parameter_info(wire, ret_stmt_type > 1, direct_execute):
            if direct_execute and ret_stmt_type % 2 == StatementType.QUERY.value:
                # result set type check
                self._in_message.wire._move_to_end()
            if direct_execute:
                raise Exception("Parameter list mismatch")
        if direct_execute and self._parameter_list_mismatch_exception:
            raise Exception("Parameter list mismatch")
        if self._cursor_type == CursorType.CALLABLE:
            self._statementType = StatementType(ret_stmt_type % 2)
            return
        if self._cursor_type == CursorType.PREPARED:
            if ret_stmt_type % 2 == StatementType.QUERY.value:
                self._statementType = StatementType.PREPARED_CALL_QUERY
            else:
                self._statementType = StatementType.PREPARED_CALL_UPDATE
            return
        if ret_stmt_type % 2 == StatementType.QUERY.value:
            self._statementType = StatementType.DIRECT_CALL_QUERY
        else:
            self._statementType = StatementType.DIRECT_CALL_UPDATE
        return

    def _multiple_result_sets_metadata(self, wire, server_has_return, direct_execute):
        self._multiple_result_sets = True
        self._mrs_done = False
        if not self._stored_procedure_parameter_info(wire, server_has_return, direct_execute):
            raise Exception("Parameter list mismatch")

        if self._cursor_type == CursorType.CALLABLE:
            self._statementType = StatementType.QUERY
        elif self._cursor_type == CursorType.PREPARED:
            self._statementType = StatementType.PREPARED_CALL_QUERY
        else:
            self._statementType = StatementType.DIRECT_CALL_QUERY

    def _stored_procedure_parameter_info(self, wire, server_has_return, direct_execute):
        count = wire._get()
        size = len(self._params._params_list)
        if self._cursor_type != CursorType.CALLABLE and self._has_return_value == 1:
            wire._move_to_end()
            return False
        if self._exec_params != None:
            while size < count:
                self._params._params_list.insert(0, intersystems_iris.dbapi._Parameter._Parameter())
                size = size + 1
            if server_has_return:
                self._has_return_value = ServerReturnType.IGNORE_RETURN_VALUE
        elif size == count:
            if server_has_return and self._has_return_value == ServerReturnType.HAS_RETURN_VALUE:
                self._has_return_value = ServerReturnType.HAS_RETURN_VALUE
            elif not server_has_return and self._has_return_value == ServerReturnType.NO_RETURN_VALUE:
                self._has_return_value = ServerReturnType.NO_RETURN_VALUE
            elif (
                size == 1
                and count == 1
                and (
                    self._params._params_list[0].mode == ParameterMode.DEFAULT_PARAMETER
                    and self._has_return_value == ServerReturnType.NO_RETURN_VALUE
                    and server_has_return
                )
                or (
                    self._params._params_list[0].mode == ParameterMode.UNKNOWN
                    and self._has_return_value == ServerReturnType.IGNORE_RETURN_VALUE
                    and server_has_return
                )
            ):
                self._params._params_list.pop(0)
                self._params._params_list.insert(0, intersystems_iris.dbapi._Parameter._Parameter())
                self._has_return_value = ServerReturnType.IGNORE_RETURN_VALUE
            else:
                wire._move_to_end()
                return False
        elif size == count + 1:
            if not server_has_return and self._has_return_value == ServerReturnType.HAS_RETURN_VALUE:
                self._has_return_value = ServerReturnType.NULL_RETURN_VALUE
            elif (size == 2 and count == 1) or (size == 1 and count == 0):
                if self._params._params_list[-1].mode == ParameterMode.DEFAULT_PARAMETER:
                    if server_has_return and self._has_return_value == ServerReturnType.HAS_RETURN_VALUE:
                        self._params._params_list.pop(-1)
                        self._has_return_value = ServerReturnType.HAS_RETURN_VALUE
                    elif not server_has_return and self._has_return_value == ServerReturnType.NO_RETURN_VALUE:
                        self._params._params_list.pop(-1)
                        self._has_return_value = ServerReturnType.NO_RETURN_VALUE
                    else:
                        wire._move_to_end()
                        return False
                else:
                    wire._move_to_end()
                    return False
            else:
                wire._move_to_end()
                return False
        elif size == count - 1:
            if server_has_return and self._has_return_value == ServerReturnType.NO_RETURN_VALUE:
                self._params._params_list.insert(0, intersystems_iris.dbapi._Parameter._Parameter())
                self._has_return_value = ServerReturnType.IGNORE_RETURN_VALUE
            else:
                self._params._params_list.append(
                    intersystems_iris.dbapi._Parameter._Parameter(ParameterMode.DEFAULT_PARAMETER, "c")
                )
        else:
            self._parameter_list_mismatch_exception = True
            if server_has_return and self._has_return_value == ServerReturnType.NO_RETURN_VALUE:
                self._has_return_value = ServerReturnType.IGNORE_RETURN_VALUE
                param = intersystems_iris.dbapi._Parameter._Parameter()
                param.mode = ParameterMode.OUTPUT
                self._params._params_list.insert(0, param)
                size = size + 1
            while size < count:
                self._params._params_list.append(
                    intersystems_iris.dbapi._Parameter._Parameter(ParameterMode.DEFAULT_PARAMETER, "c")
                )
                size = size + 1
        self._read_parameter_data(wire, count, True)
        return True

    def _bind_exec_params(self):
        for param in self._params._params_list:
            exec_param = self._get_exec_param_by_name(param.name)
            if exec_param != None:
                if exec_param.mode == ParameterMode.UNKNOWN:
                    continue
                param.mode = exec_param.mode
                if exec_param.mode != ParameterMode.OUTPUT:
                    if isinstance(exec_param.value, list):
                        self._params._array_bound = True
                    param._bind(exec_param.value, self._parameter_sets)
                else:
                    param.__bound = exec_param.bound
                if exec_param.scale != -1:
                    param.scale = exec_param.scale
            else:
                if param.mode == ParameterMode.UNKNOWN:
                    param.mode = ParameterMode.DEFAULT_PARAMETER

    def _get_exec_param_by_name(self, name):
        self._exec_params._has_bound_by_param_name = True
        return self._exec_params._params_list[self._exec_params._param_names.get(name.upper())]

    def _stored_procedure_update(self):
        with self._connection._lock:
            # message header
            self._out_message.wire._write_header(_Message.STORED_PROCEDURE_UPDATE_EXECUTE)
            intersystems_iris._MessageHeader._MessageHeader._set_statement_id(
                self._out_message.wire.buffer, self._statement_id
            )

            # message body
            self._out_message.wire._set(0)  # isStatic should always be 0 for non-queries
            self._out_message.wire._set(0)  # query timeout
            self._out_message.wire._set(0)  # maxRows (0 = all rows)
            self._write_stored_procedure_parameters()

            # send
            sequence_number = self._connection._get_new_sequence_number()
            self._out_message._send(sequence_number)

            # retrieve response
            self._in_message._read_message_sql(sequence_number, self._statement_id, 0, [404, 100])
            self._sqlcode = self._in_message.wire.header._get_function_code()
            self._process_sqlcode(self._sqlcode)
            if self._sqlcode == 404:
                self._update404(404)
            else:
                self._get_output_parameters(self._in_message.wire)
        return

    def _stored_procedure_query(self):
        if self._multiple_result_sets:
            self._execute_multiple_result_sets(False)
            return
        with self._connection._lock:
            # message header
            self._out_message.wire._write_header(_Message.STORED_PROCEDURE_QUERY_EXECUTE)
            intersystems_iris._MessageHeader._MessageHeader._set_statement_id(
                self._out_message.wire.buffer, self._statement_id
            )

            # message body
            self._out_message.wire._set(0)  # ResultSet.TYPE_SCROLL_INSENSITIVE
            self._out_message.wire._set(0)  # query timeout
            self._out_message.wire._set(0)  # maxRows (0 = all rows)
            self._write_stored_procedure_parameters()

            # send
            sequence_number = self._connection._get_new_sequence_number()
            self._out_message._send(sequence_number)

            # retrieve response
            self._in_message._read_message_sql(sequence_number, self._statement_id, 0, [404, 100])
            self._sqlcode = self._in_message.wire.header._get_function_code()
            if self._sqlcode == 404:
                self._handle_error_504(404)
                return
            elif self._sqlcode == 100:
                self._handle_error_100(100)
                return
            self._get_output_parameters(self._in_message.wire)
            self._in_message._read_message_sql(sequence_number, self._statement_id, _InStream.FETCH_DATA, [100])
            self._sqlcode = self._in_message.wire.header._get_function_code()
            self._process_sqlcode(self._sqlcode)
            if self._sqlcode == 100:
                self._handle_error_100(100)

            self._current_wire = self._in_message.wire
            self._result_set = [self._current_wire]
            self._rs_index = 0

    def _execute_multiple_result_sets(self, validate):
        self._fetch_done = False
        if validate:
            self._validate_parameters()
        with self._connection._lock:
            self._out_message.wire._write_header(_Message.EXECUTE_MULTIPLE_RESULT_SETS)
            self._out_message.wire._set(0)  # resultSetType != ResultSet.TYPE_SCROLL_INSENSITIVE
            self._out_message.wire._set(0)  # query timeout
            self._write_stored_procedure_parameters()

            # send
            sequence_number = self._connection._get_new_sequence_number()
            self._out_message._send(sequence_number)

            if self.statementFeatureOption & Feature.optionFastSelect == Feature.optionFastSelect:
                self._rsrow = _ResultSetRow(self._connection, self._columns, self.maxRowItemCount)
            else:
                self._rsrow = _ResultSetRow(self._connection, self._columns, 0)

            self._in_message._read_message_sql(sequence_number, self._statement_id, _InStream.FETCH_DATA, [100])
            self._sqlcode = self._in_message.wire.header._get_function_code()
            if self._sqlcode == 100:
                self._fetch_done = True
            self._get_output_parameters(self._in_message.wire)

            self._current_wire = self._in_message.wire
            self._result_set = [self._current_wire]
            self._rs_index = 0

            results = self._in_message.wire._get()
            if results >= 0:
                self._update_cnt = results
                return False
            elif results == -1:
                return True
            elif results == -2:
                self._update_cnt = -1
                self._mrs_done = True
                # self._rsrow._is_after_last = True
                return False
            else:
                raise Exception("Invalid result type value")

    def _write_stored_procedure_parameters(self):
        i = 0
        if self._parameter_sets != 0:
            self._out_message.wire._set(self._parameter_sets)
            self._out_message.wire._set(len(self._params._params_list))
            for j in range(self._parameter_sets):
                for i, param in enumerate(self._params._params_list):
                    self._out_message.wire._set_parameter(param._values[j])
            return
        self._out_message.wire._set(1)
        if self._has_return_value != ServerReturnType.NO_RETURN_VALUE:
            i = 1
        self._out_message.wire._set(len(self._params._params_list) - i)
        param_index = 0
        param_counter = 0
        for param in self._params._params_list:
            if i == 1:
                i = 0
                continue
            if param.mode == ParameterMode.OUTPUT or param.mode == ParameterMode.DEFAULT_PARAMETER:
                self._out_message.wire._set_undefined()
            elif not (param.mode == ParameterMode.INPUT or param.mode == ParameterMode.INPUT_OUTPUT):
                self._out_message.wire._set_parameter_type(param.type, param.value)
            elif len(self.params) > 0:
                item = self.params[param_counter]
                if isinstance(item, list) or isinstance(item, tuple):
                    value = item[param_index]
                    param_index = param_index + 1
                else:
                    value = item
                    param_counter = param_counter + 1
                self._out_message.wire._set_parameter_type(param.type, value)
            else:
                raise Exception("Missing value")

    def _get_output_parameters(self, wire):
        beg = wire._get_offset()
        i = 0
        if self._has_return_value == ServerReturnType.NULL_RETURN_VALUE:
            i += 1
        for param in self._params._params_list:
            if i == 1:
                i = 0
                continue
            if (
                param.mode == ParameterMode.INPUT_OUTPUT
                or param.mode == ParameterMode.OUTPUT
                or param.mode == ParameterMode.RETURN_VALUE
            ):
                wire._next_unless_undefined()
            else:
                wire._next()
        if self._has_return_value == ServerReturnType.NULL_RETURN_VALUE:
            self._output_parameter_list = wire._get_output_parameter_list(beg, True)
            self._has_return_value = ServerReturnType.HAS_RETURN_VALUE
        else:
            self._output_parameter_list = wire._get_output_parameter_list(beg, False)
        if self._statementType not in [StatementType.DIRECT_CALL_UPDATE]:
            self._params._prep_list_index(
                False, self._output_parameter_list
            )  # fast select not supported for stored procedures
        return

    def _handle_error_504(self, error):
        if error == 404:
            self._query404()
            return
        self._handle_error_100(error)

    def _handle_error_100(self, error):
        if error == 100:
            self._fetch_done = True
            pass
        else:
            pass

    def stored_results(self):
        if self._closed:
            raise InterfaceError("Cursor is closed")
        if self._statementType not in [
            StatementType.QUERY,
            StatementType.DIRECT_CALL_QUERY,
            StatementType.PREPARED_CALL_QUERY,
        ]:
            return None
        # getResultSet()
        if self._multiple_result_sets:
            if self._rsrow == None and self._rowcount == -1:
                return None
            if self._mrs_done:
                return None
            self._get_column_info(self._in_message.wire)
        self.nextset()
        return iter(self._stored_results)

    def nextset(self):
        if len(self._stored_results) == 0:
            # getResultSet()
            if self.statementFeatureOption & Feature.optionFastSelect == Feature.optionFastSelect:
                self._rsrow = _ResultSetRow(self._connection, self._columns, self.maxRowItemCount)
            else:
                self._rsrow = _ResultSetRow(self._connection, self._columns, 0)
            self._rsrow.indexRow(self._in_message.wire.list_item)
            self._stored_results.append(self._rsrow._offsets)
            return True
        else:
            # getMoreResults()
            if self._closed:
                raise InterfaceError("Cursor is closed")
            if self._connection == None or self._connection.isClosed():
                raise InterfaceError("Connection not open")
            if (
                self._mrs_done
                or not self._multiple_result_sets
                or (
                    self._statementType != StatementType.PREPARED_CALL_QUERY
                    and self._statementType != StatementType.DIRECT_CALL_QUERY
                    and self._statementType != StatementType.CALL
                    and self._statementType != StatementType.CALLWITHRESULT
                    and not (self._statementType == StatementType.QUERY and self._cursor_type == CursorType.CALLABLE)
                )
            ):
                return False
            with self._connection._lock:
                self._out_message.wire._write_header(_Message.GET_MORE_RESULTS)
                self._out_message.wire._set(1)  # current = CLOSE_CURRENT_RESULT

                # send
                sequence_number = self._connection._get_new_sequence_number()
                self._out_message._send(sequence_number)

                self._in_message._read_message_sql(sequence_number, self._statement_id, 0, [100])
                sqlcode = self._in_message.wire.header._get_function_code()

                self._current_wire = self._in_message.wire
                self._result_set = [self._current_wire]
                self._rs_index = 0

                results = self._in_message.wire._get()
                if results >= 0:
                    self._update_cnt = results
                    return False
                elif results == -1:
                    self._rsrow = None
                    self._get_column_info(self._in_message.wire)
                    if self.statementFeatureOption & Feature.optionFastSelect == Feature.optionFastSelect:
                        self._rsrow = _ResultSetRow(self._connection, self._columns, self.maxRowItemCount)
                    else:
                        self._rsrow = _ResultSetRow(self._connection, self._columns, 0)
                    self._rsrow.indexRow(self._in_message.wire.list_item)
                    self._stored_results.append(self._rsrow._offsets)
                    if sqlcode == 100:
                        self._fetch_done = True
                    else:
                        self._fetch_done = False
                    return True
                elif results == -2:
                    self._update_cnt = -1
                    self._mrs_done = True
                    return False
                else:
                    raise Exception("Invalid result type value")
        return

    def _get_result_set(self, oref):
        if oref == None:
            return None

        with self._connection._lock:
            self._out_message.wire._write_header(_Message.GET_RESULT_SET_OBJECT)
            self._out_message.wire._set(oref)
            self._out_message.wire._set(0)  # IRISResultSet.GET_RESULT_SET_OBJECT_INIT

            # send
            sequence_number = self._connection._get_new_sequence_number()
            self._out_message._send(sequence_number)

            # retrieve response
            error = self._in_message._read_message_sql(sequence_number, self._statement_id, 0, [100])
            if error == 100:
                self._fetch_done = True
            self._get_column_info(self._in_message.wire)
            return

    def callproc(self, procname, *params):
        if self._closed:
            raise InterfaceError("Cursor is closed")
        if self._connection == None or self._connection.isClosed():
            raise InterfaceError("Connection not open")

        self.statement = procname
        if len(params) == 1 and (isinstance(params[0], tuple) or isinstance(params[0], list)):
            self.params = self.params[0]
        else:
            self.params = params

        self._params.set_input_params(self.params)

        self._cursor_type = CursorType.CALLABLE
        self._cleanup()
        self._preparse()
        self._stored_results = []

        if not self._get_cached_info():
            self._prepare_stored_procedure()

        # execute() in IrisPreparedStatement
        if self._multiple_result_sets:
            return self._execute_multiple_result_sets(True)
        if self._statementType == StatementType.QUERY or self._statementType == StatementType.PREPARED_CALL_QUERY:
            self._execute_query()
            self._rowcount = -1
            if self._fetch_done and self._in_message.wire.header._get_message_length() == 0:
                return
            return self._process_return_values()
        self._execute_update()
        if self._parameter_sets == 0 and not self._multiple_result_sets:
            self._rowcount = self._in_message.wire._get()

        return self._process_return_values()

    def _process_return_values(self):
        return_args = []
        for i, param in enumerate(self._params._params_list):
            if param.mode in [ParameterMode.RETURN_VALUE, ParameterMode.OUTPUT, ParameterMode.INPUT]:
                offset = self._params._get_user_list_offset(i + 1)
                val = self._output_parameter_list._get_at_offset(offset)
                if param.type == -51:  # RESULT_SET_TYPE
                    self._get_result_set(val)
                    self.nextset()
                    return_args.append(self._stored_results[0])
                else:
                    if val == "\x01":  # Either represents the number 1 or a null/None value
                        # maybe move this to _grab_ascii_string in DBList?
                        off = self._output_parameter_list.list_item.data_offset
                        buf = self._output_parameter_list.list_item.buffer
                        if buf[off] == 1 and buf[off - 1] == 1 and buf[off - 2] == 3:
                            return_args.append(None)
                    else:
                        return_args.append(val)
        if len(return_args) > 0:
            if any(i != None for i in return_args):
                return return_args
            else:
                return
        else:
            return

    @property
    def lastrowid(self):
        if self._lastrowid is not None:
            return self._lastrowid

        if self._closed:
            return None
        if self._connection == None or self._connection.isClosed():
            return None

        if self._statementType is not StatementType.UPDATE or self._rowcount < 1:
            return None

        if self._rowcount > 1:
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT LAST_IDENTITY()")
                self._lastrowid = cursor.fetchone()[0]
            return self._lastrowid

        # In multiple rows inserted it returns the first inserted value, not the last one
        with self._connection._lock:
            self._out_message.wire._write_header(_Message.GET_AUTO_GENERATED_KEYS)
            sequence_number = self._connection._get_new_sequence_number()
            self._out_message._send(sequence_number)
            self._in_message._read_message_sql(sequence_number)
            self._sqlcode = self._in_message.wire.header._get_function_code()
            if self._sqlcode != 100:
                raise DatabaseError(self._get_error_info(self._sqlcode))
            self._get_column_info(self._in_message.wire)
            self._lastrowid = self._in_message.wire._get()
        return self._lastrowid

    def _cleanup(self):
        super()._cleanup()
        if self._rsrow != None:
            self._rsrow = None
        if self._params != None:
            self._params._clear()
        self._multiple_result_sets = False
        self._mrs_done = False
        self._fetch_done = False
        self._parameter_list_mismatch_exception = False
        self._parameter_sets = 0
        self._rowcount = -1
        self._exec_params = None
        self._statementType = StatementType.UPDATE
        self.statementFeatureOption = 0
        self.maxRowItemCount = 0
        self._is_batch_update = False

    def close(self):
        if self._closed:
            return
        self._columns = None
        self._rowcount = -1
        self.arraysize = 0

        self._connection = None
        self._in_message = None
        self._out_message = None

        self._result_set = None

        if self._rsrow != None:
            self._rsrow = None
        self._cursor_ptr = 0
        self._scroll_flag = False
        self._warehouse = []
        self._warehouse_dict = {}
        self._last_row_in_warehouse_dict = -1
        self._warehouse_dict_keys = []

        self._params = None
        self._parsed_statement = None
        self._cursor_type = CursorType.DEFAULT
        self._statementType = StatementType.UPDATE  # default
        self._paramInfo = None
        self.statement = None
        self.statementFeatureOption = Feature.optionNone
        self._statement_id = None
        self._sqlcode = None
        self._current_wire = None
        self._result_set = []
        self._rs_index = -1

        self._parameter_sets = 0
        self._exec_params = None
        self._is_batch_update = False
        self._multiple_result_sets = False
        self._mrs_done = False
        self._fetch_done = False
        self._parameter_list_mismatch_exception = False
        if self._output_parameter_list != None:
            self._output_parameter_list._clear_list()

        self._closed = True

    def executemany(self, operation, seq_of_params):
        super().executemany(operation, seq_of_params)
        self._rowcount = 0
        for i in range(len(self.params)):
            self._rowcount += self._in_message.wire._get()
        return self._rowcount

    def scroll(self, value, mode):
        if mode == None or mode == "":
            mode = "relative"
        mode = mode.lower()
        if mode != "absolute" and mode != "relative":
            raise ValueError("This mode is not supported - use 'relative' or 'absolute'.")

        # Backward Scrolling
        if value < 0:
            if mode == "relative":
                self._rownumber = self._cursor_ptr + value - 1
            else:
                raise ValueError("Negative values with absolute scrolling are not allowed.")
            self._cursor_ptr = self._rownumber + 1
            if self._rs_index == 0:
                return self._warehouse[self._rownumber]
            else:
                if self._rownumber <= self._last_row_in_warehouse_dict:
                    return self._retrieve_from_warehouse(self._rownumber)
                else:
                    if self._current_wire == None:
                        rows_available = self._last_row_in_warehouse_dict
                    else:
                        rows_available = self._last_row_in_warehouse_dict + len(self._warehouse)
                    if self._rownumber <= rows_available:
                        return self._warehouse[self._rownumber - self._last_row_in_warehouse_dict - 1]
        # Forward Scrolling
        else:
            if mode == "absolute":
                self._cursor_ptr = 0
            self._scroll_flag = True
            self._rownumber = self._cursor_ptr + value - 1
            if self._rs_index == 0:
                if self._rownumber >= len(self._warehouse):
                    if mode == "absolute":
                        self._cursor_ptr = len(self._warehouse)
                    return self.fetchone()
                else:
                    self._scroll_flag = False
                    self._cursor_ptr = self._rownumber + 1
                    return self._warehouse[self._rownumber]
            else:
                if self._rownumber <= self._last_row_in_warehouse_dict:
                    self._scroll_flag = False
                    self._cursor_ptr = self._rownumber + 1
                    return self._retrieve_from_warehouse(self._rownumber)
                else:
                    if self._current_wire == None:
                        rows_available = self._last_row_in_warehouse_dict
                    else:
                        rows_available = self._last_row_in_warehouse_dict + len(self._warehouse)
                    if self._rownumber <= rows_available:
                        self._scroll_flag = False
                        self._cursor_ptr = self._rownumber + 1
                        return self._warehouse[self._rownumber - self._last_row_in_warehouse_dict - 1]
                    else:
                        if mode == "absolute":
                            self._cursor_ptr = rows_available + 1
                        return self.fetchone()

    def _retrieve_from_warehouse(self, value):
        for idx, (key, val) in enumerate(self._warehouse_dict.items()):
            if value <= key:
                if idx != 0:
                    prev_key = self._warehouse_dict_keys[idx - 1]
                    return val[value - prev_key - 1]
                return val[value]

    def _switch_buffer(self):
        if self._sqlcode == 0:
            with self._connection._lock:
                self._out_message.wire._write_header(_Message.FETCH_DATA)
                intersystems_iris._MessageHeader._MessageHeader._set_statement_id(
                    self._out_message.wire.buffer, self._statement_id
                )

                sequence_number = self._connection._get_new_sequence_number()
                self._out_message._send(sequence_number)

                self._in_message._read_message_sql(sequence_number, self._statement_id, 0, [100])
                self._sqlcode = self._in_message.wire.header._get_function_code()
                self._process_sqlcode(self._sqlcode)
            self._result_set.append(self._in_message.wire)

        if self._sqlcode == 404:
            self._query404()
            return

        if self._rs_index + 1 == len(self._result_set):
            self._warehouse_dict[self._cursor_ptr] = self._warehouse
            self._warehouse_dict_keys = sorted(self._warehouse_dict.keys())
            self._last_row_in_warehouse_dict = self._warehouse_dict_keys[-1]
            self._current_wire = None
        else:
            self._warehouse_dict[self._cursor_ptr] = self._warehouse
            self._warehouse_dict_keys = sorted(self._warehouse_dict.keys())
            self._last_row_in_warehouse_dict = self._warehouse_dict_keys[-1]
            self._warehouse = []
            self._rsrow._new_buffer = True
            self._rs_index += 1
            self._current_wire = self._result_set[self._rs_index]

    def fetchone_helper(self):
        row_indexing = True
        if self.statementFeatureOption & Feature.optionFastSelect == Feature.optionFastSelect:
            list_item = self._current_wire.list_item
            buffer = list_item.buffer
            length = list_item.list_buffer_end

            if self._rsrow._new_buffer:
                prev_offset = list_item.next_offset
                self._rsrow._new_buffer = False
            else:
                prev_offset = self._rsrow._offsets._length

            if prev_offset < length:
                if self._rsrow._fast_first_iter:
                    self._rsrow._fast_first_iter = False
                else:
                    if self._rsrow._new_buffer:
                        list_item.buffer = buffer
                    list_item.next_offset = prev_offset
                intersystems_iris._DBList._DBList._get_list_element(list_item)
                length = list_item.next_offset
                prev_offset = list_item.data_offset
                if list_item.data_length == 0:  #
                    for j in range(self._rsrow.colCount):
                        rowItems[j] = -1
                    self._rsrow._offsets = self._rsrow.update(rowItems)  # ???
                    return True
            else:
                if self._rsrow.rowItems != None:
                    self._rsrow.rowItems[-1] = 0
                return False

            self._rsrow._last_list_item = list_item
            self._rsrow._offsets = self._rsrow.DataRowFastSelect(self._rsrow, prev_offset, length, buffer)
            self._warehouse.append(self._rsrow._offsets)

            if self._current_wire._is_end():
                self._switch_buffer()

        else:
            row_indexing = self._rsrow.indexRow(self._current_wire.list_item)
            if row_indexing:
                self._warehouse.append(self._rsrow._offsets)

            if self._rsrow.rowItems == None:
                return

            if self._rsrow.rowItems[-1] >= self._current_wire.list_item.list_buffer_end:
                self._switch_buffer()

        self._cursor_ptr += 1
        return row_indexing

    def fetchone(self):
        if self._result_set == None:
            raise InterfaceError(
                "Either execute has not yet been called, or the previous call of execute did not return a result set"
            )

        if self._current_wire == None and self._cursor_ptr > self._last_row_in_warehouse_dict:
            return None

        retval = None
        if self._rs_index == 0:
            if self._cursor_ptr < len(self._warehouse):
                self._cursor_ptr += 1
                retval = self._warehouse[self._cursor_ptr - 1]
        else:
            rownumber = self._cursor_ptr
            if rownumber <= self._last_row_in_warehouse_dict:
                self._cursor_ptr += 1
                retval = self._retrieve_from_warehouse(rownumber)
            else:
                if self._current_wire == None:
                    rows_available = self._last_row_in_warehouse_dict
                else:
                    rows_available = self._last_row_in_warehouse_dict + len(self._warehouse)
                if rownumber <= rows_available:
                    self._cursor_ptr += 1
                    retval = self._warehouse[rownumber - self._last_row_in_warehouse_dict - 1]

        if retval is None:
            if self._scroll_flag:
                while self._cursor_ptr <= self._rownumber:
                    if self.fetchone_helper():
                        retval = self._rsrow._offsets
                self._scroll_flag = False
            else:
                if self.fetchone_helper():
                    retval = self._rsrow._offsets

        if retval is None:
            return retval
        # print('retval', retval[:])
        return retval.as_tuple()
        # return tuple(retval[:])

    def fetchmany(self, size=None):
        if self._result_set == None:
            raise InterfaceError(
                "Either execute has not yet been called, or the previous call of execute did not return a result set"
            )

        if self._current_wire == None:
            if self._cursor_ptr > self._last_row_in_warehouse_dict:
                return []
            if size is None:
                size = self.arraysize
            if self._rs_index == 0:
                if self._cursor_ptr < len(self._warehouse):
                    rows = []
                    for i in range(size):
                        row = self._warehouse[self._cursor_ptr]
                        rows.append(row[:])
                        if self._cursor_ptr + 1 >= len(self._warehouse):
                            self._cursor_ptr += 1
                            break
                        self._cursor_ptr += 1
                    return rows
            else:
                rows = []
                for i in range(size):
                    row = self._retrieve_from_warehouse(self._cursor_ptr)
                    rows.append(row[:])
                    if self._cursor_ptr + 1 > self._last_row_in_warehouse_dict:
                        self._cursor_ptr += 1
                        break
                    self._cursor_ptr += 1
                return rows

        if size is None:
            size = self.arraysize

        rows = []
        for i in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def fetchall(self):
        if self._result_set == None:
            raise InterfaceError(
                "Either execute has not yet been called, or the previous call of execute did not return a result set"
            )

        if self._current_wire == None:
            if self._cursor_ptr > self._last_row_in_warehouse_dict:
                return []
            if self._rs_index == 0:
                if self._cursor_ptr < len(self._warehouse):
                    rows = []
                    while 1:
                        row = self._warehouse[self._cursor_ptr]
                        rows.append(row[:])
                        if self._cursor_ptr + 1 >= len(self._warehouse):
                            self._cursor_ptr += 1
                            break
                        self._cursor_ptr += 1
                    return rows
            else:
                rows = []
                while 1:
                    row = self._retrieve_from_warehouse(self._cursor_ptr)
                    rows.append(row[:])
                    if self._cursor_ptr + 1 > self._last_row_in_warehouse_dict:
                        self._cursor_ptr += 1
                        break
                    self._cursor_ptr += 1
                return rows

        rows = []
        while self._current_wire is not None:
            row = self.fetchone()
            if not row:
                break
            rows.append(row)

        return rows


class EmbdeddedCursor(_BaseCursor):
    embedded = True
    _result_set = None

    def __init__(self, connection: _IRISEmbedded) -> None:
        super().__init__(connection)
        self._sql = connection.iris.sql
        self._iris = connection.iris
        self._closed = False
        self._connection = connection
        self.restore_selectmode = self._iris.system.SQL.SetSelectMode(1)
        self.restore_autocommit = self._iris.system.SQL.SetAutoCommit(
            0 if self._connection.autoCommit is None else 1 if self._connection.autoCommit else 2
        )

    def close(self):
        self._iris.system.SQL.SetSelectMode(self.restore_selectmode)
        self._iris.system.SQL.SetAutoCommit(self.restore_autocommit)
        super().close()

    def __del__(self):
        try:
            self.close()
        except:
            pass
        return

    def _get_cached_info(self):
        return False

    def _get_parameters(self, params_set=0):
        params = self._params.collect(params_set)
        # None = '', '' = b'\x00'
        _conv = {
            type(None): lambda v: "",
            str: lambda v: v or b"\x00",
            decimal.Decimal: lambda v: float(v),
        }
        params = [_conv[type(v)](v) if type(v) in _conv else v for v in params]
        return params

    def _get_column_info(self):
        self._columns = []
        if self._result_set is None:
            return

        metadata = self._result_set.ResultSet._GetMetadata()
        count = metadata.columnCount if metadata != "" and metadata is not None else 0
        for i in range(count):
            slotPosition = i + 1
            _column_info = metadata.columns.GetAt(slotPosition)
            name = _column_info.colName
            odbctype = _column_info.ODBCType
            if _column_info.scale in SQLType.__members__:
                # There is a bug on IRIS side, when it may return incorrectly when it passed that way NUMERIC(?, ?)
                precision = 15
                scale = 15
            else:
                precision = _column_info.precision or None
                scale = _column_info.scale or None
            nullable = _column_info.isNullable
            label = _column_info.label
            tableName = _column_info.tableName
            schema = _column_info.schemaName
            catalog = None
            additionalData = [
                _column_info.isAutoIncrement,
                _column_info.isCaseSensitive,
                _column_info.isCurrency,
                _column_info.isReadOnly,
                _column_info.isRowVersion,
                _column_info.isUnique,
                _column_info.isAliased,
                _column_info.isExpression,
                _column_info.isHidden,
                _column_info.isIdentity,
                _column_info.isKeyColumn,
                _column_info.isRowId,
            ]
            self._columns.append(
                intersystems_iris.dbapi._Column._Column(
                    name,
                    odbctype,
                    precision,
                    scale,
                    nullable,
                    label,
                    tableName,
                    schema,
                    catalog,
                    additionalData,
                    slotPosition,
                )
            )

    @property
    def lastrowid(self):
        return self._lastrowid

    def _prepare_new(self):
        statement = self._parsed_statement
        sqlcode = 0
        message = None
        try:
            self._statement = self._sql.prepare(statement)
        except Exception as ex:
            sqlcode = ex.sqlcode
            message = ex.message
        self._process_sqlcode(sqlcode, message)

    def _prepared_query_execute(self):
        self._rowcount = 0
        params = self._get_parameters()
        sqlcode = 0
        message = None
        try:
            self._result_set = self._statement.execute(*params)
            self._get_column_info()
            self._rowcount += self._result_set.ResultSet._ROWCOUNT
        except Exception as ex:
            sqlcode = ex.sqlcode
            message = ex.message

        self._process_sqlcode(sqlcode, message)

    def _send_direct_update_request(self):
        self._rowcount = 0
        self._lastrowid = None
        statement = self._parsed_statement

        sets = self._parameter_sets or 1
        for i in range(sets):
            params = self._get_parameters(i)

            sqlcode = 0
            message = None
            try:
                _result_set = self._sql.exec(statement, *params)
                self._rowcount += _result_set.ResultSet._ROWCOUNT
                self._lastrowid = _result_set.ResultSet._ROWID
            except Exception as ex:
                sqlcode = ex.sqlcode
                message = ex.message

            self._process_sqlcode(sqlcode, message)

    def _send_direct_query_request(self):
        self._rowcount = 0
        statement = self._parsed_statement

        params = self._get_parameters()
        sqlcode = 0
        message = None
        try:
            self._result_set = self._sql.exec(statement, *params)
            self._rowcount = self._result_set.ResultSet._ROWCOUNT
            self._get_column_info()
        except Exception as ex:
            sqlcode = ex.sqlcode
            message = ex.message
        self._process_sqlcode(sqlcode, message)

    def _prepared_update_execute(self):
        self._rowcount = 0
        self._lastrowid = None
        sets = self._parameter_sets or 1
        metadata = self._statement.Statement._Metadata.parameters
        param_types = [metadata.GetAt(i + 1).ODBCType for i in range(metadata.Size)]

        stream_chunk_size = 32000

        for i in range(sets):
            params = self._get_parameters(i)
            for ip, param in enumerate(params):
                if param_types[ip] in (SQLType.LONGVARBINARY, SQLType.LONGVARCHAR):
                    stream_class = (
                        "%Stream.GlobalBinary"
                        if param_types[ip] == SQLType.LONGVARBINARY
                        else "%Stream.GlobalCharacter"
                    )
                    stream = self._iris.cls(stream_class)._New()
                    while param:
                        stream.Write(param[:stream_chunk_size])
                        param = param[stream_chunk_size:]
                    params[ip] = stream

            sqlcode = 0
            message = None
            try:
                _result_set = self._statement.execute(*params)
                self._rowcount += _result_set.ResultSet._ROWCOUNT
                self._lastrowid = _result_set.ResultSet._ROWID
            except Exception as ex:
                sqlcode = ex.sqlcode
                message = ex.message
            self._process_sqlcode(sqlcode, message)

    def _send_direct_stored_procedure_request(self):
        sqlproc = self._parsed_statement
        self._rowcount = 0
        params = self._get_parameters()
        params_marks = ", ".join(["?"] * len(params))
        statement = f"CALL {sqlproc} ({params_marks})"

        sqlcode = 0
        message = None
        try:
            self._result_set = self._sql.exec(statement, *params)
            self._rowcount = self._result_set.ResultSet._ROWCOUNT
            self._get_column_info()
        except Exception as ex:
            sqlcode = ex.sqlcode
            message = ex.message
        self._process_sqlcode(sqlcode, message)

    @property
    def rowcount(self):
        return self._rowcount

    def fetchone(self):
        if self._result_set == None:
            raise InterfaceError(
                "Either execute has not yet been called, or the previous call of execute did not return a result set"
            )

        try:
            values = self._result_set.__next__()
        except:
            return None

        values = [None if v == "" else "" if v == "\x00" else v for v in values]
        row = namedtuple("Row", [col.name for col in self._columns], rename=True)

        _types = {
            SQLType.GUID: uuid.UUID,
            SQLType.BIGINT: int,
            SQLType.BINARY: bytes,
            SQLType.VARBINARY: bytes,
            SQLType.BIT: bool,
            SQLType.FLOAT: float,
            SQLType.NUMERIC: decimal.Decimal,
            SQLType.INTEGER: int,
            SQLType.VARCHAR: str,
            SQLType.LONGVARBINARY: IRISBinaryStream,
            SQLType.LONGVARCHAR: IRISStream,
        }

        if self._columns:
            for _column in self._columns:
                value = values[_column.slotPosition - 1]

                ctype = _column.type
                value_type = _types[ctype] if ctype in _types else None
                try:
                    if not _column.tableName and not _column.schema:
                        if type(value) == float:
                            value = decimal.Decimal(str(value))
                    elif value is None or value_type is None:
                        pass
                    elif value_type is bytes:
                        value = bytes(map(ord, value))
                    elif value_type is decimal.Decimal:
                        value = decimal.Decimal(str(value))
                    elif issubclass(value_type, IRISStream):
                        stream = value_type(self._connection, value, embedded=True)
                        value = stream.fetch()
                    elif not isinstance(value, value_type):
                        value = value_type(value)
                except Exception as ex:
                    raise ex
                    pass
                values[_column.slotPosition - 1] = value
        return row(*values)

    def fetchall(self):
        if self._result_set == None:
            raise InterfaceError(
                "Either execute has not yet been called, or the previous call of execute did not return a result set"
            )

        rows = []
        while True:
            row = self.fetchone()
            if not row:
                break
            rows.append(row)
        return rows

    def fetchmany(self, size=None):
        if self._result_set == None:
            raise InterfaceError(
                "Either execute has not yet been called, or the previous call of execute did not return a result set"
            )

        if size is None:
            size = self.arraysize

        rows = []
        for i in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def nextset(self):
        raise NotImplementedErrorDBAPI()


# Type Objects
def Date(year, month, day):
    raise NotImplementedErrorDBAPI()


def Time(hour, minutes, second):
    raise NotImplementedErrorDBAPI()


def Timestamp(year, month, day, hour, minute, second):
    raise NotImplementedErrorDBAPI()


def DateFromTicks(ticks):
    raise NotImplementedErrorDBAPI()


def TimeFromTicks(ticks):
    raise NotImplementedErrorDBAPI()


def TimestampFromTicks(ticks):
    raise NotImplementedErrorDBAPI()


# def Binary(string):
#     return string

# Type definitions.
Binary = bytes

STRING = str
BINARY = bytes
NUMBER = float
ROWID = str

# still needs type singletons (?)


# Exception architecture
class Error(Exception):
    pass


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass
