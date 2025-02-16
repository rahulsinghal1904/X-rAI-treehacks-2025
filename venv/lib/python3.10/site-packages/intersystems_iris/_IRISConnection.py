import base64
import enum
import getpass
import os
import platform
import socket
import struct
import sys
import threading
import ssl
import intersystems_iris._ConnectionInformation
import intersystems_iris._ConnectionParameters
import intersystems_iris._Constant
import intersystems_iris._Device
import intersystems_iris._GatewayException
import intersystems_iris._IRISOREF
import intersystems_iris._InStream
import intersystems_iris._LogFileStream
import intersystems_iris._OutStream
import intersystems_iris._PythonGateway
import intersystems_iris._IRIS
import intersystems_iris._IRISObject
import intersystems_iris.dbapi

class Feature():
    optionNone = 0
    optionFastSelect = 1
    optionFastInsert = 2
    optionRedirectOutput = 32
    optionAllowedOptions = optionRedirectOutput
    optionDefaultOptions = optionRedirectOutput

class _IRISConnection():
    '''
A connection to an IRIS instance.

Create one by calling the iris.createConnection method. The hostname, port, namespace, timeout, and logfile from the last successful connection attempt are saved as properties of the connection object.
'''

    CLOSED_PROXY_UPDATE_THRESHOLD = 100
    MESSAGE_HANDSHAKE = b'HS'
    MESSAGE_CONNECT = b'CN'
    MESSAGE_DISCONNECT = b'DC'

    def __init__(self):
        self._connection_info = intersystems_iris._ConnectionInformation._ConnectionInformation()
        self._connection_params = intersystems_iris._ConnectionParameters._ConnectionParameters() 
        self._sequence_number = -1
        self._statement_id = 0
        self._lock = threading.RLock()
        self._lock_closed_oref = threading.RLock()
        self._disable_output_redirect = False
        self._oref_registry = {}
        self._oref_to_class_map = {}
        self._iris_object_proxy_map = {}
        self._iris_object_proxy_closed = []
        self._pre_preparse_cache = {}
        self._preparedCache = {}
        self._autoCommit = True
        self._device = None
        self._gateways = {}
        self._log_stream = None
        self._output_redirect_handler = None


    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __del__(self):
        try:
            self.close()
        except:
            pass
        return

    @property
    def hostname(self):
        return self._connection_params.hostname

    @property
    def port(self):
        return self._connection_params.port

    @property
    def namespace(self):
        return self._connection_params.namespace

    @property
    def timeout(self):
        return self._connection_params.timeout

    @property
    def sharedmemory(self):
        return self._connection_params.sharedmemory

    @property
    def logfile(self):
        return self._connection_params.logfile

    @property
    def compact_double(self):
        return self._connection_info._compact_double

    def _is_using_sharedmemory(self):
        return self._device.is_sharedmemory()

    def close(self):
        '''
Close the connection to the IRIS instance if it is open.

close()

Do nothing if the connection is already closed.

Return Value
------------
None.
'''
        if self._device != None:
            with self._lock:
                try:
                    self._out_message.wire._write_header(intersystems_iris.IRISConnection.MESSAGE_DISCONNECT)
                    self._out_message._send(0)
                except Exception as e:
                    pass
            self._preparedCache = {}
            self._device.close()
            self._device = None
        return

    def isClosed(self):
        '''
Return True if the connection is closed.

isClosed()

Returns
-------
bool
    True if the connection is closed,
    False if it is open.
'''
        return self._device == None

    def isUsingSharedMemory(self):
        '''
Return True if the connection is open and using shared memory.

isUsingSharedMemory()

Returns
-------
bool
    True if the connection has an open shared memory connection,
    False otherwise.
'''
        return self._device.is_sharedmemory()

    def _set_gateway(self, gateway):
        thread_id = threading.get_ident()
        self._gateways[thread_id] = gateway
        return

    def _get_gateway(self):
        thread_id = threading.get_ident()
        if thread_id not in self._gateways.keys():
            self._gateways[thread_id] = intersystems_iris._PythonGateway._PythonGateway(self, None, None, None)
        return self._gateways[thread_id]

    def _connect(
        self, 
        hostname, 
        port, 
        namespace, 
        username, 
        password, 
        timeout, 
        sharedmemory, 
        logfile, 
        sslcontext, 
        autoCommit, 
        isolationLevel, 
        featureOptions, 
        application_name=None,
    ):
        if type(hostname) != str or len(hostname) == 0:
            raise ValueError("invalid hostname: non-empty string required")
        if type(port) != int or port == 0:
            raise ValueError("invalid port: non-zero integer required")
        if type(namespace) != str or len(namespace) == 0:
            raise ValueError("invalid namespace: non-empty string required")
        if ((featureOptions & ~intersystems_iris._IRISConnection.Feature.optionAllowedOptions) != 0):
            raise ValueError("invalid featureOptions value specified: " + str(featureOptions))
        try:
            self._log_stream = intersystems_iris._LogFileStream._LogFileStream(logfile) if len(logfile)>0 else None
            with self._lock:
                server_address = (hostname, port)
                self._device = intersystems_iris._Device._Device(self, None, sslcontext) 
                self._device.settimeout(timeout) 
                self._device.connect(server_address) 
                self._in_message = intersystems_iris._InStream._InStream(self)
                self._out_message = intersystems_iris._OutStream._OutStream(self)
                # send MESSAGE_HANDSHAKE message
                self._out_message.wire._write_header(intersystems_iris.IRISConnection.MESSAGE_HANDSHAKE)
                protocol_bytes = struct.pack("<H",intersystems_iris._Constant._Constant.PROTOCOL_VERSION)
                self._out_message.wire._set_raw_bytes(protocol_bytes)
                sequence_number = self._get_new_sequence_number()
                self._out_message._send(sequence_number)
                self._in_message._read_message_sql(sequence_number)
                protocol_bytes = self._in_message.wire._get_raw_bytes(2)
                protocol = struct.unpack("<H", protocol_bytes)[0]
                if protocol<61:
                    raise Exception("connection failed: IRIS xDBC protocol is not compatible")
                self._connection_info.protocol_version = protocol
                featureBits = struct.unpack("<H", self._in_message.wire._get_raw_bytes(2))[0]
                self._connection_info._is_unicode = (featureBits & 1) > 0
                self._connection_info._compact_double = (protocol >= 65) and (featureBits & 2) > 0
                self._connection_info._set_server_locale(self._in_message.wire._get())
                self._out_message.wire._set_connection_info(self._connection_info)
                # send MESSAGE_CONNECT message
                self._out_message.wire._write_header(intersystems_iris.IRISConnection.MESSAGE_CONNECT)
                # namespace
                self._out_message.wire._set(namespace)
                # encoded username
                if self._connection_info._is_unicode:
                    username_encoded = self.__encodew(username)
                else:
                    username_encoded = self.__encode(username)
                self._out_message.wire._set(username_encoded)
                # encoded password
                if self._connection_info._is_unicode:
                    password_encoded = self.__encodew(password)
                else:
                    password_encoded = self.__encode(password)
                self._out_message.wire._set(password_encoded)
                # OS username
                osuser = getpass.getuser()
                self._out_message.wire._set(osuser)
                # machine hostname
                thishostname = self._device.gethostname()
                self._out_message.wire._set(thishostname)
                # application name
                appname = application_name or os.path.basename(__file__)
                self._out_message.wire._set(appname)
                # machine OS Info, not used anymore
                osinfo = bytearray(12)
                self._out_message.wire._set(osinfo)
                # temporarily disable shared-memory
                sharedmemory = False
                # HostName or IP for licensing
                if (sharedmemory):
                    self._out_message.wire._set("SHM|||||")
                else:
                    thishostIP = self._device.gethostbyname(hostname)
                    self._out_message.wire._set(thishostIP)
                # EventClass -> null for now
                self._out_message.wire._set("")
                # Autocommit
                if autoCommit:
                    self._out_message.wire._set(1)
                else:
                    self._out_message.wire._set(2)
                # Isolation Level
                self._out_message.wire._set(isolationLevel)
                # Feature Option
                self._out_message.wire._set(featureOptions)
                sequence_number = self._get_new_sequence_number()
                self._out_message._send(sequence_number)
                code = self._in_message._read_message_sql(sequence_number)
                if code != 0:
                    raise Exception(self._in_message.wire._get())
                self._connection_info._parse_server_version(self._in_message.wire._get())
                self._connection_info._delimited_ids = self._in_message.wire._get()
                self._in_message.wire._get() # ignore
                isolationLevel = self._in_message.wire._get()
                self._connection_info._server_job_number = self._in_message.wire._get()
                sqlEmptyString = self._in_message.wire._get()
                featureOptions = self._in_message.wire._get()
                self._device.settimeout(None)
                if(sharedmemory):
                    self._device.establishSHMSocket()
                self._connection_params.hostname = hostname
                self._connection_params.port = port
                self._connection_params.namespace = namespace
                self._connection_params.timeout = timeout
                self._connection_params.sharedmemory = sharedmemory
                self._connection_params.logfile = logfile
                self._connection_params.sslcontext = sslcontext
                self._autoCommit = autoCommit
                self._connection_params.isolationLevel = isolationLevel
                self._connection_params.featureOptions = featureOptions
        except ssl.SSLError:
            # To ensure security, replace any ssl error messages with a generic error
            self._device.close()
            self._device = None
            raise ssl.SSLError(1, "Error establishing SSL connection")
        except Exception as e:
            if self._device != None:
                self._device.close()
                self._device = None
            raise e
        return

    # Encodes the given Unicode string
    def __encodew(self, input_data):
        length = len(input_data)
        out_char = [None] * length
        i = 0
        while length > 0:
            length -= 1
            tint2 = ord(input_data[i])
            tint = (((ord(input_data[i]) ^ 0xA7) & 255) + length) & 255
            out_char[length] =  chr((tint2 & (255<<8)) | (((tint << 5) | (tint >> 3)) & 255))
            i += 1
        if i == 0:
            return None
        return ''.join(out_char)

    # Encodes the given Multi Byte string
    def __encode(self, input_data):
        length = len(input_data)
        out = [None] * length
        i = 0
        while length > 0:
            length -= 1
            tint = (((ord(input_data[i]) ^ 0xA7) & 255) + length) & 255
            tmp = (tint << 5) | (tint >> 3)
            tmp = tmp.to_bytes((tmp.bit_length() + 7) // 8, byteorder='little', signed=True)[0]
            out[length] = tmp
            i += 1
        return bytes(out)

    # this method should only be called when the connection is locked
    def _get_new_sequence_number(self):
        self._sequence_number += 2
        return self._sequence_number

    # this method should only be called when the connection is locked
    def _get_new_statement_id(self):
        self._statement_id += 1
        return self._statement_id

    def _oref_registry_lookup(self, object):
        for key in self._oref_registry:
            if self._oref_registry[key] == object:
                return key
        for key in self._iris_object_proxy_map:
            if self._iris_object_proxy_map[key]() == object:
                return key
        return None

    def _map_local_object_to_oref(self, caller_in_message, caller_out_message, object):
        class_name = "%Net.Remote.Object"
        with self._lock:
            sequence_number = self._get_new_sequence_number()
            caller_out_message.wire._write_header(intersystems_iris._Constant._Constant.MESSAGE_CREATE_OBJECT)
            caller_out_message.wire._set(1)
            caller_out_message.wire._set(class_name)
            caller_out_message._send(sequence_number)
            caller_in_message._read_message_gateway(sequence_number)
            oref = caller_in_message.wire._get()._oref
            if oref == "error":
                raise Exception(caller_in_message.wire._get())
        self._oref_registry[oref] = object
        self._oref_to_class_map[oref] = type(object).__name__
        with self._lock_closed_oref:
            self._iris_object_proxy_closed.append(oref)
        return oref

    def _map_local_object_from_oref(self, oref):
        created_proxy = False
        obj = self._oref_registry.get(oref)
        if obj == None:
            weak = self._iris_object_proxy_map.get(oref)
            if weak != None:
                obj = weak()
        if obj == None:
            obj = intersystems_iris.IRISObject(self, oref)
            created_proxy = True
        if not created_proxy:
            with self._lock_closed_oref:
                self._iris_object_proxy_closed.append(oref)
        return obj

    def _close_unused_oref(self, oref):
        with self._lock_closed_oref:
            self._iris_object_proxy_closed.append(oref)

    def _get_closed_iris_objects(self):
        with self._lock_closed_oref:
            closed_iris_objects = self._iris_object_proxy_closed
            self._iris_object_proxy_closed = []
        return ",".join(closed_iris_objects)

    def releaseIRISObjects(self):
        '''
Immediately release proxy references on IRISObject proxy objects that have been closed.

Proxy references are references on OREFs taken out on behave of IRISObject objects. Once an IRISObject is closed, the proxy reference will be released, but that process usually doesn't happen immediately.

Calling releaseIRISObjects will cause all the proxy references of already-closed proxy objects to be release, which can lead to closing of OREFs if no other references exist in the IRIS process.
'''
        native = intersystems_iris.IRIS(self)
        native._release_closed_iris_object(True)
        return

    def _get_parameter_listream(self, caller_in_message, caller_out_message, oref, instance):
        if oref.endswith("@%Library.ListOfDataTypes") or oref.endswith("@%Library.ListOfObjects"):
            if type(instance) != list:
                raise intersystems_iris._GatewayException._GatewayException("Object is not a list")
            with self._lock:
                caller_out_message.wire._write_header(intersystems_iris._Constant._Constant.MESSAGE_GET_LIST)
                caller_out_message.wire._set(oref)
                sequence_number = self._get_new_sequence_number()
                caller_out_message._send(sequence_number)
                code = caller_in_message._read_message_gateway(sequence_number)
                response = caller_in_message.wire._get()
                if response == "error":
                    raise  intersystems_iris._GatewayException._GatewayException(caller_in_message.wire._get())
                size = caller_in_message.wire._get()
                for i in range(size):
                    if i>= len(instance):
                        break
                    value = caller_in_message.wire._get()
                    if isinstance(value, intersystems_iris._IRISOREF._IRISOREF):
                        value = self._map_local_object_from_oref(value._oref)
                    instance[i] = value
                return
        if oref.endswith("@%Library.GlobalBinaryStream"):
            if type(instance) != bytearray:
                raise intersystems_iris._GatewayException._GatewayException("Object is not a bytearray")
            with self._lock:
                caller_out_message.wire._write_header(intersystems_iris._Constant._Constant.MESSAGE_GET_STREAM)
                caller_out_message.wire._set(oref)
                sequence_number = self._get_new_sequence_number()
                caller_out_message._send(sequence_number)
                code = caller_in_message._read_message_gateway(sequence_number)
                response = caller_in_message.wire._get()
                if response == "error":
                    raise  intersystems_iris._GatewayException._GatewayException(caller_in_message.wire._get())
                size = caller_in_message.wire._get()
                pointer = 0
                buffer_size = len(instance)
                while (size>0):
                    chunk_string = caller_in_message.wire._get()
                    chunk_bytes = bytes(chunk_string, self._connection_info._locale)
                    chunk_size = len(chunk_bytes)
                    if pointer+chunk_size>buffer_size:
                       chunk_size = buffer_size-pointer
                    instance[pointer:pointer+chunk_size] = chunk_bytes[0:chunk_size]
                    pointer += chunk_size
                    size -= chunk_size
                    if pointer >= buffer_size:
                        break
                return
        raise intersystems_iris._GatewayException._GatewayException("Invalid argument for %setall(1): " + str(oref))

    def setOutputRedirectHandler(self, class_name, method_name):
        class_object = self._get_gateway()._load_class(class_name)
        self._output_redirect_handler = getattr(class_object, method_name)

    def _cache_prepared_statement(self, prepared_statement):
        self._preparedCache[prepared_statement.statement] = prepared_statement
    
    def _isFastSelectOption(self):
        return bool(self._connection_params.featureOptions & intersystems_iris._IRISConnection.Feature.optionFastSelect) 

    def _isFastInsertOption(self):
        return bool(self._connection_params.featureOptions & intersystems_iris._IRISConnection.Feature.optionFastInsert) 

    def _isFastOption(self):
        return self._isFastSelectOption() or self._isFastInsertOption()

    def cursor(self):
        if self.isClosed():
            raise intersystems_iris.dbapi._DBAPI.InterfaceError("Connection is closed")
        return intersystems_iris.dbapi._DBAPI.Cursor(self)

    def commit(self):
        with self._lock:
            self._out_message.wire._write_header(intersystems_iris.dbapi._Message.COMMIT)
            sequence_number = self._get_new_sequence_number()
            self._out_message._send(sequence_number)
            self._in_message._read_message_sql(sequence_number, -1, 0, [0])

    def rollback(self):
        with self._lock:
            self._out_message.wire._write_header(intersystems_iris.dbapi._Message.ROLLBACK)
            sequence_number = self._get_new_sequence_number()
            self._out_message._send(sequence_number)
            self._in_message._read_message_sql(sequence_number, -1, 0, [0])
            
    def setAutoCommit(self, enableAutoCommit):
        self._autoCommit = enableAutoCommit
        code = intersystems_iris.dbapi._Message.AUTOCOMMIT_ON if enableAutoCommit else intersystems_iris.dbapi._Message.AUTOCOMMIT_OFF
        with self._lock:
            self._out_message.wire._write_header(code)
            sequence_number = self._get_new_sequence_number()
            self._out_message._send(sequence_number)

    def _add_pre_preparse_cache(self, sql, cursor):
        preparse_cache_size = 50 # this variable is in ConnectionParameters class of Java and is hardcoded to this value
        if len(self._pre_preparse_cache) < preparse_cache_size:
            if cursor._exec_params == None:
                self._pre_preparse_cache[sql] = CachedSQL(cursor)

class CachedSQL:
    def __init__(self, cursor):
        self._has_return_value = cursor._has_return_value
        self._statementType = cursor._statementType
        self._parsed_statement = cursor._parsed_statement
        self._params = intersystems_iris.dbapi._ParameterCollection._ParameterCollection(cursor._params, False)
        self._paramInfo = cursor._paramInfo
