import functools
import hashlib
import importlib
import os
import platform
import signal
import socket
import ssl
import xml.etree.ElementTree as ET
import base64
import sys
import threading
import traceback
import inspect
import decimal
import zipfile
import intersystems_iris._Constant
import intersystems_iris._GatewayException
import intersystems_iris._GatewayUtility
import intersystems_iris._InStream
import intersystems_iris._IRISOREF
import intersystems_iris._LogFileStream
import intersystems_iris._OutStream
import intersystems_iris._PrintStream
import intersystems_iris._GatewayContext
import intersystems_iris._IRIS
import intersystems_iris._IRISConnection
import intersystems_iris._IRISList

def synchronized(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        with self.lock:
            return func(self, *args, **kwargs)
    return wrapper

class _PythonGateway(threading.Thread):

    _output_redirect = None
    _error_redirect = None
    _server_name = None

    @classmethod
    def _main(cls, args):
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        port = cls.__get_port_number(args)
        log_stream = cls.__get_log_stream(args)
        cls._server_name = cls.__get_server_name(args)
        host = cls.__get_host(args)
        # secret_hash = cls.__get_secret(args)
        # sslcontext = cls.__get_sslcontext(args)
        (secret_hash, sslcontext) = cls.__get_secret_or_sslcontext(args, log_stream)
        try:
            cls.__initialize_output_redirection()
            server_socket = cls.__setup_server_socket(host, port, sslcontext)
            while True:
                try:
                    sock, addr = server_socket.accept()
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    try:
                        thread = cls(None, sock, log_stream, secret_hash)
                        thread.daemon = True
                        thread.start()
                    except Exception as e:
                        traceback.print_exc()
                except ssl.SSLError:
                    traceback.print_exc()
        except Exception as e:
            traceback.print_exc()
            raise intersystems_iris._GatewayException._GatewayException("[Python Gateway] Communication link failure: " + str(e))

    @staticmethod
    def __get_port_number(args):
        if len(args) == 0:
            raise intersystems_iris._GatewayException._GatewayException("Port number must be supplied as the first command line argument")
        else:
            return int(args[0])

    @staticmethod
    def __get_log_stream(args):
        if len(args) > 1 and len(args[1]) > 0:
            return intersystems_iris._LogFileStream._LogFileStream(args[1])
        return None

    @staticmethod
    def __get_server_name(args):
        if len(args) > 2:
            return args[2]
        return None

    @staticmethod
    def __get_host(args):
        if len(args) > 3 and len(args[3]) > 0:
            return args[3]
        # Using 0.0.0.0 causes the gateway to listen on all IP addresses
        return "127.0.0.1"

    @staticmethod
    def __hex_to_bin(ch):
        if ord('0') <= ord(ch) and ord(ch) <= ord('9'):
            return ord(ch) - ord('0')
        if ord('A') <= ord(ch) and ord(ch) <= ord('F'):
            return ord(ch) - ord('A') + 10
        if ord('a') <= ord(ch) and ord(ch) <= ord('f'):
            return ord(ch) - ord('a') + 10
        return -1

    @classmethod
    def __get_secret_or_sslcontext(cls, args, log_stream):
        if len(args) > 4:
            security_string = args[4]
            if security_string[:4] == "ssl:":
                try:
                    return (None, cls.__get_sslcontext(security_string[4:], log_stream))
                except Exception as e:
                    raise intersystems_iris._GatewayException._GatewayException("Error loading SSL Configuration: " + str(e))
            else:
                return (cls.__get_secret(security_string), None)
        return (None, None)
    
    @classmethod
    def __get_secret(cls, secure_str):
        number_chars = len(secure_str)
        if number_chars > 0:
            if number_chars > 16:
                number_chars = 16
            secure_str = secure_str[0:number_chars]
            # parse hex binary
            if number_chars % 2 != 0:
                raise ValueError("hexBinary needs to be even-length: " + secure_str)
            parsed_bytes = [b''] * int(number_chars / 2)
            for i in range(0, number_chars, 2):
                h = cls.__hex_to_bin(secure_str[i])
                l = cls.__hex_to_bin(secure_str[i + 1])
                if h == -1 or l == -1:
                    raise ValueError("contains illegal character for hexBinary: " + secure_str)
                parsed_bytes[int(i / 2)] = bytes([h * 16 + l])
            eight_bytes = b''
            for i in range(int(number_chars / 2)):
                eight_bytes += parsed_bytes[i]
            hashvalue = hashlib.sha256(eight_bytes).hexdigest()
            return hashvalue
        return None

    @classmethod
    def __get_sslcontext(cls, config_args, log_stream):
        config_split = config_args.split("?")
        config_file = config_split[0]
        key_pwd = cls.__decode_pwd(config_split[1]) if len(config_split) > 1 else ""

        # parse XML output from IRIS ssl config
        tree = ET.parse(config_file)
        root = tree.getroot()
        config = root[0]
        config_dict = {}
        for config_item in config:
            config_dict[config_item.tag] = config_item.text
        # print XML file to log, then delete it
        if log_stream:
            with open(log_stream.path, 'a') as f:
                end_line = intersystems_iris._LogFileStream._LogFileStream.LINE_SEPARATOR
                f.write(end_line + "SSL Configuration File:" + end_line)
                with open(config_file) as xml:
                    f.write(xml.read() + end_line)
        os.remove(config_file)
        # as a sanity check, verify the parsed XML has various required fields
        required_tags = ["Name", "Type", "Enabled", "VerifyPeer", "TLSMaxVersion", "TLSMinVersion", "CipherList", "Ciphersuites"]
        for tag in required_tags:
            if tag not in config_dict:
                raise ValueError("SSL config file did not contain the field '" + tag + "'")
        # verify provided configuration was for a server
        if config_dict["Type"] != "true":
            raise ValueError("Chosen SSL configuration must be a server configuration")
        # configure SSLContext object
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        # set verify_mode
        verify_dict = {"0": ssl.CERT_NONE, "1": ssl.CERT_OPTIONAL, "3": ssl.CERT_REQUIRED}
        context.verify_mode = verify_dict[config_dict["VerifyPeer"]]
        # load CA certificate, if one was provided
        if "CAFile" in config_dict:
            context.load_verify_locations(cafile = config_dict["CAFile"])
        # load certificate/key pair, if one was provided
        if "CertificateFile" in config_dict and "PrivateKeyFile" in config_dict:
            context.load_cert_chain(config_dict["CertificateFile"], config_dict["PrivateKeyFile"], key_pwd)
        # set cipher options
        context.set_ciphers(config_dict["CipherList"] + ":" + config_dict["Ciphersuites"])
        # set min and max TLS version
        tls_dict = {"4": ssl.TLSVersion.TLSv1, "8": ssl.TLSVersion.TLSv1_1, "16": ssl.TLSVersion.TLSv1_2, "32": ssl.TLSVersion.TLSv1_3}
        context.minimum_version = tls_dict[config_dict["TLSMinVersion"]]
        context.maximum_version = tls_dict[config_dict["TLSMaxVersion"]]
        return context

    @staticmethod
    def __decode_pwd(pwd):
        pwd_bytes = bytearray()
        pwd_bytes.extend(map(ord, pwd))
        pwd_decoded = base64.b64decode(pwd_bytes)
        return pwd_decoded.decode()

    @staticmethod
    def __setup_server_socket(host, port, sslcontext = None):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if platform.system().startswith("Windows"):
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        else:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((host, port))
        server_socket.listen(50)
        if sslcontext is None:
            return server_socket
        else:
            return sslcontext.wrap_socket(server_socket, True)

    @classmethod
    def __initialize_output_redirection(cls):
        cls._output_redirect = intersystems_iris._PrintStream._PrintStream(0)
        sys.stdout = cls._output_redirect
        cls._error_redirect = intersystems_iris._PrintStream._PrintStream(1)
        sys.stderr = cls._error_redirect

    def __set_redirect(self):
        if not self._connection._disable_output_redirect:
            self._output_redirect._register()
            self._error_redirect._register()

    def __init__(self, conn, sock, log_stream, hash):
        threading.Thread.__init__(self)
        self.lock = threading.RLock()
        if conn != None:
            self._connection = conn
            self._device = conn._device
            self._log_stream = conn._log_stream
        else:
            self._connection = intersystems_iris.IRISConnection()
            self._device = intersystems_iris._Device._Device(self._connection, sock)
            self._connection._log_stream = self._log_stream = log_stream
            self._connection._in_message = intersystems_iris._InStream._InStream(self._connection)
            self._connection._out_message = intersystems_iris._OutStream._OutStream(self._connection)
        self.__rundown = False
        self.in_message = intersystems_iris._InStream._InStream(self._connection)
        self.out_message = intersystems_iris._OutStream._OutStream(self._connection)
        self.in_message_secondary = intersystems_iris._InStream._InStream(self._connection)
        self.out_message_secondary = intersystems_iris._OutStream._OutStream(self._connection)
        self.out_message_sequence_number = 0
        self._thread_modules = {}
        self._sys_modules_lock = threading.RLock()
        if hash is not None:
            try:
                if not self.in_message._check_sheader(hash, sock):
                    self.__rundown = True
            except Exception as e:
                message = str(e)
                if (message == "Server closed communication device" or
                    message == "Communication error: Server closed communication device" or
                    message == "Connection reset"):
                        self.__rundown = True
                        return
                raise intersystems_iris._GatewayException._GatewayException(message)

    def __cleanup(self):
        if self._output_redirect != None:
            self._output_redirect._unregister()
        if self._error_redirect != None:
            self._error_redirect._unregister()
        self._connection.close()

    def _dispatch_reentrancy(self, other_in_message):
        code = other_in_message.wire.header._get_function_code()
        code = ((0xFF00 & code) >> 8) - 48
        previous_connection = intersystems_iris.GatewayContext._get_connection()
        intersystems_iris.GatewayContext._set_connection(self._connection)
        self.in_message.wire = other_in_message.wire
        is_disconnect = self.__process_message(code)
        self.out_message._send(self.out_message_sequence_number)
        if is_disconnect:
            if self._connection._log_stream != None:
                self._connection._log_stream._logApi(" << GatewayException: Connection closed inside reentrancy")
            self.__cleanup()
            raise Exception("Connection closed")
        intersystems_iris.GatewayContext._set_connection(previous_connection);

    @synchronized
    def run(self):
        try:
            self._connection._set_gateway(self)
            to_send_response = False;
            is_disconnect = False;
            while True:
                try:
                    with self._connection._lock:
                        if self.__rundown:
                            self.__cleanup()
                            return
                        if to_send_response:
                            self.out_message._send(self.out_message_sequence_number)
                        if is_disconnect:
                            self.__cleanup()
                            return
                        code = self.in_message._read_message_gateway()
                except Exception as e:
                    message = str(e)
                    if (message == "Server closed communication device" or message == "Connection reset"):
                        self._device.close()
                        return
                    raise intersystems_iris._GatewayException._GatewayException(message)
                is_disconnect = self.__process_message(code)
                to_send_response = (code != intersystems_iris._Constant._Constant.ENUM_MESSAGE_CONNECT)
        except BaseException as e:
            traceback.print_exc()
            try:
                self._device.close()
            except BaseException as e:
                traceback.print_exc()

    def __process_message(self, code):
        switcher = {
            intersystems_iris._Constant._Constant.ENUM_MESSAGE_CONNECT: self.__connect,
            intersystems_iris._Constant._Constant.ENUM_MESSAGE_DISCONNECT: self.__disconnect,
            intersystems_iris._Constant._Constant.ENUM_MESSAGE_SHUTDOWN: self.__shutdown,
            intersystems_iris._Constant._Constant.ENUM_MESSAGE_PING: self.__ping,
            intersystems_iris._Constant._Constant.ENUM_MESSAGE_BENCHMARK_ECHO: self.__benchmark_echo,
            intersystems_iris._Constant._Constant.ENUM_MESSAGE_LOAD_MODULES: self.__load_modules,
            intersystems_iris._Constant._Constant.ENUM_MESSAGE_DYNAMIC_EXECUTE_CONSTRUCTOR: self.__dynamic_execute_constructor,
            intersystems_iris._Constant._Constant.ENUM_MESSAGE_DYNAMIC_EXECUTE_METHOD: self.__dynamic_execute_method,
            intersystems_iris._Constant._Constant.ENUM_MESSAGE_DYNAMIC_EXECUTE_GET: self.__dynamic_execute_get,
            intersystems_iris._Constant._Constant.ENUM_MESSAGE_DYNAMIC_EXECUTE_SET: self.__dynamic_execute_set
        }
        func = switcher.get(code, "passphrase" if code == intersystems_iris._Constant._Constant.ENUM_MESSAGE_PASSPHRASE else "default")
        if func == "passphrase":
            pass
        elif func == "default":
            raise intersystems_iris._GatewayException._GatewayException("Unknown Message:", code)
        else:
            func()
        if code == intersystems_iris._Constant._Constant.ENUM_MESSAGE_DISCONNECT:
            return True
        else:
            return False

    def __connect(self):
        sequence_number = self.in_message.wire._get_header_count();
        unused = self.in_message.wire._get()
        self._connection._connection_info._is_unicode = bool(self.in_message.wire._get())
        self._connection._connection_info._set_server_locale(self.in_message.wire._get())
        self._connection._connection_info._server_job_number = self.in_message.wire._get()
        self._connection._connection_params.namespace = self.in_message.wire._get()
        module_count = int(self.in_message.wire._get())
        for i in range(module_count):
            try:
                self.__load_one_module(self.in_message.wire._get(), False)
            except BaseException as e:
                pass
        self._connection._disable_output_redirect = bool(self.in_message.wire._get())
        self.__set_redirect()
        usused = self.in_message.wire._get()
        dbsrv_protocol_version = int(self.in_message.wire._get())
        negotiated_protocol_version = min(intersystems_iris._Constant._Constant.PROTOCOL_VERSION, dbsrv_protocol_version)
        self._connection._connection_info._iris_install_dir = None if self.in_message.wire._is_end() else self.in_message.wire._get()
        self._connection._connection_params.hostname = None if self.in_message.wire._is_end() else self.in_message.wire._get()
        # temporarily disable shared-memory
        # self._connection._connection_params._set_sharedmemory(self._connection._connection_info._iris_install_dir != None)
        self._connection._connection_params._set_sharedmemory(False)
        compact_double = False if self.in_message.wire._is_end() else bool(self.in_message.wire._get())
        self._connection._connection_info.protocol_version = negotiated_protocol_version;
        self._connection._connection_info._compact_double = compact_double if negotiated_protocol_version >= 65 else False
        self.out_message.wire._set_connection_info(self._connection._connection_info)
        self.out_message.wire._write_header(intersystems_iris._Constant._Constant.MESSAGE_CONNECT)
        self.out_message.wire._set(self._connection._connection_info.protocol_version)
        self.out_message.wire._set(self._connection._connection_info.protocol_version)
        self.out_message.wire._set(self._connection._connection_params._get_sharedmemory())
        self.out_message.wire._set(self._server_name, True)
        self.out_message._send(sequence_number)
        if self._connection._connection_params._get_sharedmemory():
            self._connection._device.establishSHMSocket()
            if self._connection._is_using_sharedmemory():
                self.in_message = intersystems_iris._InStream._InStream(self._connection)
                self.out_message = intersystems_iris._OutStream._OutStream(self._connection)
                self.in_message_secondary = intersystems_iris._InStream._InStream(self._connection)
                self.out_message_secondary = intersystems_iris._OutStream._OutStream(self._connection)
                self._connection._in_message = intersystems_iris._InStream._InStream(self._connection)
                self._connection._out_message = intersystems_iris._OutStream._OutStream(self._connection)
        intersystems_iris.GatewayContext._set_connection(self._connection)
        return
    
    def __load_one_module(self, filename_full, process_wide):
        if filename_full is None: return
        if os.path.isdir(filename_full):
            if filename_full[-1] == os.sep:
                filename_full = os.path.join(filename_full,"__init__.py")
            else:
                filename_full = filename_full + ".__init__.py"
        if filename_full.endswith(".py"):
            filename_path = os.path.dirname(filename_full)
            filename_name = os.path.basename(filename_full)
            filename_short = filename_name.rsplit('.', 1)[0]
            filename_ext = filename_name.rsplit('.', 1)[1]
            with self._sys_modules_lock:
                self.__save_sys_module(process_wide)
                sys.path.insert(0,filename_path)
                importlib.import_module(filename_short)
                del sys.path[0]
                self.__restore_sys_module(process_wide)
            return
        elif filename_full.endswith(".whl"):
            ziplist = zipfile.ZipFile(filename_full).namelist()
            with self._sys_modules_lock:
                self.__save_sys_module(process_wide)
                sys.path.insert(0,filename_full)
                for file in ziplist:
                    if file.endswith(".py"):
                        package_name = file[0:-3].replace("/",".")
                        importlib.import_module(package_name)
                del sys.path[0]
                self.__restore_sys_module(process_wide)
            return
        else:
            # import built-in modules
            package_name = filename_full
            self.__save_sys_module(process_wide)
            importlib.import_module(package_name)
            self.__restore_sys_module(process_wide)
        return

    def __save_sys_module(self, process_wide):
        if process_wide: return
        self._saved_modules = sys.modules.copy()
        return

    def __restore_sys_module(self, process_wide):
        if process_wide: return
        new_modules = [key for key in sys.modules if key not in self._saved_modules] 
        for key in new_modules:
            self._thread_modules[key] = sys.modules[key]
            del sys.modules[key]
        return

    def _load_class(self, class_name):
        if "." in class_name:
            module_name = class_name.rsplit(".", 1)[0]
            class_name_short = class_name.rsplit(".", 1)[1]
        else:
            module_name = None
            class_name_short = class_name
        if class_name_short == "":
            try:
                return self._thread_modules[module_name]
            except Exception as ex:
                pass
            try:
                with self._sys_modules_lock:
                    return sys.modules[module_name]
            except Exception as ex:
                pass
            raise intersystems_iris._GatewayException._GatewayException("Module not found: " + module_name)
        else:
            for module in self._thread_modules:
                try:
                    class_object = getattr(self._thread_modules[module], class_name_short)
                    if module_name == None:
                        return class_object
                    if class_object.__module__ == module_name:
                        return class_object
                except Exception as ex:
                    pass
            with self._sys_modules_lock:
                for module in sys.modules:
                    try:
                        class_object = getattr(sys.modules[module], class_name_short)
                        if module_name == None:
                            return class_object
                        if class_object.__module__ == module_name:
                            return class_object
                    except Exception as ex:
                        pass
            raise intersystems_iris._GatewayException._GatewayException("Class not found: " + class_name)
        return

    def __find_method(self, class_object, method_name):
        try:
            return getattr(class_object, method_name)
        except BaseException as e:
            raise intersystems_iris._GatewayException._GatewayException("Method not found: " + method_name)

    def __get_method_hints(self, method_object, cardinality):
        hints = [object]*cardinality
        try:
            pointer = 0
            params = inspect.signature(method_object).parameters
            for key in params:
                if pointer >= cardinality:
                    break
                if key == "self":
                    continue
                if params[key].kind == inspect.Parameter.POSITIONAL_ONLY or params[key].kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                    hints[pointer] = params[key].annotation
                    pointer += 1
                    continue
                if params[key].kind == inspect.Parameter.VAR_POSITIONAL:
                    while (pointer<cardinality):
                        hints[pointer] = params[key].annotation
                        pointer += 1
                    break
        except Exception as ex:
            pass
        for i in range(len(hints)):
            if not self._is_datatype(hints[i]) and hints[i] != intersystems_iris.IRISList:
                hints[i] = object
        return hints

    def __disconnect(self):
        sequence_number = self.in_message.wire._get_header_count();
        self.out_message.wire._write_header(intersystems_iris._Constant._Constant.MESSAGE_DISCONNECT)
        self.out_message_sequence_number = sequence_number

    def __shutdown(self):
        self._device.close()
        os._exit(0)

    def __ping(self):
        sequence_number = self.in_message.wire._get_header_count();
        self.out_message.wire._write_header(intersystems_iris._Constant._Constant.MESSAGE_PING)
        self.out_message.wire._set(self._server_name)
        self.out_message_sequence_number = sequence_number

    def __benchmark_echo(self):
        sequence_number = self.in_message.wire._get_header_count();
        self.out_message.wire._write_header(intersystems_iris._Constant._Constant.MESSAGE_BENCHMARK_ECHO)
        self.out_message_sequence_number = sequence_number

    def __load_modules(self):
        sequence_number = self.in_message.wire._get_header_count();
        try:
            module_count = self.in_message.wire._get()
            process_wide = False
            if type(module_count)==str and module_count.endswith(":system"):
                module_count = int(module_count[0:-7])
                process_wide = True
            for i in range(module_count):
                self.__load_one_module(self.in_message.wire._get(), process_wide)
            self.out_message.wire._write_header(intersystems_iris._Constant._Constant.MESSAGE_LOAD_MODULES)
            self.out_message_sequence_number = sequence_number
            return
        except Exception as ex:
            self.__process_exception(ex, sequence_number)
            return

    def __dynamic_execute_constructor(self):
        sequence_number = self.in_message.wire._get_header_count();
        try:
            closed_proxy_count = int(self.in_message.wire._get())
            for i in range(closed_proxy_count):
                closed_oref = self.in_message.wire._get()
                del self._connection._oref_registry[closed_oref]
            oref = self.in_message.wire._get()
            class_name = self.in_message.wire._get()
            cardinality = self.in_message.wire._get()
            if class_name == "[]":
                args = self.__unmarshal_parameters(cardinality, [object]*cardinality)
                instance = args
            elif class_name == "()":
                args = self.__unmarshal_parameters(cardinality, [object]*cardinality)
                instance = tuple(args)
            elif class_name == "{}":
                args = self.__unmarshal_parameters(cardinality, [object]*cardinality)
                instance = set(args)
            else:
                class_object = self._load_class(class_name)
                constructor_object = self.__find_method(class_object, "__init__")
                hints = self.__get_method_hints(constructor_object, cardinality)
                args = self.__unmarshal_parameters(cardinality, hints)
                instance = class_object(*args)
            self._connection._oref_registry[oref] = instance
            self._connection._oref_to_class_map[oref] = class_name
            self.__release_closed_iris_object()
            self.out_message.wire._write_header(intersystems_iris._Constant._Constant.MESSAGE_DYNAMIC_EXECUTE_CONSTRUCTOR)
            self.out_message.wire._set("end")
            self.__redirect_output()
            self.out_message_sequence_number = sequence_number
            return
        except Exception as ex:
            self.__process_exception(ex, sequence_number)
            return

    def __dynamic_execute_method(self):
        sequence_number = self.in_message.wire._get_header_count();
        map_listream = False
        try:
            closed_proxy_count = int(self.in_message.wire._get())
            for i in range(closed_proxy_count):
                closed_oref = self.in_message.wire._get()
                del self._connection._oref_registry[closed_oref]
            oref_or_class_name = self.in_message.wire._get()
            method_name = self.in_message.wire._get()
            cardinality = int(self.in_message.wire._get())
            if oref_or_class_name == "**Utility**":
                oref_or_class_name = "iris._GatewayUtility._GatewayUtility"
            if oref_or_class_name == None:
                # built-in function when oref_or_class_name is None
                function_object = getattr(sys.modules["builtins"],method_name)
                hints = self.__get_method_hints(function_object, cardinality)
                args = self.__unmarshal_parameters(cardinality, hints)
                return_value = function_object(*args)
            elif "@" in oref_or_class_name:
                # instance method
                instance = self._connection._oref_registry[oref_or_class_name]
                if method_name.startswith("%"):
                    # special methods: %get, %set, %getall, %setall
                    if method_name == "%get":
                        if cardinality != 1:
                            raise intersystems_iris._GatewayException._GatewayException("Method not found: %get(" + str(cardinality) + ")")
                        key = self.in_message.wire._get()
                        if type(instance) == bytes or type(instance) == bytearray:
                            return_value = instance[key:key+1].decode()
                        else:
                            return_value = instance[key]
                    elif method_name == "%set":
                        if cardinality != 2:
                            raise intersystems_iris._GatewayException._GatewayException("Method not found: %set(" + str(cardinality) + ")")
                        args = self.__unmarshal_parameters(cardinality, [object]*cardinality)
                        try:
                            if type(instance) == bytes or type(instance) == bytearray:
                                return_value = instance[args[0]:args[0]+1].decode()
                            else:
                                return_value = instance[args[0]]
                        except (IndexError, KeyError):
                            return_value = None
                        if type(instance) == bytes or type(instance) == bytearray:
                            instance[args[0]] = ord(args[1][0])
                        else:
                            instance[args[0]] = args[1]
                    elif method_name == "%getall":
                        if cardinality != 0:
                            raise intersystems_iris._GatewayException._GatewayException("Method not found: %getall(" + str(cardinality) + ")")
                        if type(instance) != bytes and type(instance) != bytearray and type(instance) != list and type(instance) != tuple:
                            raise intersystems_iris._GatewayException._GatewayException("Object is not a list or byte array")
                        return_value = instance
                        map_listream = True
                    elif method_name == "%setall":
                        if cardinality != 1:
                            raise intersystems_iris._GatewayException._GatewayException("Method not found: %setall(" + str(cardinality) + ")")
                        value = self.in_message.wire._get()
                        if not isinstance(value, intersystems_iris._IRISOREF._IRISOREF):
                            raise intersystems_iris._GatewayException._GatewayException("Argument for %setall(" + str(cardinality) + ") is invalid")
                        self._connection._close_unused_oref(value._oref)
                        new_listream = self._connection._get_parameter_listream(self.in_message_secondary, self.out_message_secondary, value._oref, instance)
                        return_value = None
                    elif method_name == "%len":
                        return_value = len(instance)
                    else:
                        raise intersystems_iris._GatewayException._GatewayException("Method not found: " + method_name)
                else:
                    # process non-special instance method
                    method_object = getattr(instance, method_name)
                    hints = self.__get_method_hints(method_object, cardinality)
                    args = self.__unmarshal_parameters(cardinality, hints)
                    return_value = method_object(*args)
            else:
                # static method
                class_object = self._load_class(oref_or_class_name)
                method_object = self.__find_method(class_object, method_name)
                hints = self.__get_method_hints(method_object, cardinality)
                args = self.__unmarshal_parameters(cardinality, hints)
                return_value = method_object(*args)
            self.__release_closed_iris_object()
            self.out_message.wire._write_header(intersystems_iris._Constant._Constant.MESSAGE_DYNAMIC_EXECUTE_METHOD)
            self.__marshal_return_value(return_value, map_listream)
            self.out_message.wire._set("end")
            self.__redirect_output()
            self.out_message_sequence_number = sequence_number
            return
        except Exception as ex:
            self.__process_exception(ex, sequence_number)
            return

    def __dynamic_execute_get(self):
        sequence_number = self.in_message.wire._get_header_count();
        try:
            closed_proxy_count = int(self.in_message.wire._get())
            for x in range(closed_proxy_count):
                closed_oref = self.in_message.wire._get()
                del self._connection._oref_registry[closed_oref]
            oref_or_class_name = self.in_message.wire._get()
            property_name = self.in_message.wire._get()
            if "@" in oref_or_class_name:
                instance = self._connection._oref_registry[oref_or_class_name]
                return_value = getattr(instance, property_name)
            else:
                class_object = self._load_class(oref_or_class_name)
                return_value = getattr(class_object, property_name)
            self.__release_closed_iris_object()
            self.out_message.wire._write_header(intersystems_iris._Constant._Constant.MESSAGE_DYNAMIC_EXECUTE_GET)
            self.__marshal_return_value(return_value, False)
            self.__redirect_output()
            self.out_message_sequence_number = sequence_number
            return
        except Exception as ex:
            self.__process_exception(ex, sequence_number)
            return

    def __dynamic_execute_set(self):
        sequence_number = self.in_message.wire._get_header_count();
        try:
            closed_proxy_count = int(self.in_message.wire._get())
            for x in range(closed_proxy_count):
                closed_oref = self.in_message.wire._get()
                del self._connection._oref_registry[closed_oref]
            oref_or_class_name = self.in_message.wire._get()
            property_name = self.in_message.wire._get()
            args = self.__unmarshal_parameters(1,[object])
            property_value = args[0]
            if "@" in oref_or_class_name:
                instance = self._connection._oref_registry[oref_or_class_name]
                setattr(instance, property_name, property_value)
            else:
                class_object = self._load_class(oref_or_class_name)
                setattr(class_object, property_name, property_value)
            self.__release_closed_iris_object()
            self.out_message.wire._write_header(intersystems_iris._Constant._Constant.MESSAGE_DYNAMIC_EXECUTE_SET)
            self.__redirect_output()
            self.out_message_sequence_number = sequence_number
            return
        except Exception as ex:
            self.__process_exception(ex, sequence_number)
            return

    def __redirect_output(self):
        if not self._connection._disable_output_redirect and self._output_redirect._was_written_to():
            self.out_message.wire._set(self._output_redirect._get_buffer_contents())
        else:
            self.out_message.wire._set_undefined()
        if not self._connection._disable_output_redirect and self._error_redirect._was_written_to():
            self.out_message.wire._set(self._error_redirect._get_buffer_contents())
        else:
            self.out_message.wire._set_undefined()
        return

    def __process_exception(self, ex, sequence_number):
        self.out_message.wire._write_header(intersystems_iris._Constant._Constant.MESSAGE_EXCEPTION_RAISED)
        stack_string = traceback.format_exc()
        stack_list = stack_string.split("\n")
        stack_list.pop(0)  # remove first line whic is always 'Traceback (most recent call last):'      
        stack_list.pop(-1) # remove last line which is always blank
        # reverse the stack list, add module name before the exception name
        error_text = ex.__class__.__module__ + "." + "\n".join(reversed(stack_list))
        self.out_message.wire._set(error_text)
        self.out_message_sequence_number = sequence_number
        return
    
    def __unmarshal_parameters(self, cardinality, hints):
        args = [None]*cardinality
        for i in range(cardinality):
            args[i] = self.__unmarshal_one_parameter(hints[i])
        return args

    def __unmarshal_one_parameter(self, hint):
        asBytes = hint != str and hint != object
        value = self.in_message.wire._get(asBytes, True)
        if type(value) == intersystems_iris._IRISOREF._IRISOREF:
            if hint == object:
                return self._connection._map_local_object_from_oref(value._oref)
            else:
                self._connection._close_unused_oref(value._oref)
                value = value._oref
        mode = intersystems_iris.IRIS.MODE_RUNTIME
        locale = self._connection._connection_info._locale
        is_unicode = self._connection._connection_info._is_unicode
        if hint == bool:
            return intersystems_iris.IRIS._convertToBoolean(value, mode, locale)
            pass
        if hint == bytes:
            return intersystems_iris.IRIS._convertToBytes(value, mode, locale, is_unicode)
        if hint == float:
            return intersystems_iris.IRIS._convertToFloat(value, mode, locale)
        if hint == int:
            return intersystems_iris.IRIS._convertToInteger(value, mode, locale)
        if hint == str:
            return intersystems_iris.IRIS._convertToString(value, mode, locale)
        if hint == decimal.Decimal:
            return intersystems_iris.IRIS._convertToDecimal(value, mode, locale)
        if hint == intersystems_iris.IRISList:
            value = intersystems_iris.IRIS._convertToBytes(value, mode, locale, is_unicode)
            return None if value is None else intersystems_iris.IRISList(value, locale, self._connection._connection_info._is_unicode, self._connection._connection_info._compact_double)
        if hint == object:
            return value
        else:
            raise TypeError("unrecognized argument hint type")

    def __marshal_return_value(self, return_value, map_listream):
        if map_listream:
            self.__marshal_listream_return_value(return_value)
            return
        if self._is_datatype(type(return_value)):
            self.out_message.wire._set("value")
            self.out_message.wire._set(return_value, True)
        elif type(return_value) == intersystems_iris.IRISList:
            self.out_message.wire._set("value")
            self.out_message.wire._set(return_value.getBuffer())
        else:
            oref = self._connection._oref_registry_lookup(return_value);
            if oref is None:
                oref = self._connection._map_local_object_to_oref(self.in_message_secondary, self.out_message_secondary, return_value);
            self.out_message.wire._set("object");
            self.out_message.wire._set(oref);
        return

    def __marshal_listream_return_value(self, return_value):
        if type(return_value) == bytes or type(return_value) == bytearray:
            self.out_message.wire._set("bstream")
            self.out_message.wire._set(len(return_value))
            stream_size = len(return_value)
            pointer = 0
            chunk_size = 3000000
            while (pointer < stream_size):
                this_length = stream_size - pointer
                if this_length>chunk_size:
                    this_length = chunk_size
                self.out_message.wire._set(return_value[pointer:pointer+this_length])
                pointer = pointer + this_length
        else:
            self.out_message.wire._set("mlist")
            self.out_message.wire._set(len(return_value))
            for v in return_value:
                if self._is_datatype(type(v)):
                    self.out_message.wire._set_null()
                    self.out_message.wire._set(v)
                else:
                    oref = self._connection._oref_registry_lookup(v);
                    if oref is None:
                        oref = self._connection._map_local_object_to_oref(self.in_message_secondary, self.out_message_secondary, v);
                    self.out_message.wire._set("obj");
                    self.out_message.wire._set(oref);
        return

    def __release_closed_iris_object(self):
        if len(self._connection._iris_object_proxy_closed) > intersystems_iris.IRISConnection.CLOSED_PROXY_UPDATE_THRESHOLD:
            native = intersystems_iris.GatewayContext.getIRIS()
            native._release_closed_iris_object

    @staticmethod
    def _is_datatype(value_type):
        switcher = {
            type(None): True,
            bool: True,
            bytes: True,
            int: True,
            float: True,
            str: True,
            decimal.Decimal: True
        }
        return switcher.get(value_type, False)
