import hashlib
from ._BufferReader import _BufferReader
from ._LogFileStream import _LogFileStream
from ._MessageHeader import _MessageHeader
from ._Device import _Device


class _InStream(object):

    SEND_DATA = 0
    BYTE_STREAM = 1
    FETCH_DATA = 2
    OOB_FETCH = 3
    GATEWAY = 4
    IRISNATIVE = 5

    def __init__(self, connection):
        self._connection = connection
        self._device = connection._device
        self._log_stream = connection._log_stream
        self.wire = None

    def _read_message_gateway(self, expected_message_id=-1):
        is_for_gateway = self.__read_message_internal(expected_message_id, -1, _InStream.GATEWAY)
        if expected_message_id != -1 and is_for_gateway:
            raise Exception("Invalid message received")
        if expected_message_id == -1 and not is_for_gateway:
            raise Exception("Invalid message received")
        code = self.wire.header._get_function_code()
        return ((0xFF00 & code) >> 8) - 48

    def _read_message_sysio(self, expected_message_id, allowedErrors):
        while True:
            is_for_gateway = self.__read_message_internal(expected_message_id, -1, _InStream.IRISNATIVE)
            if is_for_gateway:
                self._connection._get_gateway()._dispatch_reentrancy(self)
                continue
            code = self.wire.header._get_function_code() - 51712
            if code != 0 and (allowedErrors == None or code not in allowedErrors):
                error_message = self.wire._get()
                raise RuntimeError(error_message)
            return code

    def _read_message_sql(self, expected_message_id, expected_statement_id=-1, type=0, allowedErrors=None):
        while True:
            is_for_gateway = self.__read_message_internal(expected_message_id, expected_statement_id, type)
            if is_for_gateway:
                self._connection._get_gateway()._dispatch_reentrancy(self)
                continue
            code = self.wire.header._get_function_code()
            return code

    def __read_message_internal(self, expected_message_id, expected_statement_id, call_type):
        high_bit = 1
        is_for_gateway = False
        header = _MessageHeader()
        final_buffer = bytearray()
        while high_bit != 0:
            self.__read_buffer(header.buffer, 0, _MessageHeader.HEADER_SIZE)
            if self._log_stream is not None:
                self._log_stream._dump_header(
                    header.buffer, _LogFileStream.LOG_RECEIVED, self._connection)
            if ((expected_message_id == -1 or expected_message_id) == header._get_message_id()) and (expected_statement_id == -1 or expected_statement_id == header._get_statement_id()):
                is_for_gateway = False
            elif self.__is_header_initizted_from_iris(header, call_type):
                is_for_gateway = True
            else:
                self._connection.close()
                raise Exception("Invalid Message Count: expected: " + str(expected_message_id) +
                                " got: " + str(header._get_message_id()))

            high_bit = (header._MessageHeader__get_4_byte_int_raw(8) & 0x80000000)

            header_msg_length = header._get_message_length()
            if header_msg_length == 0:
                buffer = bytearray(0)
            else:
                buffer = bytearray(header_msg_length)
                self.__read_buffer(buffer, 0, header_msg_length)
                if self._log_stream is not None:
                    self._log_stream._dump_message(buffer)

            if not high_bit and len(final_buffer) == 0:
                self.wire = _BufferReader(
                    header, buffer, self._connection._connection_info._locale)
                return is_for_gateway

            final_buffer.extend(buffer)
        self.wire = _BufferReader(
            header, final_buffer, self._connection._connection_info._locale)
        return is_for_gateway

    def _check_sheader(self, hash, sock):
        sheader = _MessageHeader()
        try:
            timeout = sock.gettimeout()
            sock.settimeout(5)
            self.__read_buffer(sheader.buffer, 0, _MessageHeader.HEADER_SIZE)
            sock.settimeout(timeout)
        except Exception as e:
            return False
        if self._log_stream is not None:
            self._log_stream._dump_header(
                sheader.buffer, _LogFileStream.LOG_RECEIVED, self._connection)
        if sheader._get_message_length() != 0 or sheader._get_error() != 0:
            return False
        header_bytes = sheader.buffer[4:12]
        header_bytes_str = b''
        for i in range(len(header_bytes)):
            header_bytes_str += bytes([header_bytes[i]])
        in_hash = hashlib.sha256(header_bytes_str).hexdigest()
        if len(hash) != len(in_hash):
            return False
        for i in range(len(hash)):
            if hash[i] != in_hash[i]:
                return False
        return True

    def __is_header_initizted_from_iris(self, header, call_type):
        data_length = header._get_message_length()
        message_id = header._get_count()
        statement_id = header._get_statement_id()
        # Gateway initiated message: message-ID is even. if type==GATEWAY, zero is allowed, otherwise, zero is not allowed.
        if (call_type == _InStream.GATEWAY or message_id > 0) and message_id % 2 == 0:
            return True
        function_code = header._get_function_code()
        # Special case for PassPhrase: type=GATEWAY, data_length==0 and function_code==0, middle 8 bytes are for SHA-256 hash code)
        if call_type == _InStream.GATEWAY and data_length == 0 and function_code == 0:
            return True
        # Special case for %Ping (YQ) and %Disconnect (Y4) data_length==0 and message_id==0 and statement_id==0 and function_code=="YQ" or "Y4"
        if data_length == 0 and message_id == 0 and statement_id == 0 and (function_code == 20825 or function_code == 13401):
            return True
        return False

    def __read_buffer(self, buffer, offset, length):
        if self._device is None:
            raise RuntimeError("no longer connected to server")
        data = self._device.recv(length)
        buffer[offset:offset+len(data)] = data
        if len(data) == length:
            return
        cb = len(data)
        while cb < length:
            data = self._device.recv(length-cb)
            if len(data) == 0:
                raise Exception("Server unexpectedly closing communication device")
            buffer[offset+cb:offset+cb+len(data)] = data
            cb += len(data)
        return
