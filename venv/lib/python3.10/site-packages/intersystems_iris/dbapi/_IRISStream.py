from . import _Message
from .._MessageHeader import _MessageHeader
from .._InStream import _InStream
from .._OutStream import _OutStream


class IRISStream:
    _handle = None
    _binary = False
    _connection = None
    _embedded = False

    def __init__(self, connection, handle, embedded=False):
        self._connection = connection
        self._handle = handle
        self._embedded = embedded
        self._locale = connection._connection_info._locale
        if not self._embedded:
            self._in_message = _InStream(connection)
            self._out_message = _OutStream(connection)
    
    def fetch_embdedded(self):
        result = b'' if self._binary else ''

        stream = self._connection.iris.cls('%Stream.Object')._Open(self._handle)
        while not stream.AtEnd:
            chunk = stream.Read()
            result += bytes(chunk, self._locale) if self._binary else chunk
        
        return result

    def fetch(self):
        if self._embedded:
            return self.fetch_embdedded()
        result = None
        if not self._handle:
            return result

        with self._connection._lock:
            # message header
            self._statement_id = self._connection._get_new_statement_id()
            self._out_message.wire._write_header(_Message.READ_STREAM)
            _MessageHeader._set_statement_id(self._out_message.wire.buffer, self._statement_id)

            # message body
            self._out_message.wire._set(self._handle) # stream handle
            self._out_message.wire._set(-1) # length

            # send message
            sequence_number = self._connection._get_new_sequence_number()
            self._out_message._send(sequence_number)

            code = self._in_message._read_message_sql(sequence_number, self._statement_id, _InStream.BYTE_STREAM, 403)
            if code == 403:
                return None

            result = self._in_message.wire._get_raw()
            if not self._binary:
                result = str(result, self._locale)

        return result


class IRISBinaryStream(IRISStream):
    _binary = True
