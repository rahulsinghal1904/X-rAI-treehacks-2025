import intersystems_iris._BufferWriter
import intersystems_iris._LogFileStream

class _OutStream(object):

    def __init__(self, connection):
        self._connection = connection
        self._device = connection._device
        self._log_stream = connection._log_stream
        self.wire = intersystems_iris._BufferWriter._BufferWriter(self._connection._connection_info._locale,self._connection._connection_info._is_unicode,self._connection._connection_info._compact_double)

    def _send(self, sequence_number):
        list_len = self.wire._size() - intersystems_iris._MessageHeader._MessageHeader.HEADER_SIZE
        intersystems_iris._MessageHeader._MessageHeader._set_message_length(self.wire.buffer, list_len)
        intersystems_iris._MessageHeader._MessageHeader._set_count(self.wire.buffer, sequence_number)
        if self._device is None:
            raise RuntimeError("no longer connected to server")
        self._device.sendall(self.wire._get_buffer())
        if self._log_stream is not None:
            self._log_stream._dump_header(self.wire._get_header_buffer(), intersystems_iris._LogFileStream._LogFileStream.LOG_SENT, self._connection)
        if list_len > 0:
            if self._log_stream is not None:
                self._log_stream._dump_message(self.wire._get_data_buffer())
        return

