import intersystems_iris._ListWriter
import intersystems_iris._MessageHeader

class _BufferWriter(intersystems_iris._ListWriter._ListWriter):

    def __init__(self, locale, is_unicode, compact_double):
        super().__init__(locale, is_unicode, compact_double)
        self.offset = intersystems_iris._MessageHeader._MessageHeader.HEADER_SIZE

    def _write_header(self, function_code):
        self.buffer[12] = function_code[0]
        self.buffer[13] = function_code[1]
        self.offset = intersystems_iris._MessageHeader._MessageHeader.HEADER_SIZE

    def _write_header_sysio(self, sysio_code):
        code_int = sysio_code + 49728
        code_bytes = code_int.to_bytes(2, 'little')
        self.buffer[12] = code_bytes[0]
        self.buffer[13] = code_bytes[1]
        self.offset = intersystems_iris._MessageHeader._MessageHeader.HEADER_SIZE

    def _set_connection_info(self, connection_info):
        self._locale = connection_info._locale
        self._is_unicode = connection_info._is_unicode
        self._compact_double = connection_info._compact_double

    def _get_header_buffer(self):
        return self.buffer[0:intersystems_iris._MessageHeader._MessageHeader.HEADER_SIZE]

    def _get_data_buffer(self):
        return self.buffer[intersystems_iris._MessageHeader._MessageHeader.HEADER_SIZE:self.offset]

