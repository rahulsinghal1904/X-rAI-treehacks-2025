class _MessageHeader(object):
    
    HEADER_SIZE = 14

    def __init__(self):
        self.buffer = bytearray(self.HEADER_SIZE)

    def _get_message_length(self):
        return self.__get_4_byte_int_raw(0)

    def _get_message_id(self):
        return self.__get_4_byte_int_raw(4)

    # TODO: remove this method, use _get_message_id()
    def _get_count(self):
        return self.__get_4_byte_int_raw(4)

    def _get_statement_id(self):
        return self.__get_4_byte_int_raw(8) & ~(0x80000000)

    def _get_function_code(self):
        return (self.buffer[12] & 0x00FF) | ((self.buffer[13] & 0x00FF) << 8)

    # TODO: remove this method, use _get_function_code()
    def _get_error(self):
        return (self.buffer[12] & 0x00FF) | ((self.buffer[13] & 0x00FF) << 8)

    @classmethod
    def _set_count(cls, buffer, value):
        cls.__set_raw_4_byte_int(buffer, 4, value)
    
    @classmethod
    def _set_message_length(cls, buffer, value):
        cls.__set_raw_4_byte_int(buffer, 0, value)

    @classmethod
    def _set_statement_id(cls, buffer, value):
        cls.__set_raw_4_byte_int(buffer, 8, value)

    def __get_4_byte_int_raw(self, offset):
        return ((self.buffer[offset] & 0x000000FF) | 
          ((self.buffer[offset + 1] & 0x000000FF) << 8) |
          ((self.buffer[offset + 2] & 0x000000FF) << 16) | 
          ((self.buffer[offset + 3] & 0x000000FF) << 24))

    @staticmethod
    def __set_raw_4_byte_int(buffer, offset, value):
        buffer[offset] = value & 0xFF
        buffer[offset + 1] = (value >> 8) & 0xFF
        buffer[offset + 2] = (value >> 16) & 0xFF
        buffer[offset + 3] = (value >> 24) & 0xFF
