import intersystems_iris._ListReader

class _BufferReader(intersystems_iris._ListReader._ListReader):
    
    def __init__(self, header, buffer, locale):
        self.header = header
        super().__init__(buffer, locale)

    def _get_header_count(self):
        return self.header._get_count()
