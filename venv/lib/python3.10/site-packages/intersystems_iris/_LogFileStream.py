import io
import os
import datetime
import threading
import platform

class _LogFileStream(object):

    LOG_RECEIVED = 0
    LOG_SENT = 1
    LINE_SEPARATOR = '\n'
    BUFFER_SIZE = io.DEFAULT_BUFFER_SIZE

    def __init__(self, path):
        b_new_file = False
        if (path.startswith("+")):
            b_new_file = True
            path = path[1:]
        if (path.startswith("-")):
            self.b_skip_logging = True
            path = path[1:]
        if b_new_file and os.path.exists(path):
            os.remove(path)
        self.path = path
        self.bytesPerLine = 14 if os.path.splitext(path)[0].endswith("14") else 16
        self.__dump_start()
    
    def __dump_start(self):
        with open(self.path, 'a') as f:
            f.write(
                self.LINE_SEPARATOR 
                + "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
            f.write(
                self.LINE_SEPARATOR 
                + "\tStarted At:     " + datetime.datetime.now().strftime("%b %d, %Y %I:%M:%S %p"))
            f.write(
                self.LINE_SEPARATOR 
                + "\tPython Version: " + platform.python_version())
            f.write(
                self.LINE_SEPARATOR 
                + "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-" 
                + self.LINE_SEPARATOR)

    def __dump_info(self, type, connection):
        if type == self.LOG_SENT:
            type_text = "Sent"
        else:
            type_text = "Received"
        with open(self.path, 'a') as f:
            f.write(
                self.LINE_SEPARATOR + type_text + ": (" 
                + datetime.datetime.now().strftime("%H:%M:%S:%f")[:-3] + ")"
                + " [ThreadID = " + str(threading.get_ident()) + " ]"
                + " [JobNumber = " + str(connection._connection_info._server_job_number) + " ]"
                + " [DeviceID = " + str(id(connection._device)) + " ]"
                + self.LINE_SEPARATOR)

    def _logApi(self, text):
        with open(self.path, 'a') as f:
            f.write(self.LINE_SEPARATOR + text + self.LINE_SEPARATOR)

    def _dump_header(self, buffer, type, connection):
        if buffer is None:
            return
        self.__dump_info(type, connection)
        line = "Header:"
        self._dump(line, buffer, False)

    def _dump_message(self, buffer):
        if buffer is None:
            return
        line = "Message:"
        self._dump(line, buffer, True)

    def _dump(self, line, buffer, is_message):
        asciistr = ""
        i = 0
        f = open(self.path, 'a')
        while i < len(buffer):
            if i % self.bytesPerLine == 0:
                if i != 0:
                    line += "     "
                line += asciistr + self.LINE_SEPARATOR
                asciistr = ""
                line += _LogFileStream.__format_offset(i)
            x = '{:02X}'.format(int(buffer[i]))
            asciistr += _LogFileStream.__convert_to_ascii(int(buffer[i]))
            line += " " + x + " "
            if i % _LogFileStream.BUFFER_SIZE == 0:
                f.write(line)
                line = ""
            i += 1
        line += self.__format_last_line(i)
        line += "     " + asciistr + self.LINE_SEPARATOR
        if is_message:
            line += self.LINE_SEPARATOR
        f.write(line)
        f.close()

    @staticmethod
    def __format_offset(value):
        return '  {:04X}: '.format(value)

    @staticmethod
    def __convert_to_ascii(value):
        if value >= 32 and value < 127:
            return chr(value)
        return "."

    def __format_last_line(self, count):
        spaces = ""
        while count % self.bytesPerLine != 0:
            spaces += "    "
            count += 1
        return spaces
