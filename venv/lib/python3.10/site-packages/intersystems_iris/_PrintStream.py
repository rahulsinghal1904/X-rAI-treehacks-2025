import io
import os
import sys
import threading
import intersystems_iris._GatewayContext

class _PrintStream(io.StringIO):

    def __init__(self, type):
        super().__init__()
        self.hmap = {}
        self.type = type

    def _register(self):
        thread_id = threading.get_ident()
        out = ""
        self.hmap[thread_id] = out

    def _unregister(self):
        thread_id = threading.get_ident()
        self.hmap.pop(thread_id, None)
        
    def _was_written_to(self):
        output_buffer = self.__get_out_buffer()
        if output_buffer is None:
            return False
        else:
            return len(output_buffer) != 0

    def __get_out_buffer(self):
        thread_id = threading.get_ident()
        return self.hmap.get(thread_id)

    def __empty_buffer(self):
        thread_id = threading.get_ident()
        del self.hmap[thread_id]
        self._register()

    def _get_buffer_contents(self):
        output_buffer = self.__get_out_buffer()
        if output_buffer is None: return ""
        formatted_output_buffer = "\r\n".join(output_buffer.splitlines())
        if output_buffer.endswith("\n"):
            formatted_output_buffer += "\r\n"
        self.__empty_buffer()
        return formatted_output_buffer

    def write(self, value):
        thread_id = threading.get_ident()
        if thread_id not in self.hmap.keys():
            if self.type == 0:
                sys.__stdout__.write(value)
            else:
                sys.__stderr__.write(value)
            return
        try:
            native = intersystems_iris.GatewayContext.getIRIS()
            formatted_value = "\r\n".join(value.splitlines())
            if value.endswith("\n"): formatted_value += "\r\n"
            native.classMethodVoid("%Net.Remote.Gateway", "%WriteOutput", formatted_value);
            return
        except BaseException as e:
            self.hmap[thread_id] += str(value)
            return
        
