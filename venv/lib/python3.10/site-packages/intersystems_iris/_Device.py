import base64
import getpass
import os
import platform
import socket
import struct
import sys
import threading
import intersystems_iris._SharedMemorySocket
import ssl

class _Device(object):

    def __init__(self, _connection, _socket, _sslcontext = None):
        _connection._device = self
        self._connection = _connection
        self._sslcontext = _sslcontext
        if _socket is None:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self._socket = _socket

    def close(self):
        return self._socket.close()

    def settimeout(self,time):
        return self._socket.settimeout(time)

    def connect(self, server_address):
        if self._sslcontext is None:
            return self._socket.connect(server_address)
        else:
            self._socket = self._sslcontext.wrap_socket(self._socket, server_hostname = server_address[0])
            return self._socket.connect(server_address)

    def sendall(self, buffer):
        return self._socket.sendall(buffer)

    def recv(self, len):
        return self._socket.recv(len)

    def gethostname(self):
        return socket.gethostname()

    def gethostbyname(self, hostname):
        return socket.gethostbyname(hostname)

    def is_sharedmemory(self):
        return isinstance(self._socket, intersystems_iris._SharedMemorySocket._SharedMemorySocket)

    def establishSHMSocket(self):
        try:
            iris_bin_dir = self._connection._connection_info._iris_install_dir
            server_job_number = self._connection._connection_info._server_job_number
            filename = b''
            if self._connection._connection_params.hostname != None:
                array = self._connection._connection_params.hostname.split("|")
                filename = array[3] if len(array)>=4 else ""
                filename = filename.upper()
                filename = bytes(filename, "latin-1")
            shmSocket = intersystems_iris._SharedMemorySocket._SharedMemorySocket(iris_bin_dir, server_job_number, filename)
            shmSocket.connect()
            self._socket.close()
            self._socket = shmSocket
        except BaseException as e:
            try:
                shmSocket.close()
            except BaseException as e:
                pass
