import ctypes
import os

class IOBUFFER_STRUCT(ctypes.Structure):
    _fields_ = [("send_buffer", ctypes.POINTER(ctypes.c_char)),
                ("send_length", ctypes.c_int),
                ("receive_buffer", ctypes.POINTER(ctypes.c_char)),
                ("receive_length", ctypes.c_int),
                ("status", ctypes.c_short)]

class _SharedMemorySocket(object):

    def __init__(self, iris_installation_dir, pid, filename):
        if os.name != "nt":
            self._module = ctypes.cdll.LoadLibrary(os.path.join(iris_installation_dir + "bin", "libxdevshm.so"))
            pointer_type = ctypes.c_int64
        elif ctypes.sizeof(ctypes.c_voidp) > 4:
            self._module = ctypes.cdll.LoadLibrary(os.path.join(iris_installation_dir + "bin", "XDEVshm.dll"))
            pointer_type = ctypes.c_int64
        else:
            self._module = ctypes.cdll.LoadLibrary(os.path.join(iris_installation_dir + "bin", "XDEVshm32.dll"))
            pointer_type = ctypes.c_int32
        self._module.SharedMemory_InitializeWithName.argtypes = tuple([ctypes.c_void_p, ctypes.c_uint, ctypes.c_char_p])
        self._module.SharedMemory_Connect.argtypes = tuple([pointer_type, ctypes.c_uint, ctypes.c_int, ctypes.c_void_p])
        self._module.SharedMemory_IOBuffer_Initialize.argtypes = tuple([pointer_type, ctypes.POINTER(IOBUFFER_STRUCT)])
        self._module.SharedMemory_IOBuffer_Read.argtypes = tuple([pointer_type])
        self._module.SharedMemory_IOBuffer_Write.argtypes = tuple([pointer_type])
        self._module.SharedMemory_Close.argtypes = tuple([pointer_type])
        shm_pointer = ctypes.c_void_p(0)
        self._iobuffer = None
        if self._module.SharedMemory_InitializeWithName(ctypes.byref(shm_pointer), int(pid), ctypes.c_char_p(filename)) != 0:
            raise Exception("Could not initialize shared memory")
        self._shm_pointer = shm_pointer.value
        if self._shm_pointer==0:
            raise Exception("Could not create shared memory")
        return

    def connect(self):
        # flag harcoded to 0. change to 1 to generate shared memory log.
        # timeout hardcoded to 10. user specified timeout was used to TCP/IP and not used here.
        if self._module.SharedMemory_Connect(self._shm_pointer, 0, 10, ctypes.byref(ctypes.c_int(0))) != 0:
            raise Exception("Could not connect shared memory")
        initial_allocation_size = 256
        self._send_buffer = ctypes.create_string_buffer(initial_allocation_size)
        self._receive_buffer = ctypes.create_string_buffer(initial_allocation_size)
        self._iobuffer = IOBUFFER_STRUCT(self._send_buffer,0,self._receive_buffer,0,0)
        if self._module.SharedMemory_IOBuffer_Initialize(self._shm_pointer, ctypes.byref(self._iobuffer)):
            raise Exception("Could not initialize iobuffer for shared memory")
        return

    def close(self):
        if self._module.SharedMemory_Close(self._shm_pointer) != 0:
            raise Exception("Could not close shared memory")
        self._shm_pointer=0
        return

    def sendall(self, buffer):
        if len(buffer) > len(self._send_buffer):
            self._send_buffer = ctypes.create_string_buffer(len(buffer))
            self._iobuffer.send_buffer = self._send_buffer
        self._send_buffer[0:len(buffer)] = buffer
        self._iobuffer.send_length = len(buffer)
        self._iobuffer.status = -1
        self._module.SharedMemory_IOBuffer_Write(self._shm_pointer)
        if self._iobuffer.status != 0:
            raise Exception("Could not send data via shared memory")
        return

    def recv(self, length_requesting):
        if length_requesting > len(self._receive_buffer):
            self._receive_buffer = ctypes.create_string_buffer(length_requesting)
            self._iobuffer.receive_buffer = self._receive_buffer
        self._iobuffer.receive_length = length_requesting
        self._iobuffer.status = -1
        self._module.SharedMemory_IOBuffer_Read(self._shm_pointer)
        if self._iobuffer.status != 0:
            raise Exception("Could not read data via shared memory")
        return self._receive_buffer[0:self._iobuffer.receive_length]

    def settimeout(self, timeout):
        return

    def gethostname(self):
        return ""

    def gethostbyname(self, hostname):
        return ""