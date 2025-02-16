import threading
import intersystems_iris._IRIS

class _GatewayContext(object):

    __thread_local_connection = {}

    @classmethod
    def _set_connection(cls, connection):
        thread_id = threading.get_ident()
        cls.__thread_local_connection[thread_id] = connection

    @classmethod
    def _get_connection(cls):
        thread_id = threading.get_ident()
        return cls.__thread_local_connection.get(thread_id)

    @classmethod
    def getConnection(cls):
        return cls._get_connection()

    @classmethod
    def getIRIS(cls):
        return intersystems_iris.IRIS(cls._get_connection())
    