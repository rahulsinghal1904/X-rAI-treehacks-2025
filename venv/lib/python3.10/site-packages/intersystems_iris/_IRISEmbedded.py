import os
import intersystems_iris.dbapi._DBAPI as dbapi
from intersystems_iris._IRISConnection import CachedSQL
from intersystems_iris._ConnectionInformation import _ConnectionInformation

def set_var(var, value):
    old = os.environ[var] if var in os.environ else None
    os.environ[var] = value
    return old

def setup_vars(username, password, namespace):
    vars = {}
    if username:
        vars['IRISUSERNAME'] = set_var('IRISUSERNAME', username)
    if password:
        vars['IRISPASSWORD'] = set_var('IRISPASSWORD', password)
    if namespace:
        vars['IRISNAMESPACE'] = set_var('IRISNAMESPACE', namespace)
    return vars

def reset_vars(vars):
    for var in vars:
        if vars[var] is None:
            del os.environ[var]
        else:
            os.environ[var] = vars[var]

class _IRISEmbedded:
    def __init__(self) -> None:
        self._connection_info = _ConnectionInformation()
        self._pre_preparse_cache = {}
        self._preparedCache = {}

    def connect(
        self,
        hostname=None,
        port=None,
        namespace=None,
        username=None,
        password=None,
        autoCommit=None,
        isolationLevel=None,
        **kw
    ):
        current_vars = setup_vars(username, password, namespace)
        try:
            import iris
        except:
            raise dbapi.OperationalError('Not able to connect to IRIS')
        finally:
            reset_vars(current_vars)
        self.iris = iris
        self._connection_info._delimited_ids = 1
        self.autoCommit = autoCommit

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        self.iris = None

    def isClosed(self):
        return self.iris is None

    def cursor(self):
        return dbapi.EmbdeddedCursor(self)

    def _add_pre_preparse_cache(self, sql, cursor):
        preparse_cache_size = 50 # this variable is in ConnectionParameters class of Java and is hardcoded to this value
        if len(self._pre_preparse_cache) < preparse_cache_size:
            if cursor._exec_params == None:
                self._pre_preparse_cache[sql] = CachedSQL(cursor)
    
    def setAutoCommit(self, autoCommit):
        self.autoCommit = autoCommit
        return self.iris.system.SQL.SetAutoCommit(0 if autoCommit is None else 1 if autoCommit else 2)

    def commit(self):
        self.iris.sql.exec('commit')

    def rollback(self):
        self.iris.sql.exec('rollback')
