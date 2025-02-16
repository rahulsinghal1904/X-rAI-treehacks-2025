import intersystems_iris

def createConnection(*args, connectionstr=None, hostname=None, port=None,  namespace=None, username=None, password=None, timeout=None, sharedmemory=None, logfile=None):
    '''This method has been deprecated. Please use iris.createConnection(...)'''
    return intersystems_iris.createConnection(*args, connectionstr=connectionstr, hostname=hostname, port=port,  namespace=namespace, username=username, password=password, timeout=timeout, sharedmemory=sharedmemory, logfile=logfile)

def createIris(connection):
    '''This method has been deprecated. Please use iris.createIRIS(...)'''
    return intersystems_iris.createIRIS(connection)
