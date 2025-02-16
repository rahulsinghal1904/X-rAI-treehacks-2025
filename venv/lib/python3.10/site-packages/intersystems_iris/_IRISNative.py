import sys
import ssl
import intersystems_iris._PythonGateway
import intersystems_iris._IRIS
import intersystems_iris._IRISConnection

def connect(
    *args, 
    connectionstr = None, 
    hostname = None, 
    port = None, 
     namespace = None, 
    username = None, 
    password = None, 
    timeout = None, 
    sharedmemory = None, 
    logfile = None, 
    sslcontext = None, 
    autoCommit=None, 
    isolationLevel=None, 
    featureOptions=None, 
    application_name=None,
):
    '''Return a new open connection to an IRIS instance.

iris.connect(hostname,port,namespace,username,password,timeout,sharedmemory,logfile,sslcontext,autoCommit,isolationLevel,featureOptions)

iris.connect(connectionstr,username,password,timeout,sharedmemory,logfile,sslcontext,autoCommit,isolationLevel,featureOptions)

Parameters may be passed by position or keyword.

Parameters
----------
hostname : Unicode string
    IRIS instance URL
port : int/long
    IRIS superserver port number
namespace : Unicode string
    IRIS namespace
username : Unicode string
    IRIS user
password : Unicode string
    IRIS user password
timeout : int/long, optional
    maximum number of seconds to wait while attempting the connection. defaults to 10
sharedmemory : bool, optional
    set to True to attempt a shared memory connection when the hostname
    is localhost or 127.0.0.1. set to false to force a connection over
    TCP/IP. defaults to True.
logfile : Unicode string, optional
    client-side log file path. the maximum path length is 255 ASCII characters.
connectionstr : Unicode string, optional
    "hostname:port/namespace". use this instead of the hostname, port,
    and namespace
sslcontext : ssl.SSLContext object, optional
    SSL context to be used for SSL connection
    If None, a non-SSL connection will be used
autoCommit : bool, optional
    Indicates if IRIS auto-commit is enabled
isolationLevel : int, optional
    Indicates iris.dbapi isolation level
    PLEASE NOTE: isolationLevel is currently deactivated. Any value passed in is ignored.
featureOptions : int, optional
    With a series of bit flags, it specifies whether certain features are enabled or disabled.

Returns
-------
iris.IRISConnection
    A new client connection to an IRIS server
'''
    has_TypeError = True
    while True:
        if connectionstr != None:
            if len(args) > 0:
                break
            if hostname != None or port != None or namespace != None:
                break
            if username == None or password == None:
                break
            hostname = connectionstr.split(":")[0]
            port = int(connectionstr.split(":")[1].split("/")[0])
            namespace = connectionstr.split(":")[1].split("/")[1]
            has_TypeError = False
            break
        if hostname != None:
            if len(args) > 0:
                break
            if port == None or namespace == None or username == None or password == None:
                break
            has_TypeError = False
            break
        if len(args) <= 0:
            break
        if ":" in args[0]:
            hostname = args[0].split(":")[0]
            port = int(args[0].split(":")[1].split("/")[0])
            namespace = args[0].split(":")[1].split("/")[1]
            consumed = 1
        else:
            if len(args)<3:
                break
            hostname = args[0]
            port = args[1]
            namespace = args[2]
            consumed = 3
        if len(args) <= consumed:
            has_TypeError = False
            break
        if username != None:
            break
        username = args[consumed]
        consumed += 1
        if len(args) <= consumed:
            has_TypeError = False
            break
        if password != None:
            break
        password = args[consumed]
        consumed += 1
        if len(args) <= consumed:
            has_TypeError = False
            break
        if timeout != None:
            break
        timeout = args[consumed]
        consumed += 1
        if len(args) <= consumed:
            has_TypeError = False
            break
        if sharedmemory != None:
            break
        sharedmemory = args[consumed]
        consumed += 1
        if len(args) <= consumed:
            has_TypeError = False
            break
        if logfile != None:
            break
        logfile = args[consumed]
        consumed += 1
        if len(args) <= consumed:
            has_TypeError = False
            break
        if sslcontext != None:
            break
        sslcontext = args[consumed]
        consumed += 1
        if len(args) <= consumed:
            has_TypeError = False
            break
        if autoCommit != None:
            break
        autoCommit = args[consumed]
        consumed += 1
        if len(args) <= consumed:
            has_TypeError = False
            break
        if isolationLevel != None:
            break
        isolationLevel = args[consumed]
        consumed += 1
        if len(args) <= consumed:
            has_TypeError = False
            break
        if featureOptions != None:
            break
        featureOptions = args[consumed]
        consumed += 1
        if len(args) <= consumed:
            has_TypeError = False
            break
        break
    if has_TypeError or username == None or password == None:
        raise TypeError("invalid arguments: hostname, port, namespace, username, password are required. timeout, sharedmemory, logfile are optional.")
    timeout = 10 if timeout == None else timeout
    sharedmemory = True if sharedmemory == None else sharedmemory
    logfile = "" if logfile == None else logfile
    autoCommit = True if autoCommit == None else bool(autoCommit)
    isolationLevel = 1
    featureOptions = intersystems_iris._IRISConnection.Feature.optionDefaultOptions if featureOptions == None else int(featureOptions)
    connection = intersystems_iris.IRISConnection()
    connection._disable_output_redirect = True
    connection._connect(hostname, port, namespace, username, password, timeout, sharedmemory, logfile, sslcontext, autoCommit, isolationLevel, featureOptions, application_name)
    return connection

def createIRIS(connection):
    '''Return a new iris.IRIS object that uses the given connection.

iris.createIRIS(conn)

Throw an exception if the connection is closed.

Parameters
----------
conn : iris.IRISConnection
    connection object to use

Returns
-------
iris.IRIS
    A new IRIS object that uses the given connection.
'''
    return intersystems_iris.IRIS(connection)

def createConnection(*args, **kwargs):
    '''This method has been deprecated. Please use connect() method instead.
'''
    return connect(*args, **kwargs)
