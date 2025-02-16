class _ConnectionParameters(object):
    """_ConnectionParameters represents the properties that are implicit to a URL."""
    def __init__(self):
        self.hostname = None
        self.port = None
        self.namespace = None
        self.timeout = None
        self.sharedmemory = False
        self.logfile = None
        self.sslcontext = None
        self.isolationLevel = None
        self.featureOptions = None

    def _set_sharedmemory(self, sharedmemory):
        self.sharedmemory = sharedmemory

    def _get_sharedmemory(self):
        return self.sharedmemory
