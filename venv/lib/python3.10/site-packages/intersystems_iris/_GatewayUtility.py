import inspect
import platform
import sys
import intersystems_iris._GatewayContext

class _GatewayUtility(object):

    @staticmethod
    def getLanguageName():
        return "Python"

    @staticmethod
    def getLanguageVersion():
        return platform.python_version()

    @staticmethod
    def writeOutput(data):
        connection = intersystems_iris.GatewayContext.getConnection()
        method_object = connection._output_redirect_handler
        if method_object is None:
            print(data, end="", flush=True)
        else:
            args = [ data ]
            method_object(*args)

    @classmethod
    def dumpAllModules(cls, startswith = None):
        if startswith is None: startswith = ""
        lines = {}
        gateway = intersystems_iris.GatewayContext.getConnection()._get_gateway()
        cls._dumpOneModuleCollection(startswith, lines, sys.modules.copy())
        cls._dumpOneModuleCollection(startswith, lines, gateway._thread_modules)
        return "\r\n".join(sorted(lines.keys()))

    @classmethod
    def dumpSysModules(cls, startswith = None):
        if startswith is None: startswith = ""
        lines = {}
        gateway = intersystems_iris.GatewayContext.getConnection()._get_gateway()
        cls._dumpOneModuleCollection(startswith, lines, sys.modules.copy())
        return "\r\n".join(sorted(lines.keys()))

    @classmethod
    def dumpThreadModules(cls, startswith = None):
        if startswith is None: startswith = ""
        lines = {}
        gateway = intersystems_iris.GatewayContext.getConnection()._get_gateway()
        cls._dumpOneModuleCollection(startswith, lines, gateway._thread_modules)
        return "\r\n".join(sorted(lines.keys()))

    @classmethod
    def dumpMethods(cls, class_name = None):
        lines = {}
        gateway = intersystems_iris.GatewayContext.getConnection()._get_gateway()
        class_object = gateway._load_class(class_name)
        methods = inspect.getmembers(class_object, inspect.isfunction)
        for m in range(len(methods)):
            lines[methods[m][0]] = ""
        return "\r\n".join(sorted(lines.keys()))

    @classmethod
    def _dumpOneModuleCollection(cls, startswith, lines, collection):
        for module in collection.values():
            module_name = module.__name__ + "."
            if module_name.startswith(startswith): lines[module_name] = ""
            try:
                classes = inspect.getmembers(module, inspect.isclass)
                for clazz in range(len(classes)):
                    class_name = module_name + classes[clazz][0]
                    if class_name.startswith(startswith): lines[class_name] = ""
            except Exception as ex:
                pass
        return

