import sys
import intersystems_iris._PythonGateway

if len(sys.argv) >= 2 and sys.argv[1] == "PythonGateway":
    intersystems_iris._PythonGateway._PythonGateway._main(sys.argv[2:])
else:
    print("Invalid Parameter")