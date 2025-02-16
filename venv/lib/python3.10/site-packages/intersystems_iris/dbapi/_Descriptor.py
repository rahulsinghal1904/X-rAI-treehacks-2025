class _Descriptor:
    def __init__(self, type = 0, precision = 0, scale = None, nullable = 0):
        try:
            type = int(type)
        except (TypeError, ValueError):
            raise TypeError("type must be an integer")
        try:
            precision = int(precision)
        except (TypeError, ValueError):
            raise TypeError("precision must be an integer")
        try:
            nullable = int(nullable)
        except (TypeError, ValueError):
            raise TypeError("nullable must be an integer")

        self.type = type
        self.precision = precision
        self.scale = scale
        self.nullable = nullable
        self.name = None
        self.slotPosition = None
    
    @property
    def scale(self):
        return self.__scale

    @scale.setter
    def scale(self, value):
        if value is not None:
            try:
                value = int(value)
            except (TypeError, ValueError):
                raise TypeError("scale must be an integer")
        
        self.__scale = value

    def cloneMe(self, d):
        if not isinstance(d, _Descriptor):
            raise TypeError("Must clone another _Descriptor")
        
        self.type = d.type
        self.precision = d.precision
        self.scale = d.scale
        self.nullable = d.nullable
        self.name = d.name
        self.slotPosition = d.slotPosition
