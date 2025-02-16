import intersystems_iris.dbapi._Descriptor

class _Column(intersystems_iris.dbapi._Descriptor._Descriptor):
    def __init__(self, name, type, precision, scale, nullable, label, tableName, schema, catalog, additionalData, slotPosition):
        name = str(name)
        label = str(label)
        tableName = str(tableName)
        schema = str(schema)
        if catalog is not None:
            catalog = str(catalog)
        try:
            slotPosition = int(slotPosition)
        except (TypeError, ValueError):
            raise TypeError("slotPosition must be an integer")
        if slotPosition < 0:
            raise ValueError("slotPosition must be positive")
        
        super().__init__(type, precision, scale, nullable)
        self.name = name
        self.slotPosition = slotPosition
        self.label = label
        self.tableName = tableName
        self.schema = schema
        self.catalog = catalog
        additionalDataFlags = []
        for v in additionalData:
            additionalDataFlags.append(bool(v))
        for i in range(12 - len(additionalDataFlags)):
            additionalDataFlags.append(False)
        self.isAutoIncrement = bool(additionalDataFlags[0])
        self.isCaseSensitive = bool(additionalDataFlags[1])
        self.isCurrency = bool(additionalDataFlags[2])
        self.isReadOnly = bool(additionalDataFlags[3])
        self.isRowVersion = bool(additionalDataFlags[4])
        self.isUnique = bool(additionalDataFlags[5])
        self.isAliased = bool(additionalDataFlags[6])
        self.isExpression = bool(additionalDataFlags[7])
        self.isHidden = bool(additionalDataFlags[8])
        self.isIdentity = bool(additionalDataFlags[9])
        self.isKeyColumn = bool(additionalDataFlags[10])
        self.isRowId = bool(additionalDataFlags[11])

    def Clone(self):
        additionalData = [self.isAutoIncrement,
                          self.isCaseSensitive,
                          self.isCurrency,
                          self.isReadOnly,
                          self.isRowVersion,
                          self.isUnique,
                          self.isAliased,
                          self.isExpression,
                          self.isHidden,
                          self.isIdentity,
                          self.isKeyColumn,
                          self.isRowId]
        return _Column(self.name, self.type, self.precision, self.scale, self.nullable, self.label, self.tableName, self.schema, self.catalog, additionalData, self.slotPosition)
