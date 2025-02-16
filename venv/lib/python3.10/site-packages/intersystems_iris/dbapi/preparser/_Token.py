import enum
import intersystems_iris.dbapi.preparser._PreParser

class TOKEN(enum.Enum):
    CONSTANT = u'CONSTANT'
    ID = u'ID'
    OP = u'OP'
    UNKNOWN = u'UNKNOWN'
    VAR = u'VAR'
    QUESTION_MARK = u'QUESTION_MARK'
    DTS = u'DTS'
    OPEN_PAREN = u'OPEN_PAREN'
    CLOSE_PAREN = u'CLOSE_PAREN'
    COMMA = u'COMMA'
    NULL = u'NULL'
    NOT = u'NOT'
    IS = u'IS'
    THEN = u'THEN'
    DATATYPE = u'DATATYPE'
    ATSIGN = u'ATSIGN'
    HEX = u'HEX'
    STRFUNCTION = u'STRFUNCTION'
    ELSE = u'ELSE'

class _Token(object):
    """
This class represents a 'token' parsed from the SQL statement. 
It records the classification of the token as well as retaining the original string
"""
    UNDEFINED = 0
    CAST_CHAR32 = 1
    CAST_CHAR128 = 2
    CAST_CHAR512 = 3
    CAST_CHAR = 4
    CAST_INT = 10
    CAST_NUM = 11
    
    def TokenTypeGet(self):
        return self.TokenType

    def TokenTypeSet(self, t):
        if not isinstance(t, TOKEN):
            raise TypeError("t must be a TOKEN")

        self.TokenType = t

    #  TOKENFMT type
    def __init__(self, p_eToken = TOKEN.UNKNOWN, p_strLexeme = "", p_strUpperLexeme = None):
        if not isinstance(p_eToken, TOKEN):
            raise TypeError("p_eToken must be a TOKEN")
        p_strLexeme = str(p_strLexeme)
        if p_strUpperLexeme is not None:
            p_strUpperLexeme = str(p_strUpperLexeme)

        #  The token's classification
        self.TokenType = p_eToken
        
        #  The original string as appears in the SQL statement
        if p_strUpperLexeme is None:
            self.Lexeme = p_strLexeme
            self.UpperLexeme = self.Lexeme
        else:
            if p_strUpperLexeme in intersystems_iris.dbapi.preparser._PreParser._PreParser.s_ReservedKeywords:
                self.Lexeme = p_strUpperLexeme
            else:
                self.Lexeme = p_strLexeme
            self.UpperLexeme = p_strUpperLexeme
        self.m_replaced = False
        
        #  A replaced parameter
        self.m_format = self.UNDEFINED

    def __str__(self):
        return "Token: " + str(self.TokenType) + " Lexeme: " + str(self.Lexeme)

    def UpperEquals(self, p_str):
        return self.UpperLexeme == p_str

    def UpperContains(self, p_str):
        return p_str in self.UpperLexeme

