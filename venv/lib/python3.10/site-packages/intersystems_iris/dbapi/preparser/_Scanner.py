import enum
import intersystems_iris.dbapi.preparser._Token

class ParseToken(enum.Enum):
    tokUNKN = u'tokUNKN'
    tokBOFL = u'tokBOFL'
    tokEOFL = u'tokEOFL'
    tokEOS = u'tokEOS'
    tokNEWLN = u'tokNEWLN'
    tokTAB = u'tokTAB'
    tokRETURN = u'tokRETURN'
    tokFORMFD = u'tokFORMFD'
    tokLETTER = u'tokLETTER'
    tokDIGIT = u'tokDIGIT'
    tokDOT = u'tokDOT'
    tokARROW = u'tokARROW'
    tokATSIGN = u'tokATSIGN'
    tokDQUOTE = u'tokDQUOTE'
    tokLPARN = u'tokLPARN'
    tokDOLLAR = u'tokDOLLAR'
    tokPERCENT = u'tokPERCENT'
    tokSQUOTE = u'tokSQUOTE'
    tokMINUS = u'tokMINUS'
    tokPLUS = u'tokPLUS'
    tokRPARN = u'tokRPARN'
    tokCOMMA = u'tokCOMMA'
    tokSPACE = u'tokSPACE'
    tokSEMI = u'tokSEMI'
    tokASTER = u'tokASTER'
    tokSLASH = u'tokSLASH'
    tokPOUND = u'tokPOUND'
    tokBSLASH = u'tokBSLASH'
    tokUSCORE = u'tokUSCORE'
    tokEQUAL = u'tokEQUAL'
    tokLESS = u'tokLESS'
    tokGREAT = u'tokGREAT'
    tokLBRACK = u'tokLBRACK'
    tokRBRACK = u'tokRBRACK'
    tokAMPER = u'tokAMPER'
    tokEXCLA = u'tokEXCLA'
    tokQUEST = u'tokQUEST'
    tokCOLON = u'tokCOLON'
    tokVBAR = u'tokVBAR'
    tokLBRACE = u'tokLBRACE'
    tokRBRACE = u'tokRBRACE'
    tokBQUOTE = u'tokBQUOTE'
    tokTILDE = u'tokTILDE'
    tokCRLF = u'tokCRLF'
    tokNBSP = u'tokNBSP'


class CheckPoint(object):
    def __init__(self, p_nIndex, p_nLexemeBegin):
        try:
            p_nIndex = int(p_nIndex)
        except (TypeError, ValueError):
            raise ValueError("p_nIndex must be an integer")
        try:
            p_nLexemeBegin = int(p_nLexemeBegin)
        except (TypeError, ValueError):
            raise ValueError("p_nLexemeBegin must be an integer")

        self.m_nIndex = p_nIndex
        self.m_nLexemeBegin = p_nLexemeBegin

class _Scanner(object):
    # used to convert characters' byte values into ParseTokens
    s_tokenTab = [ParseToken.tokUNKN, ParseToken.tokUNKN, ParseToken.tokUNKN, ParseToken.tokUNKN,
                  ParseToken.tokUNKN, ParseToken.tokUNKN, ParseToken.tokUNKN, ParseToken.tokUNKN,
                  ParseToken.tokUNKN, ParseToken.tokTAB, ParseToken.tokNEWLN, ParseToken.tokUNKN,
                  ParseToken.tokUNKN, ParseToken.tokRETURN, ParseToken.tokUNKN, ParseToken.tokUNKN,
                  ParseToken.tokUNKN, ParseToken.tokUNKN, ParseToken.tokUNKN, ParseToken.tokUNKN,
                  ParseToken.tokUNKN, ParseToken.tokUNKN, ParseToken.tokUNKN, ParseToken.tokUNKN,
                  ParseToken.tokUNKN, ParseToken.tokUNKN, ParseToken.tokUNKN, ParseToken.tokUNKN,
                  ParseToken.tokUNKN, ParseToken.tokUNKN, ParseToken.tokUNKN, ParseToken.tokUNKN,
                  ParseToken.tokSPACE, ParseToken.tokEXCLA, ParseToken.tokDQUOTE, ParseToken.tokPOUND,
                  ParseToken.tokDOLLAR, ParseToken.tokPERCENT, ParseToken.tokAMPER, ParseToken.tokSQUOTE,
                  ParseToken.tokLPARN, ParseToken.tokRPARN, ParseToken.tokASTER, ParseToken.tokPLUS,
                  ParseToken.tokCOMMA, ParseToken.tokMINUS, ParseToken.tokDOT, ParseToken.tokSLASH,
                  ParseToken.tokDIGIT, ParseToken.tokDIGIT, ParseToken.tokDIGIT, ParseToken.tokDIGIT,
                  ParseToken.tokDIGIT, ParseToken.tokDIGIT, ParseToken.tokDIGIT, ParseToken.tokDIGIT,
                  ParseToken.tokDIGIT, ParseToken.tokDIGIT, ParseToken.tokCOLON, ParseToken.tokSEMI,
                  ParseToken.tokLESS, ParseToken.tokEQUAL, ParseToken.tokGREAT, ParseToken.tokQUEST,
                  ParseToken.tokATSIGN, ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER,
                  ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER,
                  ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER,
                  ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER,
                  ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER,
                  ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER,
                  ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLBRACK,
                  ParseToken.tokBSLASH, ParseToken.tokRBRACK, ParseToken.tokARROW, ParseToken.tokUSCORE,
                  ParseToken.tokBQUOTE, ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER,
                  ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER,
                  ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER,
                  ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER,
                  ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER,
                  ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER,
                  ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLETTER, ParseToken.tokLBRACE,
                  ParseToken.tokVBAR, ParseToken.tokRBRACE, ParseToken.tokTILDE, ParseToken.tokUNKN]


    def CurrentTokenGet(self):
        return self.m_CurrentToken

    def CurrentTokenSet(self, token):
        if not isinstance(token, ParseToken):
            raise TypeError("token must be a ParseToken")
        self.m_CurrentToken = token

    def CurrentChar(self):
        if self.m_nIndex >= self.m_nSourceLen:
            return '\0'
        else:
            return self.m_strSource[self.m_nIndex]

    def __init__(self, p_strSource = ""):
        p_strSource = str(p_strSource)

        self.m_strSource = p_strSource
        self.m_strUpperSource = p_strSource.upper()
        self.m_nSourceLen = len(p_strSource)
        self.m_nIndex = -1
        self.m_nLexemeBegin = 0
        self.NextToken()

    def CreateCheckPoint(self):
        return CheckPoint(self.m_nIndex, self.m_nLexemeBegin)

    def RestoreCheckPoint(self, p_CP):
        if not isinstance(p_CP, CheckPoint):
            raise TypeError("p_CP must be a CheckPoint")

        self.m_nIndex = p_CP.m_nIndex - 1
        self.m_nLexemeBegin = p_CP.m_nLexemeBegin
        #  Advance to index and initialize character/token
        self.NextToken()

    def BeginLexeme(self):
        self.m_nLexemeBegin = self.m_nIndex

    def EndLexeme(self):
        t_nLexemeLen = self.m_nIndex - self.m_nLexemeBegin
        if t_nLexemeLen < 0:
            return ""
        else:
            return self.m_strSource[self.m_nLexemeBegin:self.m_nIndex]

    def EndUpperLexeme(self):
        t_nLexemeLen = self.m_nIndex - self.m_nLexemeBegin
        if t_nLexemeLen < 0:
            return ""
        else:
            return self.m_strUpperSource[self.m_nLexemeBegin:self.m_nIndex]

    def NextToken(self):
        if self.m_nIndex != self.m_nSourceLen:
            self.m_nIndex += 1
            if self.m_nIndex >= self.m_nSourceLen:
                self.CurrentTokenSet(ParseToken.tokEOS)
            else:
                self.CurrentTokenSet(self.__Classify(self.m_strSource[self.m_nIndex]))
        return self.CurrentTokenGet()

    def PeekNextToken(self):
        return self.__PeekAhead(1)

    def PeekNextNextToken(self):
        return self.__PeekAhead(2)

    # Not used
    def PeekNextChar(self):
        return self.__PeekAheadChar(1)

    # Not used
    def PeekNextNextChar(self):
        return self.__PeekAheadChar(2)

    #  Skip over whitespace, leaving the current token at the next character following
    #  or End-Of-Source if source exhausted
    def SkipToEndOfLine(self):
        while not self.IsNewLine(self.CurrentTokenGet()):
            self.NextToken()
            if self.CurrentTokenGet() == ParseToken.tokEOS:
                break

    def IsNewLine(self, p_eToken):
        if not isinstance(p_eToken, ParseToken):
            raise TypeError("p_eToken must be a ParseToken")

        if p_eToken in [ParseToken.tokNEWLN, ParseToken.tokCRLF, ParseToken.tokRETURN]:
            return True
        else:
            return False

    #  Note: Aviels spec (PreParser.txt) says the non-breaking space should
    #  be treated as whitespace. The c++ parser however does not comply with
    #  that spec so neither will we
    def IsWhitespace(self, p_eToken):
        if not isinstance(p_eToken, ParseToken):
            raise TypeError("p_eToken must be a ParseToken")

        if p_eToken in [ParseToken.tokTAB, ParseToken.tokNEWLN, ParseToken.tokCRLF, ParseToken.tokSPACE, ParseToken.tokRETURN]:
            return True
        else:
            return False

    #  Skip over whitespace, leaving the current token at the next character following
    #  or End-Of-Source if source exhausted
    def SkipWhitespace(self):
        while self.IsWhitespace(self.CurrentTokenGet()):
            self.NextToken()
            if self.CurrentTokenGet() == ParseToken.tokEOS:
                break

    #  Skip 'n' tokens
    def Skip(self, t_nTokens):
        try:
            t_nTokens = int(t_nTokens)
        except (TypeError, ValueError):
            raise TypeError("t_nTokens must be an integer")

        for i in range(t_nTokens):
            self.NextToken()

    #  <keyword> = <letter> <letter*>
    def Keyword(self):
        self.BeginLexeme()
        while self.CurrentTokenGet() == ParseToken.tokLETTER:
            self.NextToken()
        return self.EndLexeme()

    #  <comment> :- '/' '*' <chars> '*' '/'
    def Comment(self):
        t_bRet = False
        while self.CurrentTokenGet() != ParseToken.tokEOS:
            if ParseToken.tokASTER == self.CurrentTokenGet() and ParseToken.tokSLASH == self.PeekNextToken():
                self.Skip(2)
                t_bRet = True
                break
            self.NextToken()
        return t_bRet

    #  <number> :- [ {'+' | '-'} ] <digit> <digit>* [ . <digit>* ] [ {'E' | 'e'} [ { '+' | '-' } ] <digit> <digit>* ] 
    def Number(self):
        #  Assume successful parse
        m_boolReturn = True
        self.BeginLexeme()
        if ParseToken.tokMINUS == self.CurrentTokenGet() or ParseToken.tokPLUS == self.CurrentTokenGet():
            #  Skip '+' or '-'
            self.NextToken()
        while ParseToken.tokDIGIT == self.CurrentTokenGet():
            #  Skip Digits
            self.NextToken()
        if ParseToken.tokDOT == self.CurrentTokenGet():
            #  Skip '.'
            self.NextToken()
            #  Skip trailing digits
            while ParseToken.tokDIGIT == self.CurrentTokenGet():
                self.NextToken()
        if ParseToken.tokLETTER == self.CurrentTokenGet() and ('E' == self.CurrentChar() or 'e' == self.CurrentChar()):
            #  Skip 'E' or 'e'
            self.NextToken()
            if ParseToken.tokPLUS == self.CurrentTokenGet() or ParseToken.tokMINUS == self.CurrentTokenGet():
                #  Skip '+' or '-'
                self.NextToken()
            #  Must have at least one digit
            if ParseToken.tokDIGIT != self.CurrentTokenGet():
                m_boolReturn = False
            else:
                while ParseToken.tokDIGIT == self.CurrentTokenGet():
                    #  Skip trailing digits
                    self.NextToken()
        return (self.EndLexeme(), m_boolReturn)

    #  0x : <hex digit> <hex digit>*  
    def Hex(self):
        if self.CurrentChar() == '0':
            #  Skip Digits
            c = self.PeekNextChar()
            if (c == 'X') or (c == 'x'):
                self.BeginLexeme()
                #  Have at least a single letter
                while True:
                    self.NextToken()
                    if self.CurrentTokenGet() not in [ParseToken.tokDIGIT, ParseToken.tokLETTER]:
                        break
                return (self.EndLexeme(), True)
        return (self.EndLexeme(), False)

    #  Parse a quoted string. Strings may be delimited by single or double
    #  quotes. The delimited identifiers parameter affects how the output
    #  token is classified
    def String(self, p_bDelimitedIdentifiers):
        p_bDelimitedIdentifiers = bool(p_bDelimitedIdentifiers)

        m_tempToken = None
        self.BeginLexeme()
        #  Remember quote type (single or double)
        t_eToken = self.CurrentTokenGet()
        #  Advance to first string character
        self.NextToken()
        while True:
            if ParseToken.tokEOS == self.CurrentTokenGet():
                break
            #  Embedded Quotes
            if t_eToken == self.CurrentTokenGet():
                if t_eToken == self.PeekNextToken():
                    self.Skip(2)
                    continue 
                #  Final delimiter reached, skip
                self.NextToken()
                break
            else:
                #  Any other character
                self.NextToken()
        t_strRet = self.EndLexeme()
        #  RULE: Note that zero-length quoted string, 
        #  is considered a CONST even when p_bDelimitedIdentifiers == true.
        if "\"\"" == t_strRet or "''" == t_strRet:
            m_tempToken = intersystems_iris.dbapi.preparser._Token.TOKEN.CONSTANT
        else:
            if (t_strRet.startswith("'")) or (False == p_bDelimitedIdentifiers):
                m_tempToken = intersystems_iris.dbapi.preparser._Token.TOKEN.CONSTANT
            else:
                m_tempToken = intersystems_iris.dbapi.preparser._Token.TOKEN.ID
        return (t_strRet, m_tempToken)

    #  Parse a Bracket delimited Identifier string. 
    def ParseBrackets(self, p_bDelimitedIdentifiers):
        p_bDelimitedIdentifiers = bool(p_bDelimitedIdentifiers)

        #  Advance to first string character
        self.NextToken()
        self.BeginLexeme()
        while True:
            if self.CurrentTokenGet() in [ParseToken.tokRBRACK, ParseToken.tokEOS]:
                t_strRet = "\"" + self.EndLexeme() + "\""
                self.NextToken()
                break
            self.NextToken()
        return (t_strRet, intersystems_iris.dbapi.preparser._Token.TOKEN.ID)

    #  <variable> :- Letter [ Letter | Digit | '_' ]*
    def Variable(self):
        self.BeginLexeme()
        if ParseToken.tokLETTER == self.CurrentTokenGet():
            #  Have at least a single letter
            while True:
                self.NextToken()
                if self.CurrentTokenGet() not in [ParseToken.tokDIGIT, ParseToken.tokLETTER, ParseToken.tokUSCORE]:
                    break
        return self.EndLexeme()

    #  <identifier> :- { Letter | '%' | '$' | '_'} [ { Letter | Digit | '_' | '@' | '#' | '$' } ]*
    def Identifier(self):
        self.BeginLexeme()
        t_eToken = self.CurrentTokenGet()
        if self.CurrentTokenGet() in [ParseToken.tokLETTER, ParseToken.tokPERCENT, ParseToken.tokDOLLAR, ParseToken.tokPOUND, ParseToken.tokUSCORE]:
            while self.NextToken() in [ParseToken.tokLETTER, ParseToken.tokDIGIT, ParseToken.tokUSCORE, ParseToken.tokAMPER, ParseToken.tokDOLLAR, ParseToken.tokPOUND, ParseToken.tokATSIGN]:
                pass
        return self.EndLexeme()

    def __PeekAhead(self, p_nChars):
        t_nOffset = self.m_nIndex + p_nChars
        if t_nOffset >= self.m_nSourceLen:
            return ParseToken.tokEOS
        else:
            return self.__Classify(self.m_strSource[t_nOffset])

    def __PeekAheadChar(self, p_nChars):
        t_nOffset = self.m_nIndex + p_nChars
        if t_nOffset >= self.m_nSourceLen:
            return '\0'
        else:
            return self.m_strSource[t_nOffset]

    def __Classify(self, p_ch):
        p_ch = str(p_ch)
        if len(p_ch) != 1:
            raise ValueError("p_ch must be a single character")

        p_ord = ord(p_ch)
        return self.s_tokenTab[p_ord] if p_ord < 128 else ParseToken.tokLETTER

    def checkForNotPredicates(self):
        if not self.IsWhitespace(self.CurrentTokenGet()):
            if self.CurrentTokenGet() in [ParseToken.tokRBRACK, ParseToken.tokLBRACK, ParseToken.tokEQUAL, ParseToken.tokGREAT, ParseToken.tokLESS, ParseToken.tokEXCLA]:
                self.NextToken()
        return self.EndLexeme()


