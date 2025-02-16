import re
import functools
import enum
import intersystems_iris._IRISList
import intersystems_iris.dbapi._DBAPI
import intersystems_iris.dbapi._Parameter
import intersystems_iris.dbapi.preparser._Token
import intersystems_iris.dbapi.preparser._TokenList
import intersystems_iris.dbapi.preparser._Scanner
from intersystems_iris.dbapi._Parameter import ParameterMode
from intersystems_iris.dbapi.preparser._Token import TOKEN
from intersystems_iris.dbapi.preparser._Scanner import ParseToken

class MultiValuesInsert(Exception):

    def __init__(self, *args: object, query: str, rows: int, params=None) -> None:
        super().__init__(*args)
        self.query = query
        self.rows = rows
        self.params = params
    

# May want to move to its own file eventually
# SQL Statement Types
class StatementType(enum.IntEnum):
    UPDATE = 0
    QUERY = 1
    CALL = 2
    SYNC_COMMIT = 3
    ASYNC_COMMIT = 4
    STREAMS_OFF = 5
    STREAMS_ON = 6
    CALLWITHRESULT = 7
    DDL_ALTER_DROP = 8
    DDL_OTHER = 9
    DIRECT_CALL_QUERY = 10
    DIRECT_CALL_UPDATE = 11
    PREPARED_CALL_QUERY = 12
    PREPARED_CALL_UPDATE = 13
    SQL_DIALECT = 14
    STMT_USE = 15

class PreParseResult(object):
    '''
    A simple structure, returned by _PreParser.PreParse(), containing the parsed statement and statement type
'''
    def __init__(self):
        self.sResult = ""
        self.p_eStmtType = None

class _PreParser(object):
    """
    This is the interface to the SQL PreParser. A string of SQL and a list of parameters ( bound parameters or empty ) is input.
    The processed string is returned along with a count of parameters found and a classfication of the statement type
"""
    #  Class properties
    #  Table for keyword lookups (used when tokenizing the statement)
    s_KeywordTable = {}
    s_KeywordTable["AND"] = TOKEN.OP
    s_KeywordTable["BETWEEN"] = TOKEN.OP
    s_KeywordTable["CHAR"] = TOKEN.DATATYPE
    s_KeywordTable["CHARACTER"] = TOKEN.DATATYPE
    s_KeywordTable["DEC"] = TOKEN.DATATYPE
    s_KeywordTable["DECIMAL"] = TOKEN.DATATYPE
    s_KeywordTable["FLOAT"] = TOKEN.DATATYPE
    s_KeywordTable["IS"] = TOKEN.IS
    s_KeywordTable["LIKE"] = TOKEN.OP
    s_KeywordTable["LONGVARBINARY"] = TOKEN.DATATYPE
    s_KeywordTable["LONGVARCHAR"] = TOKEN.DATATYPE
    s_KeywordTable["NCHAR"] = TOKEN.DATATYPE
    s_KeywordTable["NOT["] = TOKEN.OP
    s_KeywordTable["NOT"] = TOKEN.NOT
    s_KeywordTable["NULL"] = TOKEN.NULL
    s_KeywordTable["NUMBER"] = TOKEN.DATATYPE
    s_KeywordTable["NUMERIC"] = TOKEN.DATATYPE
    s_KeywordTable["NVARCHAR"] = TOKEN.DATATYPE
    s_KeywordTable["RAW"] = TOKEN.DATATYPE
    s_KeywordTable["STARTSWITH"] = TOKEN.OP
    s_KeywordTable["THEN"] = TOKEN.THEN
    s_KeywordTable["ELSE"] = TOKEN.ELSE
    s_KeywordTable["VARBINARY"] = TOKEN.DATATYPE
    s_KeywordTable["VARCHAR"] = TOKEN.DATATYPE
    s_KeywordTable["VARCHAR2"] = TOKEN.DATATYPE
    s_KeywordTable["VARYING"] = TOKEN.DATATYPE
    s_KeywordTable["_"] = TOKEN.NOT
    s_KeywordTable["%SQLUPPER"] = TOKEN.STRFUNCTION
    s_KeywordTable["%STRING"] = TOKEN.STRFUNCTION
    s_KeywordTable["%SQLSTRING"] = TOKEN.STRFUNCTION
    s_KeywordTable["%TRUNCATE"] = TOKEN.STRFUNCTION
    s_KeywordTable["TRUNCATE"] = TOKEN.STRFUNCTION

    #  Table for statement type lookups
    s_StatementTable = {}
    s_StatementTable["ALTER"] = StatementType.DDL_ALTER_DROP
    s_StatementTable["CREATE"] = StatementType.DDL_OTHER
    s_StatementTable["DROP"] = StatementType.DDL_ALTER_DROP
    s_StatementTable["GRANT"] = StatementType.DDL_OTHER
    s_StatementTable["REVOKE"] = StatementType.DDL_OTHER
    s_StatementTable["%CHECKPRIV"] = StatementType.DDL_OTHER
    s_StatementTable["TRAIN"] = StatementType.DDL_OTHER
    s_StatementTable["VALIDATE"] = StatementType.DDL_OTHER
    s_StatementTable["TUNE"] = StatementType.DDL_OTHER
    s_StatementTable["VALIDATE"] = StatementType.DDL_OTHER

    s_StatementTable["USE"] = StatementType.STMT_USE
    s_StatementTable["EXPLAIN"] = StatementType.CALLWITHRESULT

    #  Table for common statement type lookups (SELECT,DELETE,UPDATE,INSERT)
    s_ParsedStatements = {}
    s_ParsedStatements["SELECT"] = StatementType.QUERY
    s_ParsedStatements["INSERT"] = StatementType.UPDATE
    s_ParsedStatements["DELETE"] = StatementType.UPDATE
    s_ParsedStatements["UPDATE"] = StatementType.UPDATE
    
    #  Table for statements to cache, beyond those in s_ParsedStatements
    #  TODO: change this to be a set
    #  Since the server now tells us whether to cache, this may be superfluous
    s_TransactionStatements = {}
    s_TransactionStatements["COMMIT"] = True
    s_TransactionStatements["ROLLBACK"] = True
    s_TransactionStatements["START"] = True
    s_TransactionStatements["%INTRANSACTION"] = True
    s_TransactionStatements["%INTRANS"] = True
    s_TransactionStatements["%BEGTRANS"] = True

    #  keywords for replacing parameters
    s_replaceparm = " SELECT TOP WHERE ON AND OR NOT BETWEEN %STARTSWITH LIKE CASE WHEN ELSE THEN"

    # keywords that should be output all upper case after preparsing
    s_ReservedKeywords = set()
    s_ReservedKeywords.add("%AFTERHAVING")
    s_ReservedKeywords.add("%ALLINDEX")
    s_ReservedKeywords.add("%ALPHAUP")
    s_ReservedKeywords.add("%ALTER")
    s_ReservedKeywords.add("%BEGTRANS")
    s_ReservedKeywords.add("%CHECKPRIV")
    s_ReservedKeywords.add("%CLASSNAME")
    s_ReservedKeywords.add("%CLASSPARAMETER")
    s_ReservedKeywords.add("%DBUGFULL")
    s_ReservedKeywords.add("%DELDATA")
    s_ReservedKeywords.add("%DESCRIPTION")
    s_ReservedKeywords.add("%EXACT")
    s_ReservedKeywords.add("%EXTERNAL")
    s_ReservedKeywords.add("%FILE")
    s_ReservedKeywords.add("%FIRSTTABLE")
    s_ReservedKeywords.add("%FLATTEN")
    s_ReservedKeywords.add("%FOREACH")
    s_ReservedKeywords.add("%FULL")
    s_ReservedKeywords.add("%ID")
    s_ReservedKeywords.add("%IDADDED")
    s_ReservedKeywords.add("%IGNOREINDEX")
    s_ReservedKeywords.add("%IGNOREINDICES")
    s_ReservedKeywords.add("%INLIST")
    s_ReservedKeywords.add("%INORDER")
    s_ReservedKeywords.add("%INTERNAL")
    s_ReservedKeywords.add("%INTEXT")
    s_ReservedKeywords.add("%INTRANS")
    s_ReservedKeywords.add("%INTRANSACTION")
    s_ReservedKeywords.add("%KEY")
    s_ReservedKeywords.add("%MATCHES")
    s_ReservedKeywords.add("%MCODE")
    s_ReservedKeywords.add("%MERGE")
    s_ReservedKeywords.add("%MINUS")
    s_ReservedKeywords.add("%MVR")
    s_ReservedKeywords.add("%NOCHECK")
    s_ReservedKeywords.add("%NODELDATA")
    s_ReservedKeywords.add("%NOFLATTEN")
    s_ReservedKeywords.add("%NOFPLAN")
    s_ReservedKeywords.add("%NOINDEX")
    s_ReservedKeywords.add("%NOLOCK")
    s_ReservedKeywords.add("%NOMERGE")
    s_ReservedKeywords.add("%NOPARALLEL")
    s_ReservedKeywords.add("%NOREDUCE")
    s_ReservedKeywords.add("%NORUNTIME")
    s_ReservedKeywords.add("%NOSVSO")
    s_ReservedKeywords.add("%NOTOPOPT")
    s_ReservedKeywords.add("%NOTRIGGER")
    s_ReservedKeywords.add("%NOUNIONOROPT")
    s_ReservedKeywords.add("%NUMROWS")
    s_ReservedKeywords.add("%ODBCIN")
    s_ReservedKeywords.add("%ODBCOUT")
    s_ReservedKeywords.add("%PARALLEL")
    s_ReservedKeywords.add("%PLUS")
    s_ReservedKeywords.add("%PROFILE")
    s_ReservedKeywords.add("%PROFILE_ALL")
    s_ReservedKeywords.add("%PUBLICROWID")
    s_ReservedKeywords.add("%ROUTINE")
    s_ReservedKeywords.add("%ROWCOUNT")
    s_ReservedKeywords.add("%RUNTIMEIN")
    s_ReservedKeywords.add("%RUNTIMEOUT")
    s_ReservedKeywords.add("%STARTSWITH")
    s_ReservedKeywords.add("%STARTTABLE")
    s_ReservedKeywords.add("%SQLSTRING")
    s_ReservedKeywords.add("%SQLUPPER")
    s_ReservedKeywords.add("%STRING")
    s_ReservedKeywords.add("%TABLENAME")
    s_ReservedKeywords.add("%TRUNCATE")
    s_ReservedKeywords.add("%UPPER")
    s_ReservedKeywords.add("%VALUE")
    s_ReservedKeywords.add("%VID")
    s_ReservedKeywords.add("ABSOLUTE")
    s_ReservedKeywords.add("ADD")
    s_ReservedKeywords.add("ALL")
    s_ReservedKeywords.add("ALLOCATE")
    s_ReservedKeywords.add("ALTER")
    s_ReservedKeywords.add("AND")
    s_ReservedKeywords.add("ANY")
    s_ReservedKeywords.add("ARE")
    s_ReservedKeywords.add("AS")
    s_ReservedKeywords.add("ASC")
    s_ReservedKeywords.add("ASSERTION")
    s_ReservedKeywords.add("AT")
    s_ReservedKeywords.add("AUTHORIZATION")
    s_ReservedKeywords.add("AVG")
    s_ReservedKeywords.add("BEGIN")
    s_ReservedKeywords.add("BETWEEN")
    s_ReservedKeywords.add("BIT")
    s_ReservedKeywords.add("BIT_LENGTH")
    s_ReservedKeywords.add("BOTH")
    s_ReservedKeywords.add("BY")
    s_ReservedKeywords.add("CASCADE")
    s_ReservedKeywords.add("CASE")
    s_ReservedKeywords.add("CAST")
    s_ReservedKeywords.add("CHAR")
    s_ReservedKeywords.add("CHARACTER")
    s_ReservedKeywords.add("CHARACTER_LENGTH")
    s_ReservedKeywords.add("CHAR_LENGTH")
    s_ReservedKeywords.add("CHECK")
    s_ReservedKeywords.add("CLOSE")
    s_ReservedKeywords.add("COALESCE")
    s_ReservedKeywords.add("COLLATE")
    s_ReservedKeywords.add("COMMIT")
    s_ReservedKeywords.add("CONNECT")
    s_ReservedKeywords.add("CONNECTION")
    s_ReservedKeywords.add("CONSTRAINT")
    s_ReservedKeywords.add("CONSTRAINTS")
    s_ReservedKeywords.add("CONTINUE")
    s_ReservedKeywords.add("CONVERT")
    s_ReservedKeywords.add("CORRESPONDING")
    s_ReservedKeywords.add("COUNT")
    s_ReservedKeywords.add("CREATE")
    s_ReservedKeywords.add("CROSS")
    s_ReservedKeywords.add("CURRENT")
    s_ReservedKeywords.add("CURRENT_DATE")
    s_ReservedKeywords.add("CURRENT_TIME")
    s_ReservedKeywords.add("CURRENT_TIMESTAMP")
    s_ReservedKeywords.add("CURRENT_USER")
    s_ReservedKeywords.add("CURSOR")
    s_ReservedKeywords.add("DATE")
    s_ReservedKeywords.add("DEALLOCATE")
    s_ReservedKeywords.add("DEC")
    s_ReservedKeywords.add("DECIMAL")
    s_ReservedKeywords.add("DECLARE")
    s_ReservedKeywords.add("DEFAULT")
    s_ReservedKeywords.add("DEFERRABLE")
    s_ReservedKeywords.add("DEFERRED")
    s_ReservedKeywords.add("DELETE")
    s_ReservedKeywords.add("DESC")
    s_ReservedKeywords.add("DESCRIBE")
    s_ReservedKeywords.add("DESCRIPTOR")
    s_ReservedKeywords.add("DIAGNOSTICS")
    s_ReservedKeywords.add("DISCONNECT")
    s_ReservedKeywords.add("DISTINCT")
    s_ReservedKeywords.add("DOMAIN")
    s_ReservedKeywords.add("DOUBLE")
    s_ReservedKeywords.add("DROP")
    s_ReservedKeywords.add("ELSE")
    s_ReservedKeywords.add("END")
    s_ReservedKeywords.add("ENDEXEC")
    s_ReservedKeywords.add("ESCAPE")
    s_ReservedKeywords.add("EXCEPT")
    s_ReservedKeywords.add("EXCEPTION")
    s_ReservedKeywords.add("EXEC")
    s_ReservedKeywords.add("EXECUTE")
    s_ReservedKeywords.add("EXISTS")
    s_ReservedKeywords.add("EXTERNAL")
    s_ReservedKeywords.add("EXTRACT")
    s_ReservedKeywords.add("FALSE")
    s_ReservedKeywords.add("FETCH")
    s_ReservedKeywords.add("FIRST")
    s_ReservedKeywords.add("FLOAT")
    s_ReservedKeywords.add("FOR")
    s_ReservedKeywords.add("FOREIGN")
    s_ReservedKeywords.add("FOUND")
    s_ReservedKeywords.add("FROM")
    s_ReservedKeywords.add("FULL")
    s_ReservedKeywords.add("GET")
    s_ReservedKeywords.add("GLOBAL")
    s_ReservedKeywords.add("GO")
    s_ReservedKeywords.add("GOTO")
    s_ReservedKeywords.add("GRANT")
    s_ReservedKeywords.add("GROUP")
    s_ReservedKeywords.add("HAVING")
    s_ReservedKeywords.add("HOUR")
    s_ReservedKeywords.add("IDENTITY")
    s_ReservedKeywords.add("IMMEDIATE")
    s_ReservedKeywords.add("IN")
    s_ReservedKeywords.add("INDICATOR")
    s_ReservedKeywords.add("INITIALLY")
    s_ReservedKeywords.add("INNER")
    s_ReservedKeywords.add("INPUT")
    s_ReservedKeywords.add("INSENSITIVE")
    s_ReservedKeywords.add("INSERT")
    s_ReservedKeywords.add("INT")
    s_ReservedKeywords.add("INTEGER")
    s_ReservedKeywords.add("INTERSECT")
    s_ReservedKeywords.add("INTERVAL")
    s_ReservedKeywords.add("INTO")
    s_ReservedKeywords.add("IS")
    s_ReservedKeywords.add("ISOLATION")
    s_ReservedKeywords.add("JOIN")
    s_ReservedKeywords.add("LANGUAGE")
    s_ReservedKeywords.add("LAST")
    s_ReservedKeywords.add("LEADING")
    s_ReservedKeywords.add("LEFT")
    s_ReservedKeywords.add("LEVEL")
    s_ReservedKeywords.add("LIKE")
    s_ReservedKeywords.add("LOCAL")
    s_ReservedKeywords.add("LOWER")
    s_ReservedKeywords.add("MATCH")
    s_ReservedKeywords.add("MAX")
    s_ReservedKeywords.add("MIN")
    s_ReservedKeywords.add("MINUTE")
    s_ReservedKeywords.add("MODULE")
    s_ReservedKeywords.add("NAMES")
    s_ReservedKeywords.add("NATIONAL")
    s_ReservedKeywords.add("NATURAL")
    s_ReservedKeywords.add("NCHAR")
    s_ReservedKeywords.add("NEXT")
    s_ReservedKeywords.add("NO")
    s_ReservedKeywords.add("NOT")
    s_ReservedKeywords.add("NULL")
    s_ReservedKeywords.add("NULLIF")
    s_ReservedKeywords.add("NUMERIC")
    s_ReservedKeywords.add("OCTET_LENGTH")
    s_ReservedKeywords.add("OF")
    s_ReservedKeywords.add("ON")
    s_ReservedKeywords.add("ONLY")
    s_ReservedKeywords.add("OPEN")
    s_ReservedKeywords.add("OPTION")
    s_ReservedKeywords.add("OR")
    s_ReservedKeywords.add("OUTER")
    s_ReservedKeywords.add("OUTPUT")
    s_ReservedKeywords.add("OVERLAPS")
    s_ReservedKeywords.add("PAD")
    s_ReservedKeywords.add("PARTIAL")
    s_ReservedKeywords.add("PREPARE")
    s_ReservedKeywords.add("PRESERVE")
    s_ReservedKeywords.add("PRIMARY")
    s_ReservedKeywords.add("PRIOR")
    s_ReservedKeywords.add("PRIVILEGES")
    s_ReservedKeywords.add("PROCEDURE")
    s_ReservedKeywords.add("PUBLIC")
    s_ReservedKeywords.add("READ")
    s_ReservedKeywords.add("REAL")
    s_ReservedKeywords.add("REFERENCES")
    s_ReservedKeywords.add("RELATIVE")
    s_ReservedKeywords.add("RESTRICT")
    s_ReservedKeywords.add("REVOKE")
    s_ReservedKeywords.add("RIGHT")
    s_ReservedKeywords.add("ROLE")
    s_ReservedKeywords.add("ROLLBACK")
    s_ReservedKeywords.add("ROWS")
    s_ReservedKeywords.add("SCHEMA")
    s_ReservedKeywords.add("SCROLL")
    s_ReservedKeywords.add("SECOND")
    s_ReservedKeywords.add("SECTION")
    s_ReservedKeywords.add("SELECT")
    s_ReservedKeywords.add("SESSION_USER")
    s_ReservedKeywords.add("SET")
    s_ReservedKeywords.add("SHARD")
    s_ReservedKeywords.add("SMALLINT")
    s_ReservedKeywords.add("SOME")
    s_ReservedKeywords.add("SPACE")
    s_ReservedKeywords.add("SQLERROR")
    s_ReservedKeywords.add("SQLSTATE")
    s_ReservedKeywords.add("STATISTICS")
    s_ReservedKeywords.add("SUBSTRING")
    s_ReservedKeywords.add("SUM")
    s_ReservedKeywords.add("SYSDATE")
    s_ReservedKeywords.add("SYSTEM_USER")
    s_ReservedKeywords.add("TABLE")
    s_ReservedKeywords.add("TEMPORARY")
    s_ReservedKeywords.add("THEN")
    s_ReservedKeywords.add("TIME")
    s_ReservedKeywords.add("TIMEZONE_HOUR")
    s_ReservedKeywords.add("TIMEZONE_MINUTE")
    s_ReservedKeywords.add("TO")
    s_ReservedKeywords.add("TOP")
    s_ReservedKeywords.add("TRAILING")
    s_ReservedKeywords.add("TRANSACTION")
    s_ReservedKeywords.add("TRIM")
    s_ReservedKeywords.add("TRUE")
    s_ReservedKeywords.add("UNION")
    s_ReservedKeywords.add("UNIQUE")
    s_ReservedKeywords.add("UPDATE")
    s_ReservedKeywords.add("UPPER")
    s_ReservedKeywords.add("USER")
    s_ReservedKeywords.add("USING")
    s_ReservedKeywords.add("VALUES")
    s_ReservedKeywords.add("VARCHAR")
    s_ReservedKeywords.add("VARYING")
    s_ReservedKeywords.add("WHEN")
    s_ReservedKeywords.add("WHENEVER")
    s_ReservedKeywords.add("WHERE")
    s_ReservedKeywords.add("WITH")
    s_ReservedKeywords.add("WORK")
    s_ReservedKeywords.add("WRITE")

    #  Supported SQL Dialects
    SQL_DIALECT_DEFAULT = 0
    SQL_DIALECT_MSSQL = 1
    SQL_DIALECT_SYBASE = 2

    # methods    
    def CacheOnServerGet(self):
        return self.m_CacheOnServer

    def CacheOnServerSet(self, b):
        b = bool(b)

        self.m_CacheOnServer = b

    def ParamInfoGet(self):
        return self.m_ParamInfo

    def ParamInfoSet(self, s):
        if not isinstance(type(s), intersystems_iris._ListWriter._ListWriter):
            raise TypeError("s must be a _ListWriter")

        self.m_ParamInfo = s

    # Build a PreParser
    def __init__(self, p_bDelimitedIdentifiers = False, addRID = 0, embedded = False):
        p_bDelimitedIdentifiers = bool(p_bDelimitedIdentifiers)
        try:
            addRID = int(addRID)
        except (TypeError, ValueError):
            raise TypeError("addRID must be an interger")

        self.m_addRowID = addRID
        self.m_ExecParamCount = 0
        self.m_ParamInfo = intersystems_iris._IRISList._IRISList()
    
        #  flags for delimited identifier use
        self.m_bDelimitedIdentifiers = p_bDelimitedIdentifiers
        self.m_bBracketSubstitution = False

        #  flag for when statements are cached on the server
        #  potentially irrelevant now because server tells us directly whether it cached the statement
        self.CacheOnServerSet(False)
        
        #  List for tokenizer
        self.m_Tokens = None

        #  The source scanner
        self.m_Scanner = None

        #  flag for when Named Parameters are used
        self.hasNamedParameters = False

        # use to pass UndefinedCount value from methods
        self.m_nUndefinedCount = 0
        
        self.embedded = embedded

    #  Preparse an SQL string returning output statement, parameters, parameter count and statement type
    def PreParse(self, query, p_Parameters):
        
        t_query = query
        while True:
            #  First tokenize the input
            self.Tokenize(t_query)
            #  Convert WITH Clause, can be recursive
            found_with, t_query = self.With(t_query)
            if not found_with:
                break
        
        found_insert, t_query = self.InsertMultiValues(t_query)
        if found_insert:
            self.Tokenize(t_query)

        #  Resolve the tokens and determine output
        return self.Resolve(t_query, p_Parameters)

    def With(self, query):
        try:
            found = False
            new_query = ''
            with_statements = {}

            def _query(find_end_paren=True):
                sub_query = ''
                open_parens = 0
                while tokens.MoveNext():
                    token = tokens.Current()

                    if token.TokenType is TOKEN.OPEN_PAREN:
                        open_parens += 1
                    elif token.TokenType is TOKEN.CLOSE_PAREN:
                        open_parens -= 1
                    
                    sub_query += token.Lexeme
                    sub_query += ' '

                    if token.TokenType is TOKEN.ID and (token.UpperEquals('FROM') or token.UpperEquals('JOIN')):
                        assert tokens.MoveNext() and tokens.Current().TokenType is TOKEN.ID
                        table_name = tokens.Current().Lexeme
                        table_name_upper = tokens.Current().UpperLexeme
                        if table_name_upper in with_statements:
                            sub_query += with_statements[table_name_upper]
                            sub_query += ' AS '
                        sub_query += table_name
                        sub_query += ' '

                    if find_end_paren and open_parens == 0:
                        break
                return sub_query

            tokens = self.m_Tokens.GetEnumerator()
            while tokens.MoveNext():
                token = tokens.Current()

                if token.TokenType is TOKEN.ID and token.UpperEquals("WITH"):
                    found = True
                    break
                else:
                    new_query += token.Lexeme
                    new_query += ' '

            if not found:
                return False, query

            while True:
                assert tokens.MoveNext() and tokens.Current().TokenType is TOKEN.ID
                with_name = tokens.Current().UpperLexeme
                assert tokens.MoveNext() and tokens.Current().TokenType is TOKEN.ID and tokens.Current().UpperContains('AS')
                assert tokens.MoveNext() and tokens.Current().TokenType is TOKEN.OPEN_PAREN
                
                tokens.MovePrevious()
                with_statements[with_name] = _query()
                
                if not tokens.MoveNext() or tokens.Current().TokenType is not TOKEN.COMMA:
                    tokens.MovePrevious()
                    break
            
            assert tokens.MoveNext()
            tokens.MovePrevious()
            new_query += _query(False)
            
            return found, new_query
        except:
            return False, query
        
    def InsertMultiValues(self, query):
        new_query = ''
        values_list = []

        tokens = self.m_Tokens.GetEnumerator()
        while tokens.MoveNext() and not tokens.Current().UpperEquals("INSERT"):
            new_query += tokens.Current().Lexeme + ' '
        if not tokens.MoveNext() or not tokens.Current().UpperEquals("INTO"):
            return False, query
        new_query += 'INSERT INTO '
        while tokens.MoveNext() and not tokens.Current().UpperEquals("VALUES"):
            new_query += tokens.Current().Lexeme + ' '

        values = ''
        params = []
        while tokens.MoveNext():
            assert tokens.Current().TokenType is TOKEN.OPEN_PAREN
            open_parens = 1
            while tokens.MoveNext() or open_parens > 0:
                token = tokens.Current()
                if token.TokenType is TOKEN.OPEN_PAREN:
                    open_parens += 1
                elif token.TokenType is TOKEN.CLOSE_PAREN:
                    open_parens -= 1
                if open_parens == 0:
                    break
                if token.TokenType is TOKEN.CONSTANT:
                    values += '?'
                    param = token.Lexeme
                    if param.__len__ and param[0] == "'" and param[0] == param[-1]:
                        param = param[1: -1]
                    params += [param]
                else:    
                    values += token.Lexeme
                values += ' '
            values_list.append(values)
            values = ''
            if not tokens.MoveNext() or tokens.Current().TokenType is not TOKEN.COMMA:
                break
        
        if len(values_list) <= 1:
            return False, query

        new_query += f" VALUES ({values_list[0]})"
        raise MultiValuesInsert(query=new_query, rows=len(values_list), params=params)

    #  Parse a statement
    def Tokenize(self, p_strInput):
        #  Get a scanner on the sql string
        self.m_Scanner = intersystems_iris.dbapi.preparser._Scanner._Scanner(p_strInput)
        #  Create a new token list
        self.m_Tokens = intersystems_iris.dbapi.preparser._TokenList._TokenList()
        #  Scan the input string and break into tokens
        tokenize_switcher = {
            ParseToken.tokEOS: self.Tokenize_eos,
            ParseToken.tokDOT: self.Tokenize_dot,
            ParseToken.tokDIGIT: self.Tokenize_digit,
            ParseToken.tokMINUS: self.Tokenize_minus,
            ParseToken.tokPLUS: self.Tokenize_plus,
            ParseToken.tokLBRACK: self.Tokenize_lbrack,
            ParseToken.tokDQUOTE: self.Tokenize_quote,
            ParseToken.tokSQUOTE: self.Tokenize_quote,
            ParseToken.tokSLASH: self.Tokenize_slash,
            ParseToken.tokQUEST: functools.partial(self.Tokenize_single, token = TOKEN.QUESTION_MARK, char = "?"),
            ParseToken.tokATSIGN: self.Tokenize_atsign,
            ParseToken.tokLPARN: functools.partial(self.Tokenize_single, token = TOKEN.OPEN_PAREN, char = "("),
            ParseToken.tokRPARN: functools.partial(self.Tokenize_single, token = TOKEN.CLOSE_PAREN, char = ")"),
            ParseToken.tokCOMMA: functools.partial(self.Tokenize_single, token = TOKEN.COMMA, char = ","),
            ParseToken.tokCOLON: self.Tokenize_colon,
            ParseToken.tokLETTER: self.Tokenize_identifier,
            ParseToken.tokPERCENT: self.Tokenize_identifier,
            ParseToken.tokDOLLAR: self.Tokenize_identifier,
            ParseToken.tokUSCORE: self.Tokenize_identifier,
            ParseToken.tokPOUND: self.Tokenize_identifier,
            ParseToken.tokLESS: functools.partial(self.Tokenize_op, check_tokens = [ParseToken.tokEQUAL, ParseToken.tokGREAT]),
            ParseToken.tokEXCLA: self.Tokenize_op,
            ParseToken.tokGREAT: self.Tokenize_op,
            ParseToken.tokASTER: self.Tokenize_op,
            ParseToken.tokEQUAL: functools.partial(self.Tokenize_op, check_tokens = [ParseToken.tokASTER]),
            ParseToken.tokVBAR: self.Tokenize_vbar,
            ParseToken.tokLBRACE: self.Tokenize_lbrace
        }
        while self.m_Scanner.CurrentTokenGet() != ParseToken.tokEOS:
            self.m_Scanner.SkipWhitespace()
            
            tokenize_func = tokenize_switcher.get(self.m_Scanner.CurrentTokenGet(), self.Tokenize_default)
            tokenize_func()

    # generic function for when a token consists of a single character
    def Tokenize_single(self, token, char):
        self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(token, char))
        # Skip this character
        self.m_Scanner.NextToken()

    # default behavior for an unknown character or ParseToken
    def Tokenize_default(self, token = TOKEN.UNKNOWN):
        self.m_Scanner.BeginLexeme()
        self.m_Scanner.NextToken() #  One character unknown
        self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(token, self.m_Scanner.EndLexeme(), self.m_Scanner.EndUpperLexeme()))

    # end of source, do nothing
    def Tokenize_eos(self):
        pass

    # if dot is part of a decimal, parse a number, otherwise default behavior
    def Tokenize_dot(self):
        if ParseToken.tokDIGIT != self.m_Scanner.PeekNextToken():
            self.Tokenize_default()
        else:
            self.Tokenize_digit()

    # either the beginning of hex data, or a number
    def Tokenize_digit(self):
        (t_strNumber, goodParse) = self.m_Scanner.Hex()
        if goodParse:
            self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(TOKEN.HEX, t_strNumber))
            return
                    
        (t_strNumber, goodParse) = self.m_Scanner.Number()
        if not goodParse:
            raise Exception("Invalid Numeric Constant")
        self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(TOKEN.CONSTANT, t_strNumber))

    def Tokenize_minus(self):
        nextToken = self.m_Scanner.PeekNextToken()
        if nextToken == ParseToken.tokMINUS:
            #  Continuation sequence, skip to next line
            self.m_Scanner.Skip(2)
            self.m_Scanner.BeginLexeme()
            self.m_Scanner.SkipToEndOfLine() #  Skip '--' to end of line
            # DVU             m_Tokens.Append(new _Token(TOKEN.UNKNOWN, "/*" + m_Scanner.EndLexeme() + "*/"))
            return
        elif nextToken == ParseToken.tokGREAT:
            #  -> operator
            self.m_Scanner.BeginLexeme()
            self.m_Scanner.Skip(2) #  Skip '->'
            self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(TOKEN.OP, self.m_Scanner.EndLexeme()))
            return
        self.Tokenize_plus("-")

    def Tokenize_plus(self, op_char = "+"):
        #  RULE: Per Aviel, Preparser.txt. A numeric constant may include a preceding "+" or "-" , 
        #  but only if the token before the +/- is an OP or LPAR, otherwise the +/- might be
        #  a monadic operator and should be considered an OP.
        t_eToken = self.m_Tokens.Last().GetValue().TokenTypeGet() if self.m_Tokens.Last() is not None else TOKEN.UNKNOWN
        if t_eToken in [TOKEN.OP, TOKEN.OPEN_PAREN, TOKEN.COMMA] and (self.m_Scanner.PeekNextToken == ParseToken.tokDIGIT or (self.m_Scanner.PeekNextToken() == ParseToken.tokDOT and self.m_Scanner.PeekNextNextToken() == ParseToken.tokDIGIT)):
            #  Scan in number
            (t_strNumber, goodParse) = self.m_Scanner.Number()
            if not goodParse:
                #  TO DO: Replace with ParseException
                raise Exception("Invalid Numeric Constant")
            self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(TOKEN.CONSTANT, t_strNumber))
        else:
            self.Tokenize_single(TOKEN.OP, op_char)

    def Tokenize_lbrack(self):
        if self.m_bBracketSubstitution:
            if not self.m_bDelimitedIdentifiers:
                raise Exception("Delimited identifiers must be enabled on the server to support brackets")
            (t_strString, t_eToken) = self.m_Scanner.ParseBrackets(self.m_bDelimitedIdentifiers)
            self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(t_eToken, t_strString))
            return
        self.Tokenize_default()

    # quotes indicate a string
    def Tokenize_quote(self):
        (t_strString, t_eToken) = self.m_Scanner.String(self.m_bDelimitedIdentifiers)
        self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(t_eToken, t_strString))

    def Tokenize_slash(self):
        if self.m_Scanner.PeekNextToken() == ParseToken.tokASTER:
            # scan in the comment
            self.m_Scanner.BeginLexeme()
            #  Skip '/' '*'
            self.m_Scanner.Skip(2)
            #  Scan in the comment, returns true if successful scan
            if not self.m_Scanner.Comment():
                #  Ran off end of statement
                #  TO DO: Replace with ParseException?
                raise Exception("Unexpected End-Of-Statement")
            self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(TOKEN.UNKNOWN, self.m_Scanner.EndLexeme(), self.m_Scanner.EndUpperLexeme()))
        else:
            self.Tokenize_default(TOKEN.OP) #  '/' operator

    # '@' used for named parameters
    def Tokenize_atsign(self):
        self.m_Scanner.NextToken()
        if self.m_Scanner.CurrentTokenGet() == ParseToken.tokDIGIT:
            raise Exception(("Parameter Name error, First value cannot be a digit: " + self.m_Scanner.CurrentChar()))
        t_strID = self.m_Scanner.Identifier()
        if t_strID == "":
            self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(TOKEN.QUESTION_MARK, "?"))
        else:
            self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(TOKEN.ATSIGN, "@" + t_strID))

    # ':' indicates variables
    def Tokenize_colon(self):
        #  Skip ':'
        self.m_Scanner.NextToken()
        #  Scan in a variable
        t_strVariable = self.m_Scanner.Variable() 
        t_strVariable = ":" + t_strVariable
        self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(TOKEN.VAR, t_strVariable))

    def Tokenize_identifier(self):
        #  Initially, the token is an ID
        t_eToken = TOKEN.ID
        #  Scan in an identifier
        t_strID = self.m_Scanner.Identifier()
        #  Get an uppercase version for lookups
        t_strIDUpper = self.m_Scanner.EndUpperLexeme()
        #  Do a table lookup to identify token
        if t_strIDUpper in self.s_KeywordTable:
            #  Found it, replace ID with specific type
            t_eToken = self.s_KeywordTable[t_strIDUpper]
            if (t_eToken == TOKEN.NOT):
                t_strID = self.m_Scanner.checkForNotPredicates()
                t_strIDUpper = t_strID.upper()
        if t_strID == '%s':
            self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(TOKEN.QUESTION_MARK, "?"))
        else:
            self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(t_eToken, t_strID, t_strIDUpper))

    # used for various operators
    def Tokenize_op(self, check_tokens = [ParseToken.tokEQUAL]):
        self.m_Scanner.BeginLexeme()
        if self.m_Scanner.PeekNextToken() in check_tokens:
            # Check for composite operators (e.g. <=, >=, !=, etc.)
            self.m_Scanner.NextToken()
        self.m_Scanner.NextToken()
        self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(TOKEN.OP, self.m_Scanner.EndLexeme()))

    # either || operator, or unknown
    def Tokenize_vbar(self):
        self.m_Scanner.BeginLexeme()
        t_eToken = TOKEN.OP
        if self.m_Scanner.PeekNextToken() == ParseToken.tokVBAR:
            self.m_Scanner.Skip(2)
        else:
            self.m_Scanner.NextToken()
            t_eToken = TOKEN.UNKNOWN
        self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(t_eToken, self.m_Scanner.EndLexeme(), self.m_Scanner.EndUpperLexeme()))

    def Tokenize_lbrace(self):
        self.m_Scanner.NextToken() #  Skip '{'
        #  Create a checkpoint
        t_CP = self.m_Scanner.CreateCheckPoint()
        self.m_Scanner.SkipWhitespace()
        #  Scan in a potential keyowrd
        t_strKeyword = self.m_Scanner.Keyword()
        if t_strKeyword in ["d", "ds", "t", "ts"]:
            #  Recognized dts token
            self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(TOKEN.UNKNOWN, "{"))
            self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(TOKEN.DTS, t_strKeyword))
        else:
            #  wasn't a dts keyword, restore to check point
            self.m_Scanner.RestoreCheckPoint(t_CP)
            self.m_Tokens.Append(intersystems_iris.dbapi.preparser._Token._Token(TOKEN.UNKNOWN, "{"))

    #  Resolve parameters and perform appropriate substitutions
    def Resolve(self, p_strInput, p_Parameters):
        pOut = PreParseResult()
        pOut.p_eStmtType = StatementType.UPDATE
        if self.ParamInfoGet() == None:
            self.ParamInfoSet(intersystems_iris._IRISList._IRISList())
        else:
            self.ParamInfoGet().clear() # reset buffer
        #  Get an enumerator on the token collection
        t_Enum = self.m_Tokens.GetEnumerator()
        for i in range(1):
            #  If Parameter list is not empty prior then we have bound parameters
            #  from a previous parse (or user inputted?)
            t_bBoundParameters = (len(p_Parameters._params_list) > 0)
            if self.m_Tokens.Count() < 2:
                pOut.sResult = p_strInput
                break #  Resolved
            #  Make first token current (we know we have at least 2 tokens)
            t_Enum.MoveNext()
            t_str = t_Enum.Current().UpperLexeme
            # TODO: comments are not skipped when the enumerator is reset later in the algorithm; does this need to be fixed?  Is this worth fixing?
            while TOKEN.UNKNOWN == t_Enum.Current().TokenTypeGet() and t_str.startswith("/*"):
                t_Enum.MoveNext() # skip comments
                t_str = t_Enum.Current().UpperLexeme
            #  Determine statement types that need further processing 
            if t_str in self.s_ParsedStatements:
                pOut.p_eStmtType = self.s_ParsedStatements[t_str]
                self.CacheOnServerSet(True)
            else:
                if t_str in self.s_StatementTable:
                    pOut.p_eStmtType = self.s_StatementTable[t_str]
                    #  Copy the whole statement to the output
                    if self.m_bBracketSubstitution and self.m_bDelimitedIdentifiers:
                        t_Enum.Reset()
                        while t_Enum.MoveNext():
                            pOut.sResult += t_Enum.Current().Lexeme + " "
                    else:
                        #  Copy the whole statement to the output and ignore tokenizing
                        #  syntax can fail if not exact
                        pOut.sResult += p_strInput
                        if t_str == "EXPLAIN" and pOut.p_eStmtType == StatementType.CALLWITHRESULT:
                            pQuery = p_strInput
                            pAlt = "ShowPlan"
                            pStat = "0"
                            pQuery = pQuery[(pQuery.upper().find("EXPLAIN") + len("EXPLAIN")):] # slice off "EXPLAIN"
                            while t_Enum.MoveNext():
                                if t_Enum.Current().UpperLexeme == "ALT":
                                    pAlt = "ShowPlanAlt"
                                    pQuery = pQuery[(pQuery.upper().find("ALT") + len("ALT")):] # slice off "ALT"
                                elif t_Enum.Current().UpperLexeme == "STAT":
                                    pStat = "1"
                                    pQuery = pQuery[(pQuery.upper().find("STAT") + len("STAT")):] # slice off "STAT"
                                else:
                                    p_Parameters._clear()
                                    p_Parameters._params_list.append(intersystems_iris.dbapi._Parameter._Parameter(pQuery, ParameterMode.REPLACED_LITERAL))
                                    p_Parameters._params_list.append(intersystems_iris.dbapi._Parameter._Parameter(pStat, ParameterMode.REPLACED_LITERAL))
                                    p_Parameters._params_list.append(intersystems_iris.dbapi._Parameter._Parameter(pAlt, ParameterMode.REPLACED_LITERAL))
                                    p_Parameters._params_list.append(intersystems_iris.dbapi._Parameter._Parameter("", ParameterMode.REPLACED_LITERAL))
                                    pOut.sResult = "select %SYSTEM . QUERY_PLAN ( :%qpar(1) , :%qpar(2) , :%qpar(3) , :%qpar(4) ) as Plan"
                                    pOut.p_eStmtType = StatementType.QUERY
                                    self.m_ParamInfo.add(4)
                                    self.m_ParamInfo.add('c')
                                    self.m_ParamInfo.add(2)
                                    self.m_ParamInfo.add('c')
                                    self.m_ParamInfo.add(1)
                                    self.m_ParamInfo.add('c')
                                    self.m_ParamInfo.add(1)
                                    self.m_ParamInfo.add('c')
                                    self.m_ParamInfo.add(1)
                                    self.CacheOnServerSet(False)
                                    return pOut
                    break # Resolved
                else:
                    if t_str in self.s_TransactionStatements:
                        self.CacheOnServerSet(True)
                    else:
                        if t_str.startswith("("):
                            if t_Enum.MoveNext():
                                t_str = t_Enum.Current().UpperLexeme
                                if t_str == "SELECT":
                                    pOut.p_eStmtType = self.s_ParsedStatements[t_str]
                                t_Enum.MovePrevious()
                                self.CacheOnServerSet(True)
                if self.m_Tokens.First().GetValue().UpperEquals("SET"):
                    # Resolve "SET TRANSACTION" and "SET OPTION"
                    t_NewEnum = self.m_Tokens.GetEnumerator()
                    t_NewEnum.MoveNext() # "SET" is current
                    bMoveNext = t_NewEnum.MoveNext() # token after "SET" is current (if any)
                    if bMoveNext and t_NewEnum.Current().UpperEquals("TRANSACTION"):
                        self.CacheOnServerSet(True)
                    if 5 == self.m_Tokens.Count():
                        if bMoveNext and t_NewEnum.Current().UpperEquals("OPTION"):
                            bMoveNext = t_NewEnum.MoveNext()
                            if bMoveNext and t_NewEnum.Current().UpperEquals("BLOB_SUPPORT"):
                                bMoveNext = t_NewEnum.MoveNext()
                                if bMoveNext and t_NewEnum.Current().UpperEquals("="):
                                    bMoveNext = t_NewEnum.MoveNext()
                                    if bMoveNext and t_NewEnum.Current().UpperEquals("1"):
                                        pOut.p_eStmtType = StatementType.STREAMS_ON
                                    elif bMoveNext and t_NewEnum.Current().UpperEquals("0"):
                                        pOut.p_eStmtType = StatementType.STREAMS_OFF
                                    else:
                                        raise Exception("BLOB_SUPPORT must be 0 or 1")
                                else:
                                    raise Exception("Expected '=' after BLOB_SUPPORT")
                            elif bMoveNext and t_NewEnum.Current().UpperEquals("SYNCHRONOUS_COMMIT"):
                                bMoveNext = t_NewEnum.MoveNext()
                                if bMoveNext and t_NewEnum.Current().UpperEquals("="):
                                    bMoveNext = t_NewEnum.MoveNext()
                                    if bMoveNext and t_NewEnum.Current().UpperEquals("1"):
                                        pOut.p_eStmtType = StatementType.SYNC_COMMIT
                                    elif bMoveNext and t_NewEnum.Current().UpperEquals("0"):
                                        pOut.p_eStmtType = StatementType.ASYNC_COMMIT
                                    else:
                                        raise Exception("SYNCHRONOUS_COMMIT must be 0 or 1")
                                else:
                                    raise Exception("Expected '=' after SYNCHRONOUS_COMMIT")
                            else:
                                # aren't there other options beyond BLOB_SUPPORT and SYNCHRONOUS_COMMIT?
                                raise Exception("Unknown SET OPTION")
                    t_Enum.Reset()
                    while t_Enum.MoveNext():
                        pOut.sResult += t_Enum.Current().Lexeme + " "
                    break # Resolved
                # check for Exec and Call statements
                if (not self.CacheOnServerGet()) and self.Exec(pOut, p_Parameters):
                    self.CacheOnServerSet(True)
                    break
                self.m_nUndefinedCount = 0
                if (not self.CacheOnServerGet()) and self.Call(pOut, p_Parameters):
                    self.CacheOnServerSet(True)
                    break
            
            pOut.sResult = ""
            t_Enum.Reset()
            
            self.t_nOpenParen = 0 # keeps track of number of open parentheses
            self.t_nOrdinal = 0 # keeps track of where in p_Parameters new parameters will be inserted
            self.t_nRound = 0 # keeps track of which argument of ROUND is being parsed
            self.t_nRoundNested = 0 # keeps track of any nested parentheses inside of a ROUND argument

            self.orderbyToken = None
            self.lastToken = None # previous token that was resolved (not counting things like parentheses and commas)
           
            t_bQuitLoop = False # currently nothing meaningful is done with this
            bFirstElement = True
            resolve_switcher = {
                TOKEN.QUESTION_MARK: self.Resolve_question_mark,
                TOKEN.ATSIGN: self.Resolve_atsign,
                TOKEN.HEX: self.Resolve_hex,
                TOKEN.ID: functools.partial(self.Resolve_id, stmtType = pOut.p_eStmtType),
                TOKEN.STRFUNCTION: self.Resolve_strfunction,
                TOKEN.DATATYPE: self.Resolve_datatype,
                TOKEN.OPEN_PAREN: self.Resolve_open_paren,
                TOKEN.CLOSE_PAREN: self.Resolve_close_paren,
                TOKEN.OP: self.Resolve_op,
                TOKEN.CONSTANT: self.Resolve_constant,
                # TOKEN.NULL: self.Resolve_null,
                TOKEN.COMMA: self.Resolve_comma
            }
            while (not t_bQuitLoop) and t_Enum.MoveNext():
                t_Token = t_Enum.Current()
                if bFirstElement:
                    bFirstElement = False
                    if t_Token.UpperEquals("{"):
                        raise Exception("'{' encountered at the beginning of the statement") # , "37000", 37000)

                resolve_func = resolve_switcher.get(t_Token.TokenTypeGet(), None)
                if resolve_func is not None:
                    t_bQuitLoop = resolve_func(p_Parameters, t_Enum, t_Token, t_bBoundParameters)

                if t_Token.TokenTypeGet() not in [TOKEN.COMMA, TOKEN.OPEN_PAREN, TOKEN.CLOSE_PAREN]:
                    self.lastToken = t_Token
            
            # now that we've resolved every token, need to replace parameters with ":%qpar" syntax
            t_Enum.Reset()
            t_nParamIndex = 1
            t_count = 0

            bExecute = False
            while t_Enum.MoveNext():
                t_count += 1
                t_Token = t_Enum.Current()

                # exclude an initial "EXECUTE" from the final preparsed statement
                if t_Token.UpperEquals("EXECUTE"):
                    bExecute = True
                if (2 == t_count) and (bExecute):
                    if t_Token.UpperEquals("SELECT"):
                        pOut.p_eStmtType = StatementType.QUERY
                        pOut.sResult = ""
                    elif t_Token.UpperEquals("UPDATE") or t_Token.UpperEquals("INSERT"):
                        pOut.p_eStmtType = StatementType.UPDATE
                        pOut.sResult = ""

                if TOKEN.QUESTION_MARK == t_Token.TokenTypeGet() or TOKEN.ATSIGN == t_Token.TokenTypeGet():
                    pOut.sResult += "?" if self.embedded else ":%qpar({0})".format(t_nParamIndex)
                    t_nParamIndex += 1
                    if t_count < t_Enum.Count():
                        pOut.sResult += ' '
                else:
                    pOut.sResult += t_Token.Lexeme
                    if t_count < t_Enum.Count():
                        pOut.sResult += ' '
                    if t_Token.UpperEquals("SELECT"):
                        pOut.sResult = self.appendRowId(pOut.sResult)
                    if t_Token.UpperEquals("ORDER"):
                        haveMore = t_Enum.MoveNext()
                        if haveMore:
                            pOut.sResult += t_Enum.Current().Lexeme
                            if t_count < t_Enum.Count():
                                pOut.sResult += ' '
                            if t_Enum.Current().UpperEquals("BY"):
                                pOut.sResult = self.appendIdAdded(pOut.sResult)
        # create paramInfo $list to be passed to server
        length = 0
        if len(p_Parameters._params_list) > 0:
            item = p_Parameters._params_list[0]
            if isinstance(item, list) or isinstance(item, tuple):
                length = len(item)
            else:
                length = len(p_Parameters._params_list)
        self.m_ParamInfo.add(length - self.m_ExecParamCount) #len(p_Parameters._params_list)
        if length - self.m_ExecParamCount > 0:
            t_Enum.Reset()
            nParamIndex = 1
            p_Parameters._user_index = [-1]
            while t_Enum.MoveNext():
                if TOKEN.QUESTION_MARK == t_Enum.Current().TokenTypeGet() or TOKEN.ATSIGN == t_Enum.Current().TokenTypeGet():
                    if t_Enum.Current().m_replaced:
                        self.m_ParamInfo.add('c')
                    else:
                        self.m_ParamInfo.add('?')
                        p_Parameters._add_user_param(None)
                        p_Parameters._user_index.append(nParamIndex - 1)
                    self.m_ParamInfo.add(t_Enum.Current().m_format)
                    nParamIndex += 1
                    if nParamIndex == length + 1:
                        break
        return pOut

    # '?' represents a parameter; adds a parameter to p_Parameters if none were provided
    def Resolve_question_mark(self, p_Parameters, t_Enum, t_Token, t_bBoundParameters):
        self.t_nOrdinal += 1
        if not t_bBoundParameters:
            p_Parameters._params_list.append(intersystems_iris.dbapi._Parameter._Parameter("?", ParameterMode.INPUT, '?'))
        return False

    # "@" used for named parameters
    def Resolve_atsign(self, p_Parameters, t_Enum, t_Token, t_bBoundParameters):
        self.hasNamedParameters = True
        self.t_nOrdinal += 1
        if (not t_bBoundParameters) or len(p_Parameters._params_list) == 0:
            p_Parameters._params_list.append(intersystems_iris.dbapi._Parameter._Parameter(t_Token.Lexeme, ParameterMode.UNKNOWN))
        else:
            if not matchUpParam(p_Parameters, t_Token.Lexeme, len(p_Parameters._params_list)):
                p_Parameters._params_list.append(intersystems_iris.dbapi._Parameter._Parameter(t_Token.Lexeme, ParameterMode.UNKNOWN))
        return False

    # replaces a hex literal with a parameter
    def Resolve_hex(self, p_Parameters, t_Enum, t_Token, t_bBoundParameters):
        self.t_nOrdinal += 1
        cp = intersystems_iris.dbapi._Parameter._Parameter(bytes.fromhex(t_Token.Lexeme[2:]), ParameterMode.REPLACED_LITERAL, '?', type = intersystems_iris.dbapi._DBAPI.SQLType.BINARY)
        p_Parameters._params_list.append(cp)
        t_Token.TokenTypeSet(TOKEN.QUESTION_MARK)
        return False

    def Resolve_id(self, p_Parameters, t_Enum, t_Token, t_bBoundParameters, stmtType):
        if self.orderbyToken is not None and t_Enum.Current().UpperEquals("UNION"):
            self.orderbyToken = None
        if self.lastToken is not None and self.lastToken == self.orderbyToken:
            self.orderbyToken = t_Token
            self.lastToken = t_Token
            return False
        # ORDER follows parameters, quit early
        if t_Token.UpperEquals("ORDER"):
            t_NewEnum = t_Enum.Clone()
            if t_NewEnum.MoveNext():
                t_NewToken = t_NewEnum.Current()
                if t_NewToken.UpperEquals("BY"):
                    self.orderbyToken = t_NewToken
                    if self.t_nOpenParen == 0:
                        return False
                    else:
                        while t_Enum.MoveNext():
                            if t_Enum.Current().TokenTypeGet() == TOKEN.CLOSE_PAREN:
                                self.t_nOpenParen -= 1
                                break
                            elif (TOKEN.ID == t_Enum.Current().TokenTypeGet()) and (t_Enum.Current().UpperEquals("UNION")):
                                break
        # JSON_TABLE should have no literal substitution
        if t_Token.UpperContains("JSON_") or t_Token.UpperContains("_JSON"):
            startParen = self.t_nOpenParen
            while t_Enum.MoveNext():
                if t_Enum.Current().TokenTypeGet() == TOKEN.OPEN_PAREN:
                    self.t_nOpenParen += 1
                if t_Enum.Current().TokenTypeGet() == TOKEN.CLOSE_PAREN:
                    self.t_nOpenParen -= 1
                if self.t_nOpenParen == startParen:
                    break
        # ROUND special handling for second parameter
        if t_Token.UpperEquals("ROUND"):
            if stmtType == StatementType.QUERY and self.t_nRound == 0:
                self.t_nRound = 1
        # DATEPART with first parameter sent as is, not a literal
        if t_Token.UpperEquals("DATEPART") or t_Token.UpperEquals("TIMESTAMPADD") or t_Token.UpperEquals("TIMESTAMPDIFF"):
            if t_Enum.MoveNext():
                if t_Enum.Current().TokenTypeGet() == TOKEN.OPEN_PAREN:
                    while t_Enum.MoveNext():
                        if t_Enum.Current().TokenTypeGet() == TOKEN.CONSTANT:
                            t_Enum.Current().TokenTypeSet(TOKEN.ID)
                            break
                        if t_Enum.Current().TokenTypeGet() in [TOKEN.COMMA, TOKEN.CLOSE_PAREN]:
                            break
                else:
                    t_Enum.MovePrevious()
        return False

    # I honestly have no idea why this method does what it does
    def Resolve_strfunction(self, p_Parameters, t_Enum, t_Token, t_bBoundParameters):
        parenLevel = 0
        t_TokenLast = None
        inOrderBy = False
        while t_Enum.MoveNext():
            if t_TokenLast is not None and t_TokenLast.UpperLexeme == "ORDER":
                if t_Enum.Current().UpperLexeme == "BY":
                    inOrderBy = True
            t_TokenLast = t_Enum.Current()
            if parenLevel == 1 and t_Enum.Current().TokenTypeGet() == TOKEN.COMMA:
                while t_Enum.MoveNext():
                    if t_Enum.Current().TokenTypeGet() == TOKEN.CONSTANT:
                        t_Enum.Current().TokenTypeSet(TOKEN.ID)
                        if parenLevel == 1:
                            break
                    elif t_Enum.Current().TokenTypeGet() == TOKEN.OPEN_PAREN:
                        parenLevel += 1
                    elif t_Enum.Current().TokenTypeGet() == TOKEN.CLOSE_PAREN:
                        parenLevel -= 1
                        if parenLevel == 1:
                            break
            elif t_Enum.Current().TokenTypeGet() == TOKEN.OPEN_PAREN:
                parenLevel += 1
            elif t_Enum.Current().TokenTypeGet() == TOKEN.CLOSE_PAREN:
                if parenLevel == 1:
                    break
                parenLevel -= 1
            elif t_Enum.Current().TokenTypeGet() == TOKEN.CONSTANT:
                bSubstitute = not inOrderBy
                if parenLevel > 1:
                    t_Enum.MovePrevious()
                    if TOKEN.OPEN_PAREN == t_Enum.Current().TokenTypeGet():
                        t_Enum.MoveNext()
                        t_Enum.MoveNext()
                        if TOKEN.CLOSE_PAREN == t_Enum.Current().TokenTypeGet():
                            bSubstitute = False
                        t_Enum.MovePrevious()
                    else:
                        t_Enum.MoveNext()
                if bSubstitute:
                    t_Token = t_Enum.Current()
                    self.t_nOrdinal = self.DynamicVariable(t_bBoundParameters, t_Token, self.t_nOrdinal, p_Parameters)
            if parenLevel == 0:
                break
        return False

    # Skips over the data type's arguments (if any)
    def Resolve_datatype(self, p_Parameters, t_Enum, t_Token, t_bBoundParameters):
        t_NewEnum = t_Enum.Clone()
        if t_NewEnum.MoveNext():
            t_NewToken = t_NewEnum.Current()
            if TOKEN.OPEN_PAREN == t_NewToken.TokenTypeGet():
                while t_NewEnum.MoveNext():
                    t_NewToken = t_NewEnum.Current()
                    if t_NewToken.TokenTypeGet() == TOKEN.CLOSE_PAREN:
                        break
                t_Enum = t_NewEnum
        return False

    # generally just increments t_nOpenParen (and t_nRoundNested, when relevant), 
    # but also checks for "((CONSTANT))" syntax (this is a way you can get the preparser to not replace a constant with a parameter)
    def Resolve_open_paren(self, p_Parameters, t_Enum, t_Token, t_bBoundParameters):
        self.t_nOpenParen += 1
        t_NewEnum = t_Enum.Clone()
        if self.t_nRound > 0:
            self.t_nRoundNested += 1
        if t_NewEnum.MoveNext():
            t_NewToken = t_NewEnum.Current()
            if TOKEN.OPEN_PAREN == t_NewToken.TokenTypeGet():
                self.t_nOpenParen += 1
                if t_NewEnum.MoveNext():
                    t_NewToken = t_NewEnum.Current()
                    bCurlyBrace = (t_NewToken.Lexeme == "{")
                    if TOKEN.CONSTANT == t_NewToken.TokenTypeGet() or bCurlyBrace:
                        if t_NewEnum.MoveNext():
                            t_NewToken = t_NewEnum.Current()
                            if bCurlyBrace:
                                while t_NewToken.Lexeme != "}":
                                    if not t_NewEnum.MoveNext():
                                        bCurlyBrace = False
                                        break
                                    t_NewToken = t_NewEnum.Current()
                                bCurlyBrace = False
                                if not t_NewEnum.MoveNext():
                                    return False
                                t_NewToken = t_NewEnum.Current()
                            if TOKEN.CLOSE_PAREN == t_NewToken.TokenTypeGet():
                                self.t_nOpenParen -= 1
                                if t_NewEnum.MoveNext():
                                    t_NewToken = t_NewEnum.Current()
                                    if TOKEN.CLOSE_PAREN == t_NewToken.TokenTypeGet():
                                        self.t_nOpenParen -= 1
                                        t_Enum = t_NewEnum
                                        if self.t_nRound > 0:
                                            self.t_nRoundNested -= 1
        return False
         
    # decrements t_nOpenParen (and t_nRoundNested, when relevant)
    def Resolve_close_paren(self, p_Parameters, t_Enum, t_Token, t_bBoundParameters):
        if self.t_nRound > 0:
            self.t_nRoundNested -= 1
        self.t_nOpenParen -= 1
        return False

    # skips over "(CONSTANT)" after an operator (another way to get the preparser to not replace a constant with a parameter)
    def Resolve_op(self, p_Parameters, t_Enum, t_Token, t_bBoundParameters):
        t_NewEnum = t_Enum.Clone()
        if t_NewEnum.MoveNext():
            t_NewToken = t_NewEnum.Current()
            if TOKEN.OPEN_PAREN == t_NewToken.TokenTypeGet():
                if t_NewEnum.MoveNext() and t_NewEnum.Current().TokenTypeGet() == TOKEN.CONSTANT:
                    if t_NewEnum.MoveNext() and t_NewEnum.Current().TokenTypeGet() == TOKEN.CLOSE_PAREN:
                        t_Enum = t_NewEnum
        return False

    def Resolve_constant(self, p_Parameters, t_Enum, t_Token, t_bBoundParameters):
        # the second argument (and beyond?) of ROUND should not be replaced with a parameter
        if (self.t_nRound == 2) and (self.t_nRoundNested == 1):
            t_Enum.MoveNext()
            if TOKEN.COMMA != t_Enum.Current().TokenTypeGet():
                self.t_nRound = 0
                self.t_nRoundNested = 0
            t_Enum.MovePrevious()
            return False
        # Detect and Skip IN clause
        if self.lastToken is not None:
            if self.lastToken == self.orderbyToken:
                self.orderbyToken = t_Token
                self.lastToken = t_Token
                return False
        t_NewEnum = t_Enum.Clone()

        # not 100% sure what this block does
        if t_NewEnum.MoveNext() and t_NewEnum.Current().TokenTypeGet() == TOKEN.CLOSE_PAREN:
            t_NewEnum.MovePrevious()
            if t_NewEnum.MovePrevious() and (t_NewEnum.Current().Lexeme[0] == '-'):
                t_NewEnum.MovePrevious()
            if t_Enum.Current() is not None and t_NewEnum.Current().TokenTypeGet() == TOKEN.OPEN_PAREN:
                t_NewEnum.MovePrevious()
                if t_Enum.Current() is not None:
                    if TOKEN.ID != t_NewEnum.Current().TokenTypeGet() or (t_NewEnum.Current().UpperLexeme in _PreParser.s_replaceparm):
                        t_Enum.MoveNext()
                        return False

        # determine format the constant will be sent to the server in (stored in paramInfo at the end of Resolve())
        if t_Enum.Current() is not None:
            c = t_Enum.Current().Lexeme
            if c[0] == '\'' or c[0] == '"':
                if c[-1] != c[0]:
                    raise Exception("unmatched quote in " + t_Enum.Current().Lexeme)
                t_Enum.Current().m_format = intersystems_iris.dbapi.preparser._Token._Token.CAST_CHAR
            else:
                isInt = True
                for ii in range(len(c)):
                    if c[ii] in ['.', 'e', 'E']:
                        isInt = False
                        break
                if isInt:
                    if (21 < len(c)) or ((c[0] == '-') and (20 < len(c))):
                        t_Enum.Current().m_format = intersystems_iris.dbapi.preparser._Token._Token.CAST_CHAR
                    else:
                        t_Enum.Current().m_format = intersystems_iris.dbapi.preparser._Token._Token.CAST_INT
                else:
                    t_Enum.Current().m_format = intersystems_iris.dbapi.preparser._Token._Token.CAST_NUM
        self.t_nOrdinal = self.DynamicVariable(t_bBoundParameters, t_Token, self.t_nOrdinal, p_Parameters)
        return False

    # not sure why this does what it does
    def Resolve_null(self, p_Parameters, t_Enum, t_Token, t_bBoundParameters):
        t_NewEnum = t_Enum.Clone()
        if t_NewEnum.MovePrevious():
            t_PreviousToken = t_NewEnum.Current()
            if t_PreviousToken.TokenTypeGet() not in [TOKEN.NOT, TOKEN.IS, TOKEN.THEN, TOKEN.COMMA, TOKEN.OPEN_PAREN, TOKEN.ELSE]:
                self.t_nOrdinal = Null(t_bBoundParameters, t_Token, self.t_nOrdinal, p_Parameters)
        return False

    def Resolve_comma(self, p_Parameters, t_Enum, t_Token, t_bBoundParameters):
        if (self.t_nRoundNested == 1) and (self.t_nRound == 1):
            self.t_nRound += 1
        return False

    # no idea what this does, I don't think it's used anywhere, but I kept it in just in case
    @classmethod
    def GetHexVal(cls, hex):
        """ generated source for method GetHexVal """
        val = int(hex)
        return val - (48 if val < 58 else (55 if val < 97 else 87))

    # not sure why this does what it does
    def Null(self, p_bBoundParameters, p_Token, p_nOrdinal, p_Parameters):
        p_nOrdinal += 1
        t_Parameter = intersystems_iris.dbapi._Parameter._Parameter(mode = ParameterMode.DEFAULT_PARAMETER, paramType = 'c')
        if p_bBoundParameters:
            p_Parameters._params_list.insert(p_nOrdinal - 1, t_Parameter)
        else:
            p_Parameters._params_list.append(t_Parameter)
        p_Token.Lexeme = "?"
        p_Token.TokenTypeSet(TOKEN.QUESTION_MARK)
        p_Token.m_replaced = True
        return p_nOrdinal

    # matches a named parameter in the SQL statement with a param in the list of parameters
    #  param - list of parameters (p_Parameters from Resolve())
    #  paramName - name of the parameter to be matched
    #  numParam - len(param)
    def matchUpParam(self, param, paramName, numParam):
        match = False
        if not self.hasNamedParameters or (paramName == None or paramName == "" or paramName[0] != '@'):
            return False
        for i in range(len(param._params_list)):
            if (param._params_list[i].name.upper() == paramName.upper()) or (("@" + param._params_list[i].name.upper()) == paramName.upper()):
                match = True
                if i != numParam:
                    cp = param._params_list[i]
                    cporig = cp
                    if not cporig.parsermatched:
                        del param._params_list[i:(i+1)]
                    else:
                        cp = cporig.Clone()
                        cp.name = cporig.name + str(numParam)
                        cp.mode = ParameterMode.UNKNOWN
                        if cporig.matchedParameterList == None:
                            cporig.matchedParameterList = []
                        cporig.matchedParamaterList.append(cp)
                    cp.parsermatched = True
                    param._params_list.insert(numParam, cp)
                else:
                    param._params_list[i].parsermatched = True
                break
        return match

    # I don't 100% follow this function, but I'm pretty sure it spends most of its time trying to isolate a return parameter, if any, then preparses as normal (?)
    def Call(self, pOut, p_Parameters):
        t_bRet = False
        pOut.p_eStmtType = StatementType.UPDATE
        pOut.sResult = ""
        for i in range(1):
            t_Enum = self.m_Tokens.GetEnumerator()
            t_Enum.MoveNext()
            t_str = t_Enum.Current().UpperLexeme
            while (TOKEN.UNKNOWN == t_Enum.Current().TokenTypeGet()) and t_str.startswith("/*"):
                t_Enum.MoveNext() # skip comments
                t_str = t_Enum.Current().UpperLexeme
            t_Token = t_Enum.Current()
            if t_Token.Lexeme[0] == '{':
                t_Enum.MoveNext()
                t_Token = t_Enum.Current()
            returnParam = None
            # expects either "? = ..." or one of "CALL", "EXEC", "EXECUTE"
            if t_Token.TokenTypeGet() == TOKEN.QUESTION_MARK:
                returnParam = intersystems_iris.dbapi._Parameter._Parameter("?", ParameterMode.RETURN_VALUE, '?')
                if not t_Enum.MoveNext() or t_Enum.Current().Lexeme[0] != '=':
                    break
                if not t_Enum.MoveNext():
                    break
            elif not (t_Enum.Current().UpperEquals("CALL") or t_Enum.Current().UpperEquals("EXEC") or t_Enum.Current().UpperEquals("EXECUTE")):
                return False

            # not really sure what to make of the next couple blocks of code
            # feels like they should maybe be in another elif block, not their own if block
            if t_Token.TokenTypeGet() == TOKEN.ATSIGN:
                self.hasNamedParameters = True
                returnParam = intersystems_iris.dbapi._Parameter._Parameter(t_Token.Lexeme, ParameterMode.RETURN_VALUE)
                if not t_Enum.MoveNext() or t_Enum.Current().Lexeme[0] != '=':
                    break
                if not t_Enum.MoveNext():
                    break
            if t_Enum.Current().UpperEquals("CALL") or t_Enum.Current().UpperEquals("EXEC") or t_Enum.Current().UpperEquals("EXECUTE"):
                if not t_Enum.MoveNext():
                    break
            else:
                if TOKEN.STRFUNCTION == t_Enum.Current().TokenTypeGet():
                    break

            pOut.sResult += t_Enum.Current().Lexeme
            t_Token = t_Enum.Current()
            if t_Token.UpperEquals("SELECT") or t_Token.UpperEquals("UPDATE") or t_Token.UpperEquals("INSERT"):
                pOut.sResult = ""
                break
            if not t_Enum.MoveNext():
                break
            t_Token = t_Enum.Current()
            if t_Token.UpperEquals("SELECT") or t_Token.UpperEquals("UPDATE") or t_Token.UpperEquals("INSERT"):
                pOut.sResult = ""
                break
            t_bQuitLoop = False
            while t_Token.Lexeme[0] == '.':
                pOut.sResult += '.'
                if not t_Enum.MoveNext():
                    t_bQuitLoop = True
                    break
                t_Token = t_Enum.Current()
                if t_Token.TokenTypeGet() == TOKEN.ID:
                    pOut.sResult += t_Token.Lexeme
                    if not t_Enum.MoveNext():
                        t_bQuitLoop = True
                        break
                    t_Token = t_Enum.Current()
            t_bBoundParameters = (len(p_Parameters._params_list) > 0)
            t_nOrdinal = 0
            if returnParam is not None:
                t_nOrdinal += 1
                if not t_bBoundParameters:
                    p_Parameters._params_list.insert(0, returnParam)
                else:
                    if not matchUpParam(p_Parameters, returnParam.GetName(), t_nOrdinal - 1):
                        if p_Parameters._params_list[0].mode != ParameterMode.RETURN_VALUE:
                            p_Parameters._params_list.insert(0, returnParam)
            if not t_bQuitLoop:
                t_eLastToken = TOKEN.UNKNOWN
                call_switcher = {
                    TOKEN.QUESTION_MARK: self.Call_question_mark,
                    TOKEN.ATSIGN: self.Call_atsign,
                    TOKEN.HEX: self.Call_hex,
                    TOKEN.CONSTANT: functools.partial(self.Call_constant_id, t_Enum = t_Enum),
                    TOKEN.ID: functools.partial(self.Call_constant_id, t_Enum = t_Enum),
                    TOKEN.NULL: self.Call_null,
                    TOKEN.COMMA: functools.partial(self.Call_comma_paren, t_eLastToken = t_eLastToken),
                    TOKEN.CLOSE_PAREN: functools.partial(self.Call_comma_paren, t_eLastToken = t_eLastToken)
                }
                while True:
                    t_Token = t_Enum.Current()
                    call_func = call_switcher.get(t_Token.TokenTypeGet(), self.Call_default)
                    (t_nOrdinal, t_eLastToken) = call_func(p_Parameters, t_Token, t_nOrdinal, t_bBoundParameters)
                    
                    if not t_Enum.MoveNext():
                        break
            pOut.p_eStmtType = StatementType.CALL if (returnParam == None) else StatementType.CALLWITHRESULT
            t_bRet = True
        return t_bRet

    def Call_default(self, p_Parameters, t_Token, t_nOrdinal, t_bBoundParameters):
        return (t_nOrdinal, t_Token.TokenTypeGet())

    def Call_question_mark(self, p_Parameters, t_Token, t_nOrdinal, t_bBoundParameters):
        if not t_bBoundParameters:
            p_Parameters._params_list.append(intersystems_iris.dbapi._Parameter._Parameter("?", ParameterMode.INPUT, '?'))
        return (t_nOrdinal + 1, TOKEN.QUESTION_MARK)

    def Call_atsign(self, p_Parameters, t_Token, t_nOrdinal, t_bBoundParameters):
        self.hasNamedParameters = True
        if (not t_bBoundParameters) or len(p_Parameters._params_list) == 0:
            p_Parameters._params_list.add(intersystems_iris.dbapi._Parameter._Parameter(t_Token.Lexeme, ParameterMode.UNKNOWN))
        else:
            if not matchUpParam(p_Parameters, t_Token.Lexeme, t_nOrdinal):
                p_Parameters._params_list.add(intersystems_iris.dbapi._Parameter._Parameter(t_Token.Lexeme, ParameterMode.UNKNOWN))
        return (t_nOrdinal + 1, TOKEN.ATSIGN)

    def Call_hex(self, p_Parameters, t_Token, t_nOrdinal, t_bBoundParameters):
        cp = intersystems_iris.dbapi._Parameter._Parameter(bytes.fromhex(t_Token.Lexeme[2:]), ParameterMode.REPLACED_LITERAL, '?')
        p_Parameters._params_list.append(cp)
        t_Token.TokenTypeSet(TOKEN.QUESTION_MARK)
        return (t_nOrdinal + 1, TOKEN.QUESTION_MARK)

    def Call_constant_id(self, p_Parameters, t_Token, t_nOrdinal, t_bBoundParameters, t_Enum):
        t_NewEnum = t_Enum.Clone()
        if t_NewEnum.MovePrevious():
            t_PreviousToken = t_NewEnum.Current()
            if t_PreviousToken.TokenTypeGet() == TOKEN.OP:
                t_Token.TokenTypeSet(TOKEN.QUESTION_MARK)
                return (self.DynamicVariable(t_bBoundParameters, intersystems_iris.dbapi.preparser._Token._Token(TOKEN.CONSTANT, t_PreviousToken.Lexeme, t_PreviousToken.UpperLexeme), t_nOrdinal, p_Parameters),
                        TOKEN.QUESTION_MARK)
        return (self.DynamicVariable(t_bBoundParameters, t_Token, t_nOrdinal, p_Parameters), TOKEN.QUESTION_MARK)

    def Call_null(self, p_Parameters, t_Token, t_nOrdinal, t_bBoundParameters):
        return (self.Null(t_bBoundParameters, t_Token, t_nOrdinal, p_Parameters), t_Token.TokenTypeGet())

    def Call_comma_paren(self, p_Parameters, t_Token, t_nOrdinal, t_bBoundParameters, t_eLastToken):
        if TOKEN.COMMA == t_eLastToken or TOKEN.OPEN_PAREN == t_eLastToken:
            t_Parameter = intersystems_iris.dbapi._Parameter._Parameter(mode = ParameterMode.DEFAULT_PARAMETER, paramType = 'd')
            t_nOrdinal += 1
            self.m_nUndefinedCount += 1
            if t_bBoundParameters:
                p_Parameters._params_list.insert(t_nOrdinal - 1, t_Parameter)
            else:
                p_Parameters._params_list.append(t_Parameter)
        return (t_nOrdinal, t_Token.TokenTypeGet())

    # No idea why this function does what it does
    def Exec(self, pOut, p_Parameters):
        t_bRet = False
        pOut.p_eStmtType = StatementType.UPDATE
        t_Enum = self.m_Tokens.GetEnumerator()
        for i in range(1):
            t_Enum.MoveNext()
            t_Token = t_Enum.Current()
            if not t_Token.UpperEquals("EXEC") and not t_Token.UpperEquals("EXECUTE"):
                break
            pOut.p_eStmtType = StatementType.CALL
            t_Enum.MoveNext()
            t_str = t_Enum.Current().UpperLexeme
            while (TOKEN.UNKNOWN == t_Enum.Current().TokenTypeGet()) and t_str.startswith("/*"):
                t_Enum.MoveNext() # skip comments
                t_str = t_Enum.Current().UpperLexeme
            t_Token = t_Enum.Current()
            if (t_Token.UpperEquals("SELECT")) or (t_Token.UpperEquals("UPDATE")) or (t_Token.UpperEquals("INSERT")):
                break
            t_bRet = True
            t_bHasReturnType = False
            if '@' == t_Token.Lexeme[0]:
                t_bHasReturnType = True
                if not t_Enum.MoveNext():
                    break
                if not t_Enum.MoveNext():
                    break
                t_Token = t_Enum.Current()
                if t_Token.Lexeme != "=":
                    break
                if not t_Enum.MoveNext():
                    break
                t_Token = t_Enum.Current()
            pOut.sResult += t_Token.Lexeme
            if not t_Enum.MoveNext():
                return True
            t_Token = t_Enum.Current()
            t_bQuitLoop = False
            while t_Token.Lexeme[0] == '.':
                pOut.sResult.append('.')
                if not t_Enum.MoveNext():
                    t_bQuitLoop = True
                    break
                t_Token = t_Enum.Current()
                if t_Token.TokenTypeGet() == TOKEN.ID:
                    pOut.sResult.append(t_Token.Lexeme)
                    if not t_Enum.MoveNext():
                        t_bQuitLoop = True
                        break
                    t_Token = t_Enum.Current()
            if t_bQuitLoop:
                break
            t_nOrdinal = 0
            while True:
                t_Token = t_Enum.Current()
                if TOKEN.COMMA == t_Token.TokenTypeGet():
                    if not t_Enum.MoveNext():
                        break
                    t_Token = t_Enum.Current()
                if t_Token.UpperEquals("WITH RECOMPILE"): # Shouldn't it be impossible for this to be a single token?
                    break
                t_strParameterName = ""
                if t_Token.Lexeme[0] == '@':
                    if t_Enum.MoveNext():
                        t_strParameterName = t_Enum.Current().Lexeme
                        bMoveNext = t_Enum.MoveNext()
                        if (not bMoveNext) or (t_Enum.Current().Lexeme != "="):
                            if not bMoveNext:
                                t_bQuitLoop = True
                            t_Param = intersystems_iris.dbapi._Parameter._Parameter(mode = ParameterMode.INPUT_OUTPUT, name = t_Token.Lexeme[1:], execParam = True)
                            self.m_ExecParamCount += 1
                            p_Parameters._params_list.append(t_Param)
                            continue 
                        t_Enum.MoveNext()
                    t_Token = t_Enum.Current()
                    t_Enum.MoveNext()
                if t_Token.TokenTypeGet() not in [TOKEN.OPEN_PAREN, TOKEN.CLOSE_PAREN, TOKEN.QUESTION_MARK, TOKEN.UNKNOWN]:
                    if t_Token is not None:
                        if t_Token.Lexeme[0] == '-':
                            if not t_Enum.MoveNext():
                                t_bQuitLoop = True
                            else:
                                t_Token = t_Enum.Current()
                                t_NewToken = intersystems_iris.dbapi.preparser._Token._Token(TOKEN.CONSTANT, "-" + t_Token.Lexem, "-" + t_Token.UpperLexeme)
                                t_nOrdinal = self.DynamicVariable(False, t_NewToken, t_nOrdinal, p_Parameters)
                        else:
                            t_nOrdinal = self.DynamicVariable(False, t_Token, t_nOrdinal, p_Parameters)
                    if not t_bQuitLoop:
                        t_Parameter = p_Parameters._params_list[-1]
                        t_Parameter.name = t_strParameterName
                        t_Parameter.execParam = True
                        self.m_ExecParamCount += 1
                if not t_Enum.MoveNext():
                    break
            if t_bQuitLoop:
                break
            if t_bHasReturnType:
                pOut.p_eStmtType = StatementType.CALLWITHRESULT
                t_ReturnParam = intersystems_iris.dbapi._Parameter._Parameter(mode = ParameterMode.UNKNOWN, execParam = True)
                self.m_ExecParamCount += 1
                p_Parameters._params_list.insert(0, t_ReturnParam)
            else:
                pOut.p_eStmtType = StatementType.CALL
            t_bRet = True
        if 0 == self.m_ExecParamCount:
            return False
        return t_bRet

    # creates Parameter object for replaced literals
    def DynamicVariable(self, p_bBoundParameters, p_Token, p_nOrdinal, p_Parameters):
        p_nOrdinal += 1
        t_str = p_Token.Lexeme
        t_c = t_str[0]
        if t_c in ["'", "\""]:
            # Remove leading and trailing quotes
            t_str = t_str[1:-1]
            # Condense doubled quotes to a single quote
            t_i = 0
            while t_i < len(t_str) - 1:
                if (t_str[t_i] == t_c) and (t_str[t_i + 1] == t_c):
                    t_str = t_str[:t_i] + t_str[(t_i + 1):]
                t_i += 1
        else:
            if 'e' in t_str or 'E' in t_str:
                # Normalize number
                try:
                    t_double = float(t_str)
                    t_str = str(t_double)
                except ValueError:
                    # wasn't able to parse, leave as is
                    pass
            else:
                p = 0
                if t_str[p] == '+':
                    t_str = t_str[1:]
                if t_str[p] == '-':
                    p += 1
                while (p < len(t_str)) and (t_str[p] == '0'):
                    t_str = t_str[:p] + t_str[(p + 1):]
                if '.' in t_str:
                    while t_str[-1] == '0':
                        t_str = t_str[:-1]
                    if t_str[-1] == '.':
                        t_str = t_str[:-1]
                if p >= len(t_str):
                    t_str = "0"
        if p_bBoundParameters:
            p_Parameters._params_list.insert(p_nOrdinal - 1, intersystems_iris.dbapi._Parameter._Parameter(t_str, ParameterMode.REPLACED_LITERAL))
        else:
            p_Parameters._params_list.append(intersystems_iris.dbapi._Parameter._Parameter(t_str, ParameterMode.REPLACED_LITERAL))
        p_Token.Lexeme = "?"
        p_Token.TokenTypeSet(TOKEN.QUESTION_MARK)
        p_Token.m_replaced = True
        return p_nOrdinal

    def appendRowId(self, sb):
        if self.m_addRowID != 0:
            return sb + "%ID ,"
        return sb

    def appendIdAdded(self, sb):
        if self.m_addRowID == 2:
            return sb + "%IDADDED "
        return sb