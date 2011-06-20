import sqlparse
from sqlparse import tokens

queries = '''
CREATE FUNCTION func1(a integer) RETURNS void
    LANGUAGE plpgsql
        AS $$
        BEGIN
                -- comment
       END;
       $$;
SELECT -- comment
* FROM -- comment
TABLE foo;
-- comment
INSERT INTO foo VALUES ('a -- foo bar');
INSERT INTO foo
VALUES ('
a 
-- foo bar'
);

'''

IGNORE = set(['CREATE FUNCTION',])  # extend this

def _filter(stmt, allow=0):
    ddl = [t for t in stmt.tokens if t.ttype in (tokens.DDL, tokens.Keyword)]
    start = ' '.join(d.value for d in ddl[:2])
    if ddl and start in IGNORE:
        allow = 1
    for tok in stmt.tokens:
        if allow or not isinstance(tok, sqlparse.sql.Comment):
            yield tok

for stmt in sqlparse.split(queries):
    sql = sqlparse.parse(stmt)[0]
    print sqlparse.sql.TokenList([t for t in _filter(sql)])


#sql = 'select * from foo, bar where id in (select id from bar); select * from foo;'
#print sqlparse.format(sql, reindent=True, keyword_case='upper')

#sql = 'select * from someschema.mytable where id = 1'
#sql = ("select d.name, avg(e.salary)"
#             "from employee e, department d"
#             " where e.dept_id = d.id"
#             " group by d.name")
#parsed = sqlparse.parse(sql)
#print "Parsed: %s"%parsed
#
#stmt = parsed[0] # Creates a statement class.
#stokens = stmt.tokens # creates token class
#print "IdentifierS?"
#sident = sqlparse.sql.Identifier
#print sident



#print "stmt.get_type(): %s" %stmt.get_type() #Statement functions
#print stokens[3]
#print (stokens[3]).is_whitespace() #Token functions
#
#print "IdentifierS?"
#print stmt.value
#
#
##print stokens[-1].to_unicode()
