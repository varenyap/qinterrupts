import sqlparse
#sql = 'select * from foo; select * from bar;'
#split_thing = sqlparse.split(sql)
#print split_thing[0]
#print split_thing[0][3]


#sql = 'select * from foo, bar where id in (select id from bar); select * from foo;'
#print sqlparse.format(sql, reindent=True, keyword_case='upper')

sql = 'select * from someschema.mytable where id = 1'
parsed = sqlparse.parse(sql)
print parsed


stmt = parsed[0]
print stmt.tokens

print stmt.tokens[-1].to_unicode()

