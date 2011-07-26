import sqlparse

class myqueryclauses:
    selectIdent = sqlparse.sql.IdentifierList()
    fromIdent = sqlparse.sql.IdentifierList()
    whereIdent = sqlparse.sql.IdentifierList()
    groupbyIdent = sqlparse.sql.IdentifierList()
    orderbyIdent = sqlparse.sql.IdentifierList()
    havingIdent = sqlparse.sql.IdentifierList()
    
    def __init__(self):
        selectIdent = sqlparse.sql.IdentifierList()
        fromIdent = sqlparse.sql.IdentifierList()
        whereIdent = sqlparse.sql.IdentifierList()
        groupbyIdent = sqlparse.sql.IdentifierList()
        orderbyIdent = sqlparse.sql.IdentifierList()
        havingIdent = sqlparse.sql.IdentifierList()
    
    def setSelectIdent (self,ident):
        self.selectIdent = ident
    def setFromIdent (self,ident):
        self.fromIdent = ident
    def setWhereIdent (self,ident):
        self.whereIdent = ident
    def setGroupbyIdent (self,ident):
        self.groupbyIdent = ident
    def setOrderbyIdent (self,ident):
        self.orderbyIdent = ident
    def setHavingIdent (self,ident):
        self.havingIdent = ident
    
    def getSelectIdent (self):
        print "SELECT IDENTIFIERS:^^^^^^%s^^^^^"%self.selectIdent
        return self.selectIdent
    def getFromIdent (self):
        print "FROM IDENTIFIERS:^^^^^^%s^^^^^"%self.fromIdent
        return self.fromIdent
    def getWhereIdent (self):
        print "WHERE IDENTIFIERS:^^^^^^%s^^^^^"%self.whereIdent
        return self.whereIdent
    def getGroupbyIdent (self):
        print "GROUP BY IDENTIFIERS:^^^^^^%s^^^^^"%self.groupbyIdent
        return self.groupbyIdent
    def getOrderbyIdent (self):
        print "ORDER BY IDENTIFIERS:^^^^^^%s^^^^^"%self.orderbyIdent
        return self.orderbyIdent
    def getHavingIdent (self):
        print "HAVING IDENTIFIERS:^^^^^^%s^^^^^"%self.havingIdent
        return self.havingIdent
