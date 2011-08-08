import sqlparse

class myqueryclauses:
    selectIdent = None
    fromIdent = None
    whereIdent = None
    groupbyIdent = None
    orderbyIdent = None
    havingIdent = None
    selectGroupbyIdent = None
    selectContainsAggregate = False
    
    def __init__(self):
        selectIdent = None
        fromIdent = None
        whereIdent = None
        groupbyIdent = None
        orderbyIdent = None
        havingIdent = None
        selectGroupbyIdent = None
        newSelectIdent = None
        selectContainsAggregate = False
        
#        selectIdent = sqlparse.sql.IdentifierList()
#        fromIdent = sqlparse.sql.IdentifierList()
#        whereIdent = sqlparse.sql.IdentifierList()
#        groupbyIdent = sqlparse.sql.IdentifierList()
#        orderbyIdent = sqlparse.sql.IdentifierList()
#        havingIdent = sqlparse.sql.IdentifierList()
    
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
    def setSelectContainsAggregate(self,val):
        self.selectContainsAggregate = val
    def setSelectGroupbyIdent (self,ident):
        self.selectGroupbyIdent = ident
    def setNewSelectIdent (self,ident):
        self.newSelectIdent = ident
    
    def getSelectIdent (self):
#        print "SELECT IDENTIFIERS:^^^^^^%s^^^^^"%self.selectIdent
        return self.selectIdent
    def getFromIdent (self):
#        print "FROM IDENTIFIERS:^^^^^^%s^^^^^"%self.fromIdent
        return self.fromIdent
    def getWhereIdent (self):
#        print "WHERE IDENTIFIERS:^^^^^^%s^^^^^"%self.whereIdent
        return self.whereIdent
    def getGroupbyIdent (self):
#        print "GROUP BY IDENTIFIERS:^^^^^^%s^^^^^"%self.groupbyIdent
        return self.groupbyIdent
    def getOrderbyIdent (self):
#        print "ORDER BY IDENTIFIERS:^^^^^^%s^^^^^"%self.orderbyIdent
        return self.orderbyIdent
    def getHavingIdent (self):
#        print "HAVING IDENTIFIERS:^^^^^^%s^^^^^"%self.havingIdent
        return self.havingIdent
    def getSelectContainsAggregate(self):
        return self.selectContainsAggregate
    def getSelectGroupbyIdent (self):
        return  self.selectGroupbyIdent
    
    def getNewSelectIdent (self):
        return self.newSelectIdent
   
   
    def getAll(self):
        retVal = []
        
        if (self.selectIdent is not None):
            retVal.append(self.selectIdent)
        if (self.fromIdent is not None):
            retVal.append(self.fromIdent)
        if (self.whereIdent is not None):
            retVal.append(self.whereIdent)
        if (self.groupbyIdent is not None):
            retVal.append(self.groupbyIdent)
        if (self.orderbyIdent is not None):
            retVal.append(self.orderbyIdent)
        if (self.havingIdent is not None):
            retVal.append(self.havingIdent)
        
        return retVal
    
    def dispay(self):
        if (self.selectIdent is not None):
            print "SELECT IDENTIFIERS:^^^^^^%s^^^^^"%self.selectIdent
        if (self.fromIdent is not None):
            print "FROM IDENTIFIERS:^^^^^^%s^^^^^"%self.fromIdent
        if (self.whereIdent is not None):
            print "WHERE IDENTIFIERS:^^^^^^%s^^^^^"%self.whereIdent
        if (self.groupbyIdent is not None):
            print "GROUP BY IDENTIFIERS:^^^^^^%s^^^^^"%self.groupbyIdent
        if (self.orderbyIdent is not None):
            print "ORDER BY IDENTIFIERS:^^^^^^%s^^^^^"%self.orderbyIdent
        if (self.havingIdent is not None):
            print "HAVING IDENTIFIERS:^^^^^^%s^^^^^"%self.havingIdent
        print " -----------------------Finished printing the parsed query clauses-----------------------"
