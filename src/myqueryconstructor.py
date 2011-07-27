from sqlparse import sql
import db_connection
import myqueryclauses
import  myhelper
import myparser

db = db_connection.Db_connection()

def getResultSetAsList(query):
    if (query is not None):
        #results = ['COSI','MATH','HIST']
        resultList = []
        results = db.allrows(query)
        for r in results:
            resultList.append(r[0])
        return resultList
    return None

def checkIfList(ident):
    if (isinstance(ident, sql.Identifier)):#d.name
            return False
    return True   

def findDistinctGroupbyValues(groupbyIdent,fromIdent):
    if ((groupbyIdent and fromIdent) is not None):        
        if (checkIfList(groupbyIdent)):
            query = " SELECT DISTINCT " 
            for gid in groupbyIdent:
                gAlias = gid.get_parent_name()
                query += str(gid) + ", "
            query = query.strip(", ")
            query+= " FROM "
            if (checkIfList(fromIdent)): # have a from list and group-by list
                for fid in fromIdent:
                    for gid in groupbyIdent:
                        gAlias = gid.get_parent_name()
                        if (fid.get_alias() is gAlias):
                            query += str(fid) + ", "
                query = query.strip(", ")
            else:
                query += str(fromIdent)
            
        else: #dont have a group-by list"
            gAlias = groupbyIdent.get_parent_name()
            if (checkIfList(fromIdent)):
                for fid in fromIdent:
                    if (fid.get_alias() is gAlias):
                        query = " SELECT DISTINCT " + str(groupbyIdent) + " FROM " + str(fid)
            else:
                query = " SELECT DISTINCT " + str(groupbyIdent) + " FROM " + str(fromIdent)
        
        result = getResultSetAsList(query)
#        print query
#        print result
        return result
    

def findTablenameFromAlias(fromIdent,alias):    
    if (isinstance(fromIdent, sql.Identifier)):#d.name        
        print "I have a from identifier"
        name = fromIdent.get_name()
        alias = fromIdent.get_parent_name()
        print alias
        print name
    else: #d.name, e.id 
        print "I have a from by list"
        for fid in fromIdent:
            fidAlias = fid.get_alias()
            if (alias == fidAlias):
                print fid.get_real_name()
                return fid.get_real_name()
    return False

def constructSubSelects (selectIdent, distinctGroupbyValues, fromIdent, whereIdent):
    if (selectIdent, distinctGroupbyValues, fromIdent, whereIdent):
        print " Constructing sub selects"
        print selectIdent
        print checkIfList(selectIdent)
        
        for sid in selectIdent:
            if(myhelper.isAggregate(sid)):
                print "I have an aggregate"
                print sid
        
        
#        for dgbv in distinctGroupbyValues:
#            select = " SELECT "
#            query = " SELECT '" + dgbv + "'::Text" 
#            print query



if __name__ == "__main__":
    userInput = ("SELECT d.name, AVG (e.salary) , MAX(d.name)"
              " FROM employee e, department d"
              " WHERE e.dept_id = d.id"
              " GROUP BY d.name")
    
    (mytok, mytoklen) = myparser.tokenizeUserInput (userInput)
#    displayTokens(mytok,mytoklen)
    queryclauses = myparser.myParser(mytok, mytoklen)
#    queryclauses.dispay()
    groupbyIdent = queryclauses.getGroupbyIdent()    
    fromIdent = queryclauses.getFromIdent()
    selectIdent = queryclauses.getSelectIdent()
    whereIdent = queryclauses.getWhereIdent()
    
    distinctGroupbyValues = findDistinctGroupbyValues(groupbyIdent, fromIdent)
    
    # dictionary having the temp table as key and the query for that table as value 
    subSelects = constructSubSelects (selectIdent, distinctGroupbyValues, fromIdent, whereIdent)