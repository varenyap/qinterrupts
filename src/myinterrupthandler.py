# Step 3: The sub queries generated are sent to the Interrupt Handler one by one.
#         The interrupt handler keeps track of which query is being executed at all times,
#         and  any temporary tables that are created.
#
# Step 3a: The user decides to interrupt while a sub query is begin executed.
#          This is the point where it is important for the system to keep track of which sub query
#          is being executed. So when the user decides to interrupt, then the system is able to perform
#          any clean up operations that are required. For instance, on an interrupt, the sub query could
#          finish execution or be canceled midway.

# Step 3b: If the user doesnt interrupt, then execution continues as normal. All the sub queries are
#          executed against the database sequentially. Results are returned to the user in the order
#          they are computed.


import db_connection
import myqueryclauses
import  myhelper
import myparser
import myqueryconstructor

db = db_connection.Db_connection()

def interruptHandler(eh):
    print "Im here interrup handling yo"