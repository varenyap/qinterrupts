import os.path

basepath = os.path.dirname(__file__)

#Path to SQL script
#init_script_path = os.path.abspath(os.path.join(basepath, "..", "..", "qinterrupts/src/script1.sql"))

#init_script_path = os.path.abspath(os.path.join(basepath, "..", "..", "qinterrupts/src/script2.sql"))

#init_script_path = os.path.abspath(os.path.join(basepath, "..", "..", "qinterrupts/src/script3.sql"))

#init_script_path = os.path.abspath(os.path.join(basepath, "..", "..", "qinterrupts/src/script4.sql"))

#init_script_path = os.path.abspath(os.path.join(basepath, "..", "..", "qinterrupts/src/allscripts.sql"))

init_script_path = os.path.abspath(os.path.join(basepath, "..", "..", "qinterrupts/src/quotesScript.sql"))

#db_name = "tester"
#db_user = "varenyaprasad"
#db_password = "postgres"


#Authorization to connect to the local TPC-H database
init_script_path = ""
db_name = "tpcd"
db_user = "varenyaprasad"
db_password = "postgres"