import os.path

basepath = os.path.dirname(__file__)

#Path to SQL script
#init_script_path = os.path.abspath(os.path.join(basepath, "..", "..", "qinterrupts/src/script1.sql"))

init_script_path = os.path.abspath(os.path.join(basepath, "..", "..", "qinterrupts/src/script2.sql"))

db_name = "tester"
db_user = "varenyaprasad"
db_password = "postgres"

#db_name = "godzilla"
#db_user = "postgres"
#db_password = "amaranthus"