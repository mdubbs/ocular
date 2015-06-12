import pyodbc, yaml
import sys, time

start = time.clock()

# open the db YAML and parse or exit
try:
  stream = open("config/db.yaml", 'r')
  conf = yaml.load(stream)
except:
  print("Failed to open the database config file. Exiting..")
  sys.exit(2)

# open the settings YAML and parse or exit
try:
  stream = open("config/settings.yaml", 'r')
  settings = yaml.load(stream)
except:
  print("Failed to open the settings file. Exiting...")
  sys.exit(2)

# attempt connection to MSSQL Server
try:
  cnxn = pyodbc.connect(conf["db_conn"])
except pyodbc.ProgrammingError:
  print("There was an error connecting to the database. Exiting..")
  sys.exit(2)

# set cursor and execute query on application log
cursor = cnxn.cursor()
cursor.execute("SELECT * FROM ApplicationLog WHERE TimeStamp >= DATEADD(day, -"+settings["days"]+", GETDATE())")
rows = cursor.fetchall()

# set data stores
level_info = {"Information": 0, "Error": 0, "Warning": 0}
error_messages = []

print()
print("------------------------------------------------------------------------------------")
print("Analyzing One Call V2 application logs... ")
print("------------------------------------------------------------------------------------")

# analyze data from db
for row in rows:
    level_info[row.Level] += 1;
    if row.Level == "Error":
      error_messages.append([row.Exception, row.TimeStamp])

end = time.clock()

print("Analysis finished in: "+str(end-start)+"s")

# print out the results
for k in level_info:
  print(k, "-", level_info[k])

print("------------------------------------------------------------------------------------")
print("Errors are listed below")
print("------------------------------------------------------------------------------------")

if level_info["Error"] == 0:
  print("No errors recorded")
else:
  for idx, row in enumerate(error_messages):
    print(idx+1, "\t", row[1], "\t", row[0].split(":")[0])

print("------------------------------------------------------------------------------------")
print()