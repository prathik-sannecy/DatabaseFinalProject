import psycopg2 as p

userName = "f19wdb14"
password = "v@Nnd9ae5d"
dbName = "f19wdb14"
schemaName = "FinalProject"
hostName = "dbclass.cs.pdx.edu"

queries = []
queries.append(("Display Teams", "SELECT t_name as teams from FinalProject.team"))
queries.append(("Team with highest Attendence", "SELECT t_name as team, attendence_total from FinalProject.teamAttendence where attendence_total >=(select max(attendence_total) from FinalProject.teamAttendence)"))
numQueries = len(queries)
con = p.connect(host=hostName, database=dbName, user=userName, password=password)
db = con.cursor()

while True:
    inpt = input("Please press: o (options) or e (exit) or 1-{}:".format(numQueries))
    if inpt == "o":
        for count, (desc, query) in zip(range(0, numQueries), queries):
            print(str(count + 1) +" : " + desc)
        continue
    elif inpt == "e":
        break
    try:
        val = int(inpt) - 1
        db.execute(queries[val][1])
        for row in db.fetchall():
            print("\t" + str(row))
    except ValueError:
        print("Invalid input")

db.close()
con.close()
