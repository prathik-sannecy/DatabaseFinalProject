import psycopg2 as p

userName = "f19wdb14"
password = "v@Nnd9ae5d"
dbName = "f19wdb14"
schemaName = "FinalProject"
hostName = "dbclass.cs.pdx.edu"

queries = []
queries.append(("Display Teams", "SELECT t_name as teams from FinalProject.team"))
queries.append(("Team with highest Attendence", "SELECT t_name as team, attendence_total from FinalProject.teamAttendence where attendence_total >=(select max(attendence_total) from FinalProject.teamAttendence)"))
queries.append(("Coach for team X", "SELECT C.c_name FROM FinalProject.coach C, FinalProject.team T, FinalProject.coachteamrel CT WHERE CT.c_name = C.c_name and CT.t_name = T.t_name and T.t_name = %s", "Enter team name: "))
queries.append(("Games on date", "SELECT home_team, away_team FROM FinalProject.game WHERE CAST(date AS varchar) LIKE %s", "Enter date (YYYY-MM-DD): "))
queries.append(("Player with highest steals per game", "SELECT p_name, statValue FROM FinalProject.playerstatrel WHERE player_stat_type = 'STL' and statValue =(SELECT MAX(statValue) from FinalProject.playerstatrel WHERE player_stat_type = 'STL')"))
queries.append(("Best Offenses", "SELECT * FROM FinalProject.TEAMStatRanker('OFF_RATING')"))
queries.append(("Best 3 point shooter", "SELECT * FROM FinalProject.PlayerStatRanker('FG3M')"))
numQueries = len(queries)
con = p.connect(host=hostName, database=dbName, user=userName, password=password)
db = con.cursor()

while True:
    inpt = input("Please press: o (options) or e (exit) or 1-{}:".format(numQueries))
    if inpt == "o":
        for query_num, query in enumerate(queries):
            count = query_num
            desc = query[0]
            print(str(count + 1) +" : " + desc)
        continue
    elif inpt == "e":
        break
    try:
        val = int(inpt) - 1
        if len(queries[val]) == 3:
            user_input = input(queries[val][2])
            if 'like' in queries[val][1].lower():
                db.execute(queries[val][1], ["%%" + user_input + "%%"])
            else:
                db.execute(queries[val][1], [user_input])

        else:
            db.execute(queries[val][1])
        for row in db.fetchall():
            print("\t" + str(row))
    except ValueError:
        print("Invalid input")

db.close()
con.close()
