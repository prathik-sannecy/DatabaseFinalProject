import psycopg2 as p
import csv
import re
from TeamNames import abbribs 

userName = "f19wdb14"
password = "v@Nnd9ae5d"
dbName = "f19wdb14.FinalProject"
dbName = "f19wdb14"
hostName = "dbclass.cs.pdx.edu"

playerStats = "FGM   FGA FG_PCT  FG3M    FG3A    FG3_PCT FTM FTA FT_PCT  OREB    DREB    REB AST TOV STL BLK BLKA    PF  PFD PTS".split()

teamStats = "E_OFF_RATING    OFF_RATING  E_DEF_RATING    DEF_RATING  E_NET_RATING    NET_RATING  AST_PCT AST_TO  AST_RATIO   OREB_PCT    DREB_PCT    REB_PCT TM_TOV_PCT  EFG_PCT TS_PCT  E_PACE  PACE".split()
# Holds information for a column in a Table
class Attribute:
    def __init__(self, name, type, isNullable=True, isUnique = False,):
        self.name = name
        self.isNullable = isNullable
        self.isUnique = isUnique
        self.type = type

    def __str__(self):
        ret = "{} {}".format(self.name, self.type)
        if not self.isNullable:
            ret += " NOT NULL"
        if self.isUnique:
            ret += " UNIQUE"
        return ret

# Holds schema info and performs creations/insertions
class Table:
    def __init__(self, db, con):
        self.name = ""
        self.params = []
        self.primaryKey = ""
        self.constraints = []
        self.schemaName = "FinalProject"
        self.db = db
        self.con = con
        self.csvNames = []

    # Some Names have a ' in them which is slightly problematic
    def safeQuery(self, query):
        q = re.sub("(\w)\'(\s?\w)", "\1\2", query)
        return self.db.execute(q)

    def create(self):
        self.safeQuery("DROP TABLE IF EXISTS {}.{} CASCADE;".format(self.schemaName, self.name))
        self.con.commit()
        ret = self.safeQuery(self.createQuery())
        self.con.commit()
        return ret

    # SQL statement to create a new table
    def createQuery(self):
        ret = "CREATE TABLE {}.{}(".format(self.schemaName, self.name)
        ret += ",".join(map(lambda x : str(x),self.params)) + ","
        if len(self.constraints) > 0:
            ret += ",".join(self.constraints) + ","
        ret += "PRIMARY KEY ({})".format(self.primaryKey)
        ret += ");"
        return ret

    # Inserts a new record.  Assumes that all values are provided
    def insert(self, values, doCommit=True):
        # handle wrapping string with " and numbers to strings
        formatValues = []
        for param, val in zip(self.params, values):
            pVal = str(val)
            #if param.type != "int" and param.type != "timestamp":
            if param.type != "int":
                pVal = "'{}'".format(pVal)
            if param.type == "timestamp":
                pVal = "timestamp " + re.sub("\/", "-", pVal)
            formatValues.append(pVal)

        sql = ",".join(formatValues)
        sql = "INSERT INTO {}.{} VALUES ({});".format(self.schemaName, self.name, sql)
        ret = self.safeQuery(sql)
        if doCommit:
            self.con.commit()
        return ret

    # Parses a CSV file and assumes that each row is a record
    def insertFromFile(self, filePath):
        inputFile = csv.DictReader(open(filePath), delimiter=',')
        for row in inputFile:
            values = []
            for k in self.csvNames:
                values.append(row[k])
            self.insert(values, False)
        self.con.commit()

# Hard coded table for debugging
class TestTable (Table):
    def __init__(self, db, con):
        Table.__init__(self, db, con)
        self.name = "test"
        self.params = [Attribute("t4", "varchar(255)")]
        self.params.append(Attribute("t2", "int"))
        self.primaryKey = "t4"
        self.constraints = []

# Coach table
class CoachTable (Table):
    def __init__(self, db, con):
        Table.__init__(self, db, con)
        self.name = "coach"
        self.params = [Attribute("c_name", "varchar(255)")]
        self.params.append(Attribute("age", "int"))
        self.params.append(Attribute("years_coached", "int"))
        self.primaryKey = "c_name"
        self.constraints = []
        self.csvNames = ["NAME", "EXP", "EXP"]

# Player table
class PlayerTable (Table):
    def __init__(self, db, con):
        Table.__init__(self, db, con)
        self.name = "player"
        self.params = [Attribute("p_name", "varchar(255)")]
        #self.params.append(Attribute("position", "varchar(10)"))
        self.params.append(Attribute("age", "int"))
        self.primaryKey = "p_name"
        self.constraints = []
        #self.csvNames = ["PLAYER_NAME", "POS", "AGE"]
        self.csvNames = ["PLAYER_NAME", "AGE"]

# Player table
class TeamTable (Table):
    def __init__(self, db, con):
        Table.__init__(self, db, con)
        self.name = "team"
        self.params = [Attribute("t_name", "varchar(255)")]
        self.primaryKey = "t_name"
        self.constraints = []
        self.csvNames = ["TEAM_NAME"]

# Player table
class TeamRecordTable (Table):
    def __init__(self, db, con):
        Table.__init__(self, db, con)
        self.name = "teamRecord"
        self.params = [Attribute("t_name", "varchar(255)")]
        self.params.append(Attribute("gp", "int"))
        self.params.append(Attribute("wins", "int"))
        self.params.append(Attribute("losses", "int"))
        self.params.append(Attribute("year", "int"))
        self.primaryKey = "t_name, year"
        self.constraints = ["FOREIGN KEY (t_name) REFERENCES {}.team(t_name)".format(self.schemaName)]
        self.csvNames = ["TEAM_NAME", "GP", "W", "L"]

    def insertFromFile(self, filePath):
        inputFile = csv.DictReader(open(filePath), delimiter=',')
        for row in inputFile:
            values = []
            for k in self.csvNames:
                values.append(row[k])
            values += ["2019"]
            self.insert(values, False)
        self.con.commit()

# Player table
class TeamAttTable (TeamRecordTable):
    def __init__(self, db, con):
        Table.__init__(self, db, con)
        self.name = "teamAttendence"
        self.params = [Attribute("t_name", "varchar(255)")]
        self.params.append(Attribute("attendence_total", "int"))
        self.params.append(Attribute("year", "int"))
        self.primaryKey = "t_name, year"
        self.constraints = ["FOREIGN KEY (t_name) REFERENCES {}.team(t_name)".format(self.schemaName)]
        self.csvNames = ["TEAM", "TOTAL"]


# Game table
class GameTable (Table):
    def __init__(self, db, con):
        Table.__init__(self, db, con)
        self.name = "game"
        self.params = [Attribute("home_team", "varchar(255)")]
        self.params.append(Attribute("away_team", "varchar(255)"))
        self.params.append(Attribute("date", "timestamp"))
        self.params.append(Attribute("location", "varchar(255)"))
        self.params.append(Attribute("result", "varchar(255)"))
        self.primaryKey = "home_team,away_team,date"
        self.constraints = ["FOREIGN KEY (home_team) REFERENCES {}.team(t_name)".format(self.schemaName)]
        self.constraints.append("FOREIGN KEY (away_team) REFERENCES {}.team(t_name)".format(self.schemaName))
        self.csvNames = ["Home Team", "Away Team", "Date", "Result"]

# Game table
class CoachTeamRelTable (Table):
    def __init__(self, db, con):
        Table.__init__(self, db, con)
        self.name = "coachTeamRel"
        self.params = [Attribute("c_name", "varchar(255)")]
        self.params.append(Attribute("t_name", "varchar(255)"))
        self.primaryKey = "t_name,c_name"
        self.constraints = ["FOREIGN KEY (t_name) REFERENCES {}.team(t_name)".format(self.schemaName)]
        self.constraints.append("FOREIGN KEY (c_name) REFERENCES {}.coach(c_name)".format(self.schemaName))
        self.csvNames = ["NAME", "TEAM"]

# Player Team table
class PlayerTeamRelTable (Table):
    def __init__(self, db, con):
        Table.__init__(self, db, con)
        self.name = "playerTeamRel"
        self.params = [Attribute("t_name", "varchar(255)")]
        self.params.append(Attribute("p_name", "varchar(255)"))
        self.params.append(Attribute("player_salary", "int"))
        self.primaryKey = "t_name,p_name"
        self.constraints = ["FOREIGN KEY (t_name) REFERENCES {}.team(t_name)".format(self.schemaName)]
        self.constraints.append("FOREIGN KEY (p_name) REFERENCES {}.player(p_name)".format(self.schemaName))
        self.csvNames = ["Player", "Tm", "season17_18"]
    # Parses a CSV file and assumes that each row is a record
    def insertFromFile(self, filePath):
        inputFile = csv.DictReader(open(filePath), delimiter=',')
        for row in inputFile:
            values = []
            for k in self.csvNames:
                if k == "Tm":
                    k = abbribs [k]
                values.append(row[k])
            self.insert(values, False)

# Player Team table
class PlayerStatRelTable (Table):
    def __init__(self, db, con):
        Table.__init__(self, db, con)
        self.name = "playerStatRel"
        self.params = [Attribute("p_name", "varchar(255)")]
        self.params.append(Attribute("player_stat_type", "varchar(255)"))
        self.params.append(Attribute("statValue", "float(24)"))
        self.primaryKey = "p_name,player_stat_type"
        self.constraints = ["FOREIGN KEY (p_name) REFERENCES {}.player(p_name)".format(self.schemaName)]
        self.constraints.append("FOREIGN KEY (player_stat_type) REFERENCES {}.playerstat(player_stat_type)".format(self.schemaName))
        self.csvNames = ["PLAYER_NAME"]
    
    # Parses a CSV file and assumes that each row is a record
    def insertFromFile(self, filePath):
        inputFile = csv.DictReader(open(filePath), delimiter=',')
        for row in inputFile:
            for stat in playerStats:
                values = []
                for k in self.csvNames:
                    values.append(row[k])
                values.append(stat)
                values.append(row[stat])
                self.insert(values, False)

# Player Team table
class TeamStatRelTable (Table):
    def __init__(self, db, con):
        Table.__init__(self, db, con)
        self.name = "teamStatRel"
        self.params = [Attribute("t_name", "varchar(255)")]
        self.params.append(Attribute("team_stat_type", "varchar(255)"))
        self.params.append(Attribute("statValue", "float(24)"))
        self.primaryKey = "t_name,team_stat_type"
        self.constraints = ["FOREIGN KEY (t_name) REFERENCES {}.team(t_name)".format(self.schemaName)]
        self.constraints.append("FOREIGN KEY (team_stat_type) REFERENCES {}.teamstat(team_stat_type)".format(self.schemaName))
        self.csvNames = ["TEAM_NAME"]
    
    # Parses a CSV file and assumes that each row is a record
    def insertFromFile(self, filePath):
        inputFile = csv.DictReader(open(filePath), delimiter=',')
        for row in inputFile:
            for stat in teamStats:
                values = []
                for k in self.csvNames:
                    values.append(row[k])
                values.append(stat)
                values.append(row[stat])
                self.insert(values, False)

# Player Team table
class PlayerStatTable (Table):
    def __init__(self, db, con):
        Table.__init__(self, db, con)
        self.name = "playerstat"
        self.params = [Attribute("player_stat_type", "varchar(255)")]
        self.primaryKey = "player_stat_type"
        self.constraints = []
    # Parses a CSV file and assumes that each row is a record
    def insertFromFile(self, filePath):
        for stat in playerStats:
            self.insert([stat], False)

# Player Team table
class TeamStatTable (Table):
    def __init__(self, db, con):
        Table.__init__(self, db, con)
        self.name = "teamStat"
        self.params = [Attribute("team_stat_type", "varchar(255)")]
        self.primaryKey = "team_stat_type"
        self.constraints = []
    # Parses a CSV file and assumes that each row is a record
    def insertFromFile(self, filePath):
        for stat in teamStats:
            self.insert([stat], False)

con = p.connect(host=hostName, database=dbName, user=userName, password=password)
db = con.cursor()

con = p.connect(host=hostName, database=dbName, user=userName, password=password)
db = con.cursor()
db.execute("SET datestyle = dmy")
con.commit()
data = {}
data[CoachTable] = "outcoaches.csv"
data[PlayerTable] = "outplayer.csv"
data[TeamTable] = "outteam.csv"
data[PlayerStatTable] = ""
data[TeamStatTable] = ""

for tableType, fileName in data.items():
    table = tableType(db, con)
    ret = table.create()
    table.insertFromFile("./Outputs/" + fileName)

data = {}
data[TeamRecordTable] = "outteam.csv"
data[TeamAttTable] = "outnba_team_annual_attendance2018.csv"
data[GameTable] = "outnbaschedule.csv"
data[CoachTeamRelTable] = "outcoaches.csv"
data[PlayerStatRelTable] = "outplayer.csv"
data[TeamStatRelTable] = "outteam.csv"

for tableType, fileName in data.items():
    print(fileName)
    table = tableType(db, con)
    ret = table.create()
    table.insertFromFile("./Outputs/" + fileName)


pProcedure = """
CREATE OR REPLACE FUNCTION FinalProject.PlayerStatRanker(STAT VARCHAR)
RETURNS TABLE(
    player VARCHAR,
    statistic VARCHAR,
    rank BIGINT
)
AS $$
BEGIN RETURN QUERY
    SELECT P_NAME, player_stat_type, ROW_NUMBER() OVER(ORDER BY statvalue DESC) AS Rank from FinalProject.playerstatrel where player_stat_type = STAT and statvalue > 0;
END;
$$ LANGUAGE 'plpgsql'"""

tProcedure = """
CREATE OR REPLACE FUNCTION FinalProject.TEAMStatRanker(STAT VARCHAR)
RETURNS TABLE(
    player VARCHAR,
    statistic VARCHAR,
    rank BIGINT
)
AS $$
BEGIN RETURN QUERY
    SELECT t_NAME, team_stat_type, ROW_NUMBER() OVER(ORDER BY statvalue DESC) AS Rank from FinalProject.teamStatRel where team_stat_type = STAT and statvalue > 0;
END;
$$ LANGUAGE 'plpgsql'"""
storeProcedures = [pProcedure, tProcedure]
for proc in storeProcedures:
    db.execute(proc)
    con.commit();

db.close()
con.close()
    
