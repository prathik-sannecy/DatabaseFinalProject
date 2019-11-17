import psycopg2 as p
import csv
import re

userName = "f19wdb14"
password = "v@Nnd9ae5d"
dbName = "f19wdb14.FinalProject"
dbName = "f19wdb14"
hostName = "dbclass.cs.pdx.edu"

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
        q = re.sub("(\w)\'(\w)", "\1\2", query)
        return self.db.execute(q)

    def create(self):
        self.safeQuery("DROP TABLE IF EXISTS {}.{};".format(self.schemaName, self.name))
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
            if param.type != "int":
                pVal = "'{}'".format(pVal)
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


con = p.connect(host=hostName, database=dbName, user=userName, password=password)
db = con.cursor()

test = TestTable(db, con)
ret = test.create()
test.insert(["ABC", 2])

coach = CoachTable(db, con)
ret = coach.create()
coach.insertFromFile("./Outputs/outcoaches")

db.close()
con.close()
