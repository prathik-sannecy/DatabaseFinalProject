
def parseNBAJsonFile(inputFileName, outputFileName):
    with open(inputFileName) as jsonFile, open(outputFileName, "w") as out:
        text = jsonFile.read()
        splitText = text.split("{")
        parameters = splitText[2]
        table = splitText[3]
        splitTable = table.split("[")
        tableHeader = splitTable[1].replace('],"rowSet":', "")
        out.write(tableHeader + "\n")

        for row in range(5, len(splitTable)):
            if row != "":
                out.write(splitTable[row].replace("]", "").strip(",").strip("}") + "\n")


parseNBAJsonFile("DataSets/leaguedashteamstats_scoring.json", "Outputs/outteam_scoring.csv")
