import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

def init_db():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    # Drop tables
    statement = '''
        DROP TABLE IF EXISTS 'Bars';
    '''
    cur.execute(statement)
    statement = '''
        DROP TABLE IF EXISTS 'Countries';
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'Bars' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Company' TEXT NOT NULL,
            'SpecificBeanBarName' TEXT NOT NULL,
            'REF' Text NOT NULL,
            'ReviewDate' TEXT NOT NULL,
            'CocoaPercent' REAL NOT NULL,
            'CompanyLocationId' INTEGER NOT NULL,
            'Rating' Real NOT NULL,
            'BeanType' TEXT,
            'BroadBeanOriginId' INTEGER,
            FOREIGN KEY(CompanyLocationId) REFERENCES Countries(Id),
            FOREIGN KEY(BroadBeanOriginId) REFERENCES Countries(Id)
        );
    '''
    cur.execute(statement)
    statement = '''
        CREATE TABLE 'Countries' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Alpha2' TEXT NOT NULL,
                'Alpha3' TEXT NOT NULL,
                'EnglishName' TEXT NOT NULL,
                'Region' TEXT NOT NULL,
                'Subregion' TEXT NOT NULL,
                'Population' INTEGER NOT NULL,
                'Area' REAL
        );
    '''
    cur.execute(statement)

    conn.commit()
    conn.close()

def insert_json_data():
    file = open(COUNTRIESJSON, 'r')
    content = file.read()
    data = json.loads(content)
    file.close()
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    for d in data:
        statement = '''
            INSERT INTO 'Countries' (Id, Alpha2, Alpha3, EnglishName, Region, Subregion, Population, Area) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        '''
        cur.execute(statement, (None, d['alpha2Code'], d['alpha3Code'], d['name'], d['region'], d['subregion'], d['population'], d['area']))
    conn.commit()
    conn.close()
    '''
    dumped_file = json.dumps(data, indent=4)
    fw = open(COUNTRIESJSON,"w")
    fw.write(dumped_file)
    fw.close()
    '''

def insert_csv_data():
    with open(BARSCSV) as f:
        csvReader = csv.reader(f)
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()

        for row in csvReader:
            if row[0] == "Company":
                continue
            statement = '''
                INSERT INTO 'Bars' (Id, Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, CompanyLocationId, Rating, BeanType, BroadBeanOriginId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            company_location_id = cur.execute("SELECT Id FROM 'Countries' WHERE EnglishName=?", (row[5],)).fetchone()[0]
            if row[8] == 'Unknown':
                broad_bean_origin_id = None
            else:
                broad_bean_origin_id = cur.execute("SELECT Id FROM 'Countries' WHERE EnglishName=?", (row[8],)).fetchone()[0]

            cur.execute(statement, (None, row[0], row[1], row[2], row[3], row[4], company_location_id, row[6], row[7], broad_bean_origin_id))

        conn.commit()
        conn.close()


# Part 2: Implement logic to process user commands
def process_command(command):
    return []


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')

        if response == 'help':
            print(help_text)
            continue

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    # interactive_prompt()
    init_db()
    insert_json_data()
    insert_csv_data()
