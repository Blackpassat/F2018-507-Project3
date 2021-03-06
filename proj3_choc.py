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

            cur.execute(statement, (None, row[0], row[1], row[2], row[3], float(row[4][:-1])*0.01, company_location_id, row[6], row[7], broad_bean_origin_id))

        conn.commit()
        conn.close()


# Part 2: Implement logic to process user commands
def bars_command(command):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    words = command.split()
    sellcountry = ''
    sourcecountry = ''
    sellregion = ''
    sourceregion = ''
    order = 'B.Rating'
    sort = 'DESC'
    limit = 10
    
    statement1 = '''
        SELECT B.SpecificBeanBarName,B.Company, C1.EnglishName, B.Rating, B.CocoaPercent, C2.EnglishName
        FROM Bars B
        LEFT JOIN Countries C1
            ON B.CompanyLocationId = C1.Id
        LEFT JOIN Countries C2
            ON B.BroadBeanOriginId = C2.Id
    '''
    l = len(words)
    for w in words:
        check_input_flag = False
        if 'sellcountry' in w:
            if '=' in w:
                sellcountry = w[w.index('=')+1:]
            statement2 = 'WHERE C1.Alpha2="' + sellcountry + '"'
            if '=' in w:
                if w[:w.index('=')] == 'sellcountry' and sellcountry != '':
                    check_input_flag = True
        if 'sourcecountry' in w:
            if '=' in w:
                sourcecountry = w[w.index('=')+1:]
            statement2 = 'WHERE C2.Alpha2="' + sourcecountry + '"'
            if '=' in w:
                if w[:w.index('=')] == 'sourcecountry' and sourcecountry != '':
                    check_input_flag = True
        if 'sellregion' in w:
            if '=' in w:
                sellregion = w[w.index('=')+1:]
            statement2 = 'WHERE C1.Region="' + sellregion + '"'
            if '=' in w:
                if w[:w.index('=')] == 'sellregion' and sellregion != '':
                    check_input_flag = True
        if 'sourceregion' in w:
            if '=' in w:
                sourceregion = w[w.index('=')+1:]
            statement2 = 'WHERE C2.Region="' + sourceregion + '"'
            if '=' in w:
                if w[:w.index('=')] == 'sourceregion' and sourceregion != '':
                    check_input_flag = True
        if w == 'cocoa':
            order = 'B.CocoaPercent'
            check_input_flag = True
        if 'bottom' in w:
            sort = 'ASC'
            if '=' in w:
                try:
                    limit = int(w[w.index('=')+1:])
                except ValueError:
                    limit = ''
                else:
                    if w[:w.index('=')] == 'bottom':
                        check_input_flag = True
        if 'top' in w:
            if '=' in w:
                try:
                    limit = int(w[w.index('=')+1:])
                except ValueError:
                    limit = ''
                else:
                    if w[:w.index('=')] == 'top':
                        check_input_flag = True
        if w == 'ratings':
            check_input_flag = True
        if w == 'bars' and l >= 1:
            check_input_flag = True

        if not check_input_flag:
            return ''

    if sellcountry=='' and sourcecountry=='' and sellregion=='' and sourceregion=='':
        statement2 = ''

    statement = statement1 + statement2 + ' ORDER BY ' + order + ' ' + sort + ' LIMIT ' + str(limit)
    cur.execute(statement)
    result = cur.fetchall()
    conn.close()

    # print(pp(cur, result, rowlens=1))

    return result

def companies_command(command):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    words = command.split()
    sellcountry = ''
    sellregion = ''
    statement2 = 'AVG(B.Rating) AS ratings'
    order = 'ratings'
    sort = 'DESC'
    limit = 10
    statement1 = 'SELECT B.Company, C1.EnglishName, '
    
    l = len(words)
    statement3 = '''
        FROM Bars B
        JOIN Countries C1
            ON B.CompanyLocationId = C1.Id
        GROUP BY B.Company
        HAVING COUNT(B.SpecificBeanBarName) > 4
    '''
    for w in words:
        check_input_flag = False
        if 'country' in w:
            if '=' in w:
                sellcountry = w[w.index('=')+1:]
                if w[:w.index('=')] == 'country' and sellcountry != '':
                    check_input_flag = True
            statement4 = 'AND C1.Alpha2="' + sellcountry + '"'
        if 'region' in w:
            if '=' in w:
                sellregion = w[w.index('=')+1:]
                if w[:w.index('=')] == 'region' and sellregion != '':
                    check_input_flag = True
            statement4 = 'AND C1.Region="' + sellregion + '"'
        if w == 'cocoa':
            statement2 = 'AVG(B.CocoaPercent) AS cocoas'
            order = 'cocoas'
            check_input_flag = True
        if w == 'bars_sold':
            statement2 = 'COUNT(B.SpecificBeanBarName) AS kinds'
            order = 'kinds'
            check_input_flag = True
        if 'bottom' in w:
            sort = 'ASC'
            if '=' in w:
                try:
                    limit = int(w[w.index('=')+1:])
                except:
                    limit = ''
                else:
                    if w[:w.index('=')] == 'bottom':
                        check_input_flag = True
        if 'top' in w:
            if '=' in w:
                try:
                    limit = int(w[w.index('=')+1:])
                except:
                    limit = ''
                else:
                    if w[:w.index('=')] == 'top':
                        check_input_flag = True
        if w == 'ratings':
            check_input_flag = True
        if w == 'companies' and l >= 1:
            check_input_flag = True

        if not check_input_flag:
            return ''

    if sellcountry=='' and sellregion=='':
        statement4 = ''

    statement = statement1 + statement2 + statement3 + statement4 + ' ORDER BY ' + order + ' ' + sort + ' LIMIT ' + str(limit)
    # print(statement)
    cur.execute(statement)
    result = cur.fetchall()
    conn.close()

    # print(pp(cur, result, rowlens=1))

    return result

def countries_command(command):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    words = command.split()
    sellcountry = ''
    region = ''
    statement1 = 'SELECT C1.EnglishName, C1.Region, '
    statement2 = 'AVG(B.Rating) AS ratings '
    statement3 = 'GROUP BY C1.Id'
    statement4 = ''
    order = 'ratings'
    sort = 'DESC'
    limit = 10
    region_flag = False
    source_flag = False
    l = len(words)
    statement5 = '''
        FROM Bars B
        LEFT JOIN Countries C1
            ON B.CompanyLocationId = C1.Id
        LEFT JOIN Countries C2
            ON B.BroadBeanOriginId = C2.Id
    '''
    for w in words:
        check_input_flag = False
        if 'region' in w:
            if '=' in w:
                region = w[w.index('=')+1:]
                if w[:w.index('=')] == 'region' and region != '':
                    check_input_flag = True
            region_flag = True
        if w == 'sources':
            source_flag = True
            statement1 = 'SELECT C2.EnglishName, C2.Region, '
            statement3 = 'GROUP BY C2.Id'
            check_input_flag = True
        if w == 'cocoa':
            statement2 = 'AVG(B.CocoaPercent) AS cocoas'
            order = 'cocoas'
            check_input_flag = True
        if w == 'bars_sold':
            statement2 = 'COUNT(B.SpecificBeanBarName) AS kinds'
            order = 'kinds'
            check_input_flag = True
        if 'bottom' in w:
            sort = 'ASC'
            if '=' in w:
                try:
                    limit = int(w[w.index('=')+1:])
                except ValueError:
                    limit = ''
                else:
                    if w[:w.index('=')] == 'bottom':
                        check_input_flag = True
        if 'top' in w:
            if '=' in w:
                try:
                    limit = int(w[w.index('=')+1:])
                except ValueError:
                    limit = ''
                else:
                    if w[:w.index('=')] == 'top':
                        check_input_flag = True
        if w == 'sellers':
            check_input_flag = True
        if w == 'ratings':
            check_input_flag = True
        if w == 'countries' and l >= 1:
            check_input_flag = True

        if not check_input_flag:
            return ''

    if region_flag:
        if source_flag:
            statement4 = 'AND C2.Region="' + region + '"'
        else:
            statement4 = 'AND C1.Region="' + region + '"'

    statement = statement1 + statement2 + statement5 + statement3 + ' HAVING COUNT(B.SpecificBeanBarName)>4 ' + statement4 + ' ORDER BY ' + order + ' ' + sort + ' LIMIT ' + str(limit)
    # print(statement)
    cur.execute(statement)
    result = cur.fetchall()
    conn.close()

    # print(pp(cur, result, rowlens=1))

    return result

def regions_command(command):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    words = command.split()
    sellcountry = ''
    region = ''
    statement1 = 'SELECT C1.Region, '
    statement2 = 'AVG(B.Rating) AS ratings '
    statement3 = 'GROUP BY C1.Region'
    statement4 = 'AND C1.Region IS NOT NULL'
    order = 'ratings'
    sort = 'DESC'
    limit = 10
    l = len(words)
    statement5 = '''
        FROM Bars B
        LEFT JOIN Countries C1
            ON B.CompanyLocationId = C1.Id
        LEFT JOIN Countries C2
            ON B.BroadBeanOriginId = C2.Id
    '''
    for w in words:
        check_input_flag = False
        if w == 'sources':
            statement1 = 'SELECT C2.Region, '
            statement3 = 'GROUP BY C2.Region'
            statement4 = 'AND C2.Region IS NOT NULL'
            check_input_flag = True
        if w == 'cocoa':
            statement2 = 'AVG(B.CocoaPercent) AS cocoas'
            order = 'cocoas'
            check_input_flag = True
        if w == 'bars_sold':
            statement2 = 'COUNT(B.SpecificBeanBarName) AS kinds'
            order = 'kinds'
            check_input_flag = True
        if 'bottom' in w:
            sort = 'ASC'
            if '=' in w:
                try:
                    limit = int(w[w.index('=')+1:])
                except ValueError:
                    limit = ''
                else:
                    if w[:w.index('=')] == 'bottom':
                        check_input_flag = True
        if 'top' in w:
            if '=' in w:
                try:
                    limit = int(w[w.index('=')+1:])
                except ValueError:
                    limit = ''
                else:
                    if w[:w.index('=')] == 'top':
                        check_input_flag = True
        if w == 'sellers':
            check_input_flag = True
        if w == 'ratings':
            check_input_flag = True
        if w == 'regions' and l >= 1:
            check_input_flag = True

        if not check_input_flag:
            return ''

    statement = statement1 + statement2 + statement5 + statement3 + ' HAVING COUNT(B.SpecificBeanBarName)>4 ' + statement4 + ' ORDER BY ' + order + ' ' + sort + ' LIMIT ' + str(limit)
    # print(statement)
    cur.execute(statement)
    result = cur.fetchall()
    conn.close()

    # print(pp(cur, result, rowlens=1))

    return result

def process_command(command):
    if 'bars' in command[:10]:
        result = bars_command(command)
    elif 'companies' in command:
        result = companies_command(command)
    elif 'countries' in command:
        result = countries_command(command)
    elif 'regions' in command:
        result = regions_command(command)
    elif command == 'exit':
        result = 'Bye'
    else:
        result = ''

    return result

def pp(cursor, data=None, rowlens=0):
    d = cursor.description
    if not d:
        return "#### NO RESULTS ###"
    names = []
    lengths = []
    rules = []
    if not data:
        data = cursor.fetchall(  )
    for dd in d:    # iterate over description
        l = dd[1]
        if not l:
            l = 12             # or default arg ...
        l = max(l, len(dd[0])) # Handle long names
        names.append(dd[0])
        lengths.append(l)
    for col in range(len(lengths)):
        if rowlens:
            rls = [len(row[col]) for row in data if row[col] and isinstance(row[col], str)]
            lengths[col] = max([lengths[col]]+rls)
        rules.append("-"*lengths[col])
    format = " ".join(["%%-%ss" % l for l in lengths])
    result = [format % tuple(names)]
    result.append(format % tuple(rules))
    for row in data:
        result.append(format % row)
    return "\n".join(result)

def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def nice_print(result):
    MAX_LENGTH = 15
    for item in result:
        line = ''
        for i in item:
            if i is None:
                i = 'Unknown'
            if isinstance(i, str):
                length = len(i)
                if length > 12:
                    line += i[:12]+'... '
                else:
                    line += i+(MAX_LENGTH-length+1)*' '
            else:
                if i > 1.0:
                    line += str(round(i, 1))+' '
                else:
                    line += str(int(round(i*100)))+'% '
        print(line)

def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')
        result = process_command(response)
        if result == []:
            print('Nothing Found. ')
        elif result == '':
            print('Invalid Input. Type <help> for instructions.')
        elif result == 'Bye':
            print(result)
        else:
            nice_print(result)
        if response == 'help':
            print(help_text)
            continue
        print('')
# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    init_db()
    insert_json_data()
    insert_csv_data()
    # process_command('regions sellers ratings top=10')
    interactive_prompt()
