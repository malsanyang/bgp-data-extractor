import gzip
import os
import mysql.connector

DB_HOST = '127.0.0.1'
DB_NAME = 'ejaw_data'
DB_USER = 'root'
DB_PASSWORD = 'root'

SOURCE_DIR = '/Users/malsanyang/Development/ejaw/data_extractor/data/'
OUTPUT_DIR = '/Users/malsanyang/Development/ejaw/data_extractor/output/'

def extractDataFromPath(root):
    data = []
    root = root.replace(SOURCE_DIR, '')
    parts = root.split('/')
    for part in parts:
        if part == '':
            continue

        if part.startswith('year'):
            data.append(part.replace('year', ''))
        else:
            values = part.split('=')
            data.append(values[1])

    return data

def extractDataFromFile(headingData, row):
    parts = row.split('|')
    data = headingData.copy()

    data.append(parts[0])
    data.append(parts[1])
    data.append(parts[2])

    return data

# def batch(list, size=10):
#     l = len(list)
#     for item in range(0, l, size):
#         yield list[item:min(item + size, l)]

def insertToDb(records):
    con = mysql.connector.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, database=DB_NAME)
    cursor = con.cursor()

    query = 'INSERT INTO route_records(year, month, day, hour, time, prefix, origin, peer_cnt) VALUES(%s, %s, %s, %s, %s, %s, %s, %s);'

    cursor.executemany(query, records)
    con.commit()
    
    con.close()



for root, subFolders, files in os.walk(SOURCE_DIR):
    headingData = extractDataFromPath(root)
    terminate = 0
    for file in files:
        if not file.endswith('.gz'):
            continue

        records = []
        print('processing file: ' + file)
        f = gzip.open(root + '/' + file, 'rb')
        for line in f:
            
            row_data = line.decode('utf-8')
            row_data = row_data.strip()
            if row_data.startswith('#') or row_data.startswith('prefix'):
                continue

            data = extractDataFromFile(headingData, row_data)
            records.append(data)
        
        print ('inserting records for: ' + file)
        insertToDb(records)
        print ('successfully inserted records: ' + file)
        
        f.close()
        terminate = 1
        break

    if (terminate == 1):
        break
