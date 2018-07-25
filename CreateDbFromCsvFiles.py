
import csv, ast

import os
pathName = os.getcwd()
numFiles = []
fileNames = os.listdir(pathName)
for fileNames in fileNames:
    if fileNames.endswith(".csv"):
        numFiles.append(fileNames)

        
for file in numFiles:
        

    f = open(file, 'r')
    reader = csv.reader(f)
    
    longest, headers, type_list = [], [], []
    
    
    def dataType(val, current_type):
        try:
         
            t = ast.literal_eval(val)
        except ValueError:
            return 'varchar'
        except SyntaxError:
            return 'varchar'
        if type(t) in [int, float]:
            if (type(t) in [int]) and current_type not in ['float', 'varchar']:
               # Use smallest possible int type
               if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:
                   return 'smallint'
               elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:
                   return 'int'
               else:
                   return 'bigint'
            if type(t) is float and current_type not in ['varchar']:
               return 'decimal'
        else:
            return 'varchar'
    
    #print (file.replace(".csv", ""))
    
    for row in reader:
        if len(headers) == 0:
            headers = row
            for col in row:
                longest.append(0)
                type_list.append('')
        else:
            for i in range(len(row)):
                # NA is the csv null value
                if row[i] == 'NA':
                    pass
                elif type_list[i] == 'varchar':
                    if len(row[i]) > longest[i]:
                        longest[i] = len(row[i])
                else:
                    var_type = dataType(row[i], type_list[i])
                    type_list[i] = var_type
            if len(row[i]) > longest[i]:
                longest[i] = len(row[i])
    f.close()    
    
    
    statement = 'create table ' + file.replace(".csv", "") + ' ('
    
    for i in range(len(headers)):
        if type_list[i] == 'varchar':
            statement = (statement + '\n{} varchar({}),').format(headers[i].lower(), str(longest[i]))
        else:
            statement = (statement + '\n' + '{} {}' + ',').format(headers[i].lower(), type_list[i])
    
    statement = statement[:-1] + ');'
        
    
    
    
    
    
    
    import pyodbc
    connection_string= 'DSN=sql3;UID=;PWD='
    connection= pyodbc.connect(connection_string)
    cursor = connection.cursor()
    
    cursor.execute(statement)
    connection.commit()
    
    
    with open (file, 'r') as f:
        reader = csv.reader(f)
        columns = next(reader) 
        query = 'insert into ' + file.replace(".csv", "") + '({0}) values ({1})'
        query = query.format(','.join(columns), ','.join('?' * len(columns)))
        cursor = connection.cursor()
        for data in reader:
            
            for index, item in enumerate(data):
                if (type_list[index] != 'varchar') and (item=='NA'):
                    data[index] = 0
    
            cursor.execute(query, data)
        cursor.commit()
    
    connection.close()
