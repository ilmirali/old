import cx_Oracle

fields1 = list()
fields2 = list()
structure = [' name:', 'type:', 'display_size:', 'internal_size:', 'precision:', 'scale:', 'null_ok:']
view = '_mi_mihcprofawp'  # '_drug_drugsreg' '_mi_healthissueslistv' '_mi_mihcprofawp'

with open('Accesses_and_source_list.txt', "r") as txt:
    s = txt.readlines()
    s = [line.rstrip() for line in s]

# db1 - source (views) ---------------------------------------------------------

db1_name = str(s[0])  # example: EHR_PROD_DC2
db1_username = str(s[1])  # example: EHR
db1_pass = str(s[2])  # example: a13dfa5
db1_conn_str = str(s[3])  # format: '@<URL>:<port>/<service_name>'
db1 = db1_username + '/' + db1_pass + db1_conn_str
conn = cx_Oracle.connect(db1)
cursor = conn.cursor()
query = str('SELECT * FROM ') + str(db1_username) + \
        str('."') + view + str('" FETCH FIRST 1 ROWS ONLY')
cursor.execute(query)
for i in cursor.description:
    fields1.append(i)
cursor.close()
# for i in fields1:
#    print(i, end='\n')

# db2 - target (tables) ---------------------------------------------------------

db2_name = str(s[5])  # example: EHRSTAGE_PROD_DC2
db2_username = str(s[6])  # example: EHRSTAGE
db2_pass = str(s[7])  # example: b13dfa5
db2_conn_str = str(s[8])  # format: '@<URL>:<port>/<service_name>'
db2 = db2_username + '/' + db2_pass + db2_conn_str
conn = cx_Oracle.connect(db2)
cursor = conn.cursor()
query = str('SELECT * FROM ') + str(db2_username) + \
        str('."') + view + str('" FETCH FIRST 1 ROWS ONLY')
cursor.execute(query)
for i in cursor.description:
    fields2.append(i)
cursor.close()
# for i in fields2:
#    print(i, end='\n')

# Check fields between source and target ---------------------------------------------------------
flag = 0

if len(fields1) == len(fields2):
    print('View:', view)
    print('The number of fields between source and target are same')
    print('Source:', len(fields1), '|', db1_name, '(', db1_username + db1_conn_str, ')')
    print('Target:', len(fields2), '|', db2_name, '(', db2_username + db2_conn_str, ')')

if len(fields1) != len(fields2):
    print('View:', view)
    print('The number of fields between source and target does not match:')
    print('Source:', len(fields1), '|', db1_name, '(', db1_username + db1_conn_str, ')')
    print('Target:', len(fields2), '|', db2_name, '(', db2_username + db2_conn_str, ')')
    print()

    if len(fields1) > len(fields2):
        print('New fields in source(', abs(len(fields1) - len(fields2)), ') :')
        for i in range(len(fields2)):
            if fields2[i] not in fields1:
                print(structure[0], fields2[i][0],
                      ';', structure[1], fields2[i][1],
                      ';', structure[2], fields2[i][2],
                      ';', structure[3], fields2[i][3],
                      ';', structure[4], fields2[i][4],
                      ';', structure[5], fields2[i][5],
                      ';', structure[6], fields2[i][6])
    elif len(fields1) < len(fields2):
        print('Deleted fields from source(', abs(len(fields1) - len(fields2)), ') :')
        for i in range(len(fields1)):
            if fields1[i] not in fields2:
                print(structure[0], fields1[i][0],
                      ';', structure[1], fields1[i][1],
                      ';', structure[2], fields1[i][2],
                      ';', structure[3], fields1[i][3],
                      ';', structure[4], fields1[i][4],
                      ';', structure[5], fields1[i][5],
                      ';', structure[6], fields1[i][6])

else:
    for i in range(len(fields1)):
        for j in range(len(fields1[i])):
            if fields1[i][j] != fields2[i][j]:
                print('Changed', structure[j], 'in field:', fields1[i][0], '.',
                      'Value in source:', fields1[i][j], 'Value in target:', fields2[i][j])
            # else:
                # flag = 1

# if flag == 1:

