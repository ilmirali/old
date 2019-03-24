import cx_Oracle
# (name, type, display_size, internal_size, precision, scale, null_ok)

fields1 = list()
fields2 = list()

with open('Accesses_and_source_list.txt', "r") as txt:
    s = txt.readlines()
    s = [line.rstrip() for line in s]

db1_name = str(s[0])        # example: 'EHR_PROD_DC2'
db1_username = str(s[1])    # example: EHR
db1_pass = str(s[2])        # example: a13dfa5
db1_conn_str = str(s[3])    # format: '@<URL>:<port>/<service_name>'
db1 = db1_username + '/' + db1_pass + db1_conn_str
conn = cx_Oracle.connect(db1)

cursor = conn.cursor()
cursor.execute('SELECT * FROM EHR."_mi_healthissueslistv" FETCH FIRST 1 ROWS ONLY')
for i in cursor.description:
    fields1.append(i)
cursor.close()
# for i in fields1:
#    print(i, end='\n')

# -------------

db2_name = str(s[5])        # example: 'EHR_PROD_DC2'
db2_username = str(s[6])    # example: EHR
db2_pass = str(s[7])        # example: a13dfa5
db2_conn_str = str(s[8])    # format: '@<URL>:<port>/<service_name>'
db2 = db2_username + '/' + db2_pass + db2_conn_str
conn = cx_Oracle.connect(db2)

cursor = conn.cursor()
cursor.execute('SELECT * FROM EHRSTAGE."_mi_healthissueslistv" FETCH FIRST 1 ROWS ONLY')
for i in cursor.description:
    fields2.append(i)
cursor.close()
# for i in fields2:
#    print(i, end='\n')

#################

if len(fields1) != len(fields2):
    print('The number of fields between source and target does not match:')
    print('Source:', len(fields1), '|', db1_name, '(', db1_username + db1_conn_str, ')')
    print('Target:', len(fields2), '|', db2_name, '(', db2_username + db2_conn_str, ')')
    print()
    print('Fields that are different between source and target(', abs(len(fields1) - len(fields2)), ') :')

    if len(fields1) > len(fields2):
        for i in range(len(fields2)):
            if fields2[i] not in fields1:
                print('fail', i, fields2[i]) # здесь надо доаписать вывод как в примере ниже
                # еще в оба вывода надо дописать название измененного поля
else:
    for i in range(len(fields1)):
        for j in range(len(fields1[i])):
            if fields1[i][j] != fields2[i][j]:
                print('fail', fields1[i][0], fields1[i][j], fields2[i][j])
