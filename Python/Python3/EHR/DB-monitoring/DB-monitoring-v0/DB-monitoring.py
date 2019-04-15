import cx_Oracle


# ORA-12170: TNS:Connect timeout occurred
# ORA-12560: TNS:protocol adapter error
# ORA-12516: TNS:listener could not find available handler with matching protocol stack
# ORA-00913: too many values
# ORA-01722: invalid number

# cx_Oracle.STRING
# cx_Oracle.NUMBER - in Oracle SQL its NUMBER AND FLOAT | <class 'cx_Oracle.NUMBER'>
# cx_Oracle.DATETIME
# cx_Oracle.TIMESTAMP
# ORA-00947: not enough values

def get_db_object_desc_fun(db_conn_str, db_username, dbobject):
    '''
    get description of view or table
    :param db_conn_str: connection string with username, pass, etc
    :param db_username: schema name
    :param dbobject: view or table, in target - tables, in source - views (usually)
    :return: list of attributes (7) for each field
    '''
    tmp_list = list()
    cursor = cx_Oracle.connect(db_conn_str).cursor()
    query = str('SELECT * FROM ') + str(db_username) + '.' + '"' + str(dbobject) + '"' + str(' FETCH FIRST 1 ROWS ONLY')
    cursor.execute(query)
    tmp_list = cursor.description
    cursor.close()
    return tmp_list


def fields_with_attr_by_db_object_fun(fields, fields_dict):
    tmp_dict = dict()
    for db_object in db_objects:
        for attribute in fields[db_object]:
            tmp_dict[attribute[0]] = {}  # attribute[0] - it's field name
        fields_dict[db_object] = tmp_dict
        tmp_dict = {}
        for key in fields_dict[db_object].keys():
            for attribute in fields[db_object]:
                if key == attribute[0]:
                    fields_dict[db_object][key] = attribute
    return fields_dict


def new_fields_detect_fun(db_object):
    '''
    new fields detection
    :param db_object: view name
    :return: list of new fields with attributes from source
    '''
    tmp_list = list()
    field_name = ''
    new_fields_dict = dict()
    for key in source_fields_dict[db_object].keys():
        if key not in target_fields_dict[db_object].keys():
            field_name = key
            for key, value in source_fields_dict[db_object].items():
                if key == field_name:
                    tmp_list.append(value)
    new_fields_dict[db_object] = tmp_list.copy()
    tmp_list.clear()
    return new_fields_dict[db_object]


def deleted_fields_detect_fun(db_object):
    '''
    deleted fields detection
    :param db_object: view name
    :return: list of deleted fields with attributes from target
    '''
    tmp_list = list()
    field_name = ''
    deleted_fields_dict = dict()
    for key in target_fields_dict[db_object].keys():
        if key not in source_fields_dict[db_object].keys():
            field_name = key
            for key, value in target_fields_dict[db_object].items():
                if key == field_name:
                    tmp_list.append(value)
    deleted_fields_dict[db_object] = tmp_list.copy()
    tmp_list.clear()
    return deleted_fields_dict[db_object]


attributes = ['name', 'type', 'display_size', 'internal_size', 'precision', 'scale', 'null_ok']  # attributes list (7)
source_fields = dict()  # list of views with 7 attributes for each field (from source) by db_object
target_fields = dict()  # list of tables with 7 attributes for each field (from target) by db_object
source_fields_dict = dict()  # double dict: {db_object: {field: (list of attributes)}}
target_fields_dict = dict()  # double dict: {db_object: {field: (list of attributes)}}
deleted_fields_dict = dict()
new_fields_dict = dict()

# file have the list of DB objects names (views or tables). Example (views): '_hp_hcprof', '_mi_mihcprofawp'
# here may be ~100 names of DB objects
with open('DB_objects.txt', "r") as txt:
    db_objects = txt.readlines()
    db_objects = [line.strip() for line in db_objects]

# file have source and target connection strings (2 strings to 2 different DB)
with open('Access.txt', "r") as txt:
    source_conn_str = txt.readline().rstrip()  # JDBC connection string (source)
    target_conn_str = txt.readline().rstrip()  # JDBC connection string (target)
    source_username = source_conn_str.split('/')[0]  # DB username (source); example: EHR
    target_username = target_conn_str.split('/')[0]  # DB username (target); example: EHRSTAGE

# get current timestamp to use like a GUID for insert in table with result of compare views in source and target
# stackoverflow.com/questions/53432923/how-to-convert-python-datetime-to-timestamp-and-insert-in-oracle-database-using
db_conn_str = target_conn_str
resp = list()
cursor = cx_Oracle.connect(db_conn_str).cursor()
query = str("SELECT SYSTIMESTAMP FROM DUAL")
cursor.execute(query)
for i in cursor:
    resp.append(i)
print(resp[0][0], type(resp[0][0]))
cursor.close()

'''
conn = cx_Oracle.connect(target_conn_str)
cursor = conn.cursor()
sql = "INSERT INTO EHRSTAGE.TEST (LOAD_DATE, START_TIMESTAMP) VALUES (TRUNC(SYSDATE), SYSTIMESTAMP)"
cursor.execute(sql, None)
cursor.close()
conn.commit()
'''

# get list of views/tables with 7 attributes for each field (from source & target)
for db_object in db_objects:
    source_fields[db_object] = get_db_object_desc_fun(source_conn_str, source_username, db_object)
    target_fields[db_object] = get_db_object_desc_fun(target_conn_str, target_username, db_object)

# здесь будет коммент
source_fields_dict = fields_with_attr_by_db_object_fun(source_fields, source_fields_dict)
target_fields_dict = fields_with_attr_by_db_object_fun(target_fields, target_fields_dict)

print(source_fields)

# _mi_visitlistv source 52 target 37
# --_mi_proceduredoc source 46 target 46
# _mi_referralvdoc source 58 target 47
# --_mi_vactinationdoc source 77 target 62

# здесь будет коммент
for db_object in db_objects:
    source_fields_count = len(source_fields_dict[db_object])
    target_fields_count = len(target_fields_dict[db_object])

    conn = cx_Oracle.connect(target_conn_str)
    cursor = conn.cursor()
    dtime = resp[0][0]
    sql = "INSERT INTO EHRSTAGE.TEST (LOAD_DATE, TIMESTAMP, DB_OBJECT, " \
          "SOURCE_FIELDS_COUNT, TARGET_FIELDS_COUNT) " \
          "VALUES (TRUNC(SYSDATE), :ts, :view_name, :count_s, :count_t)"
    cursor.setinputsizes(ts=cx_Oracle.TIMESTAMP)
    cursor.execute(sql, ts=dtime, view_name=db_object,
                   count_s=int(source_fields_count), count_t=int(target_fields_count))
    cursor.close()
    conn.commit()

    # здесь будет коммент
    if len(new_fields_detect_fun(db_object)) != 0:
        new_fields_dict[db_object] = new_fields_detect_fun(db_object)
        # здесь insert в БД
        conn = cx_Oracle.connect(target_conn_str)
        cursor = conn.cursor()
        sql = "UPDATE EHRSTAGE.TEST " \
              "SET NEW_FIELDS_COUNT=:new_count, NEW_FIELDS_COUNT_DET=:new_list " \
              "WHERE timestamp = :ts AND db_object = :view_name "
        cursor.setinputsizes(ts=cx_Oracle.TIMESTAMP)
        cursor.execute(sql, new_count=int(len(new_fields_dict[db_object])), new_list=str(new_fields_dict[db_object]),
                       ts=dtime, view_name=db_object)
        cursor.close()
        conn.commit()

    # здесь будет коммент
    if len(deleted_fields_detect_fun(db_object)) != 0:
        deleted_fields_dict[db_object] = deleted_fields_detect_fun(db_object)
        # здесь insert в БД

    # changed attributes detection loop
    # здесь проверка по атрибутам

# print(new_fields_dict)
# print(deleted_fields_dict)


# source_fields_dict[db_object][field_name][attribute_name]

# вывод в формате:
# source_fields _mi_visitlistv mi_id
# target_fields _mi_visitlistv ('mi_id', <class 'cx_Oracle.NUMBER'>, 20, None, 19, 0, 1)
# for db_object in db_objects:
# print('source_fields', db_object, source_fields[db_object][0][0])
# print('target_fields', db_object, target_fields[db_object][0])


