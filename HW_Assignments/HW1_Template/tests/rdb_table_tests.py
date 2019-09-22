from src.RDBDataTable import RDBDataTable
import logging
import json

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# modify the connection info
c_info = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "006121",
    "db": "hw1"
}


def t_find_by_template():
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID']
    tmp = {'teamID': 'CHN', 'yearID': '1890', 'HBP': '2'}

    rdb_tbl = RDBDataTable(c_info["db"] + ".batting", connect_info=c_info, key_columns=key_cols)

    res = rdb_tbl.find_by_template(template=tmp, field_list=fields)

    print("Query result: \n", json.dumps(res, indent=2))


def t_find_by_primary_key():
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'AB', 'H', 'HR', 'RBI']
    key_vals = ['willite01', 'BOS', '1940', '1']

    rdb_tbl = RDBDataTable(c_info["db"] + ".batting", connect_info=c_info, key_columns=key_cols)

    res = rdb_tbl.find_by_primary_key(key_fields=key_vals, field_list=fields)

    print("Query result: \n", json.dumps(res, indent=2))


def t_insert():
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID']
    new_rec = {'playerID': 'cl3910', 'teamID': 'CU', 'yearID': '2019'}

    rdb_tbl = RDBDataTable(c_info["db"] + ".batting", connect_info=c_info, key_columns=key_cols)

    rdb_tbl.insert(new_record=new_rec)

    res = rdb_tbl.find_by_template(template=new_rec, field_list=fields)

    print("New record inserted: \n", json.dumps(res, indent=2))


def t_update_by_template():
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    tmp = {'teamID': 'HOU', 'yearID': '1990'}
    new_vals = {'stint': '2'}

    rdb_tbl = RDBDataTable(c_info["db"] + ".batting", connect_info=c_info, key_columns=key_cols)

    cnt = rdb_tbl.update_by_template(template=tmp, new_values=new_vals)

    print("Number of rows updated:", cnt)


def t_update_by_key():
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    key_vals = ['willite01', 'BOS', '1950', '1']
    new_vals = {'yearID': '2020'}

    rdb_tbl = RDBDataTable(c_info["db"] + ".batting", connect_info=c_info, key_columns=key_cols)

    cnt = rdb_tbl.update_by_key(key_fields=key_vals, new_values=new_vals)

    print("Number of rows updated:", cnt)


def t_delete_by_template():
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    tmp = {'yearID': '1900'}

    rdb_tbl = RDBDataTable(c_info["db"] + ".batting", connect_info=c_info, key_columns=key_cols)

    cnt = rdb_tbl.delete_by_template(template=tmp)

    print("Number of rows deleted:", cnt)


def t_delete_by_key():
    key_cols = ['playerID', 'teamID']
    key_vals = ['bowerfr01', 'PIT']  # one-to-one map between 'key_cols' and 'key_vals', modify them together

    rdb_tbl = RDBDataTable(c_info["db"] + ".batting", connect_info=c_info, key_columns=key_cols)

    cnt = rdb_tbl.delete_by_key(key_fields=key_vals)

    print("Number of rows deleted:", cnt)


# t_find_by_template()
# t_find_by_primary_key()
# t_insert()
# t_update_by_template()
# t_update_by_key()
# t_delete_by_template()
# t_delete_by_key()
