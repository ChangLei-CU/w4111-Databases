from src.CSVDataTable import CSVDataTable
import logging
import os
import json

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)
# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

data_dir = os.path.abspath("../Data/Baseball")


def t_find_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'AB', 'H', 'HR', 'RBI']
    tmp = {'teamID': 'BOS', 'yearID': '1960'}

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    res = csv_tbl.find_by_template(template=tmp, field_list=fields)

    print("Query result: \n", json.dumps(res, indent=2))


def t_find_by_primary_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'AB', 'H', 'HR', 'RBI']
    key_vals = ['willite01', 'BOS', '1960', '1']

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    res = csv_tbl.find_by_primary_key(key_fields=key_vals, field_list=fields)

    print("Query result: \n", json.dumps(res, indent=2))


def t_insert():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID']
    new_rec = {'playerID': 'cl3910', 'teamID': 'CU', 'yearID': '2019'}

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
    csv_tbl.insert(new_record=new_rec)

    res = csv_tbl.find_by_template(template=new_rec, field_list=fields)

    print("New record inserted: \n", json.dumps(res, indent=2))


def t_update_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    tmp = {'teamID': 'BOS', 'yearID': '1960'}
    new_vals = {'yearID': '2020'}

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    cnt = csv_tbl.update_by_template(template=tmp, new_values=new_vals)

    print("Number of rows updated:", cnt)


def t_update_by_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    key_vals = ['willite01', 'BOS', '1960', '1']
    new_vals = {'yearID': '2020'}

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    cnt = csv_tbl.update_by_key(key_fields=key_vals, new_values=new_vals)

    print("Number of rows updated:", cnt)


def t_delete_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    tmp = {'teamID': 'BOS', 'yearID': '1970'}

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    cnt = csv_tbl.delete_by_template(template=tmp)

    print("Number of rows deleted:", cnt)


def t_delete_by_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['teamID', 'yearID', 'stint']
    key_vals = ['BOS', '1970', '1']

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    cnt = csv_tbl.delete_by_key(key_fields=key_vals)

    print("Number of rows deleted:", cnt)

# t_find_by_template()
# t_find_by_primary_key()
# t_insert()
# t_update_by_template()
# t_update_by_key()
# t_delete_by_template()
# t_delete_by_key()
