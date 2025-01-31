import pymysql
import src.data_service.dbutils as dbutils
import src.data_service.RDBDataTable as RDBDataTable

# The REST application server app.py will be handling multiple requests over a long period of time.
# It is inefficient to create an instance of RDBDataTable for each request.  This is a cache of created
# instances.
_db_tables = {}


def get_rdb_table(table_name, db_name, key_columns=None, connect_info=None):
    """

    :param table_name: Name of the database table.
    :param db_name: Schema/database name.
    :param key_columns: This is a trap. Just use None.
    :param connect_info: You can specify if you have some special connection, but it is
        OK to just use the default connection.
    :return:
    """
    global _db_tables

    # We use the fully qualified table name as the key into the cache, e.g. lahman2019clean.people.
    key = db_name + "." + table_name

    # Have we already created and cache the data table?
    result = _db_tables.get(key, None)

    # We have not yet accessed this table.
    if result is None:
        # Make an RDBDataTable for this database table.
        result = RDBDataTable.RDBDataTable(table_name, db_name, key_columns, connect_info)

        # Add to the cache.
        _db_tables[key] = result

    return result


#########################################
#
#
# YOU HAVE TO IMPLEMENT THE FUNCTIONS BELOW.
#
#
# -- TO IMPLEMENT --
#########################################

def get_databases():
    """

    :return: A list of databases/schema at this endpoint.
    """

    # -- TO IMPLEMENT --
    connect_info = {
        'host': 'localhost',
        'user': 'root',
        'password': '006121',
        'db': 'lahman2019clean',
        'port': 3306
    }

    _default_connect_info = pymysql.connect(
        host=connect_info['host'],
        user=connect_info['user'],
        password=connect_info['password'],
        db=connect_info['db'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)
    _db_names = []
    sql = "show databases"
    res, d = dbutils.run_q(sql, args=None, fetch=True, conn=_default_connect_info, commit=True)
    _db_names = [db_name['Database'] for db_name in d]

    return _db_names


def get_tables(dbname):
    connect_info = {
        'host': 'localhost',
        'user': 'root',
        'password': '006121',
        'db': dbname,
        'port': 3306
    }

    _default_connect_info = pymysql.connect(
        host=connect_info['host'],
        user=connect_info['user'],
        password=connect_info['password'],
        db=connect_info['db'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)

    _tb_names = []
    sql = "show tables"
    res, d = dbutils.run_q(sql, args=None, fetch=True, conn=_default_connect_info, commit=True)
    _tb_names = [tb_name['Tables_in_' + dbname] for tb_name in d]

    return _tb_names
