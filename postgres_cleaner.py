__author__ = 'Judge'
import pymongo.errors
import psycopg2.extensions
import datetime
import time
import math
import sys
import logcreate
import os
from contextlib import contextmanager
from pymongo import MongoClient
from psycopg2 import psycopg1 as psycopg
from decimal import Decimal


os.chdir(os.getcwd())
# config:
logname = 'pg.log'
# postgres connection data:
pg_database = 'nura_cleaner'
pg_user = 'postgres'
pg_password = 'postgres'
pg_host = '192.168.0.98'
# mongodb connection data:
mdb_host = '192.168.0.98'
mdb_user = 'mongodb'
mdb_pass = 'mongodb'
mdb_db = 'nura'

pg_dsn = 'database=pg_database, user=pg_user, password=pg_password, host=pg_host'
mongo_uri = 'mongodb://' + mdb_user + ':' + mdb_pass + '@' + mdb_host

daily_sql = ["DELETE FROM nonstop24payments WHERE userid IS NULL;",
             "DELETE FROM paymentsystemslog WHERE userid IS NULL;",
             "DELETE FROM paymentsystemslog WHERE NOT id IN (SELECT p.id FROM paymentsystemslog p INNER JOIN users u ON p.userid = u.id);",
             "DELETE FROM persists_auth_keys WHERE exparedate < now();",
             "DELETE FROM iptable WHERE add_date < (now()- INTERVAL '2 days');"
             ]

tmp_table_prepare_sql = [
    "DELETE FROM tmp_table_for_user_delete;",

    """
    INSERT INTO tmp_table_for_user_delete (userid)
        SELECT id FROM users WHERE
            NOT id IN (SELECT DISTINCT sourceuserid FROM cashtransfer) AND
            NOT id IN (SELECT DISTINCT destuserid FROM cashtransfer) AND
            NOT id IN (SELECT DISTINCT userid FROM log_cash_gold) AND
            NOT id IN (SELECT DISTINCT userid FROM log_cash_artcrystals) AND
            NOT id IN (SELECT DISTINCT userid FROM easypaypayments) AND
            NOT id IN (SELECT DISTINCT userid FROM interkassapayments) AND
            NOT id IN (SELECT DISTINCT userid FROM nonstop24payments) AND
            NOT id IN (SELECT DISTINCT userid FROM osmppayments) AND
            NOT id IN (SELECT DISTINCT userid FROM payments_myworld) AND
            NOT id IN (SELECT DISTINCT userid FROM payments_odkl) AND
            NOT id IN (SELECT DISTINCT userid FROM payments_play_market) AND
            NOT id IN (SELECT DISTINCT userid FROM paymentsystemslog) AND
            clanid IS NULL AND id IN (SELECT u.id FROM users u INNER JOIN userlogons ul
            ON u.id = ul.userid WHERE u.level = 0 AND ul.lastentertime < (now() - INTERVAL '1 weeks'));
    """,

    """
    INSERT INTO tmp_table_for_user_delete (userid)
        SELECT id FROM users WHERE
            NOT id IN (SELECT DISTINCT sourceuserid FROM cashtransfer) AND
            NOT id IN (SELECT DISTINCT destuserid FROM cashtransfer) AND
            NOT id IN (SELECT DISTINCT userid FROM log_cash_gold) AND
            NOT id IN (SELECT DISTINCT userid FROM log_cash_artcrystals) AND
            NOT id IN (SELECT DISTINCT userid FROM easypaypayments) AND
            NOT id IN (SELECT DISTINCT userid FROM interkassapayments) AND
            NOT id IN (SELECT DISTINCT userid FROM nonstop24payments) AND
            NOT id IN (SELECT DISTINCT userid FROM osmppayments) AND
            NOT id IN (SELECT DISTINCT userid FROM payments_myworld) AND
            NOT id IN (SELECT DISTINCT userid FROM payments_odkl) AND
            NOT id IN (SELECT DISTINCT userid FROM payments_play_market) AND
            NOT id IN (SELECT DISTINCT userid FROM paymentsystemslog) AND
            clanid IS NULL AND
            id IN (SELECT u.id FROM users u INNER JOIN userlogons ul ON u.id = ul.userid
            WHERE u.level > 0 AND u.level < 4 AND ul.lastentertime < (now() - INTERVAL '1 months'));
    """,

    """
    INSERT INTO tmp_table_for_user_delete (userid)
        SELECT id FROM users WHERE
            NOT id IN (SELECT DISTINCT sourceuserid FROM cashtransfer) AND
            NOT id IN (SELECT DISTINCT destuserid FROM cashtransfer) AND
            NOT id IN (SELECT DISTINCT userid FROM log_cash_gold) AND
            NOT id IN (SELECT DISTINCT userid FROM log_cash_artcrystals) AND
            NOT id IN (SELECT DISTINCT userid FROM easypaypayments) AND
            NOT id IN (SELECT DISTINCT userid FROM interkassapayments) AND
            NOT id IN (SELECT DISTINCT userid FROM nonstop24payments) AND
            NOT id IN (SELECT DISTINCT userid FROM osmppayments) AND
            NOT id IN (SELECT DISTINCT userid FROM payments_myworld) AND
            NOT id IN (SELECT DISTINCT userid FROM payments_odkl) AND
            NOT id IN (SELECT DISTINCT userid FROM payments_play_market) AND
            NOT id IN (SELECT DISTINCT userid FROM paymentsystemslog) AND
            clanid IS NULL AND
            id IN (SELECT u.id FROM users u INNER JOIN userlogons ul ON u.id = ul.userid
            WHERE u.level >= 4 AND u.level < 8 AND ul.lastentertime < (now() - INTERVAL '6 months'));
    """
]

sql_for_backup = {
    'users': ['id', 'SELECT * FROM users WHERE id IN (SELECT userid FROM tmp_table_for_user_delete);'],
    'usercash': ['userid', 'SELECT * FROM usercash WHERE userid IN (SELECT userid FROM tmp_table_for_user_delete);'],
    'inventory': ['id', 'SELECT * FROM inventory WHERE userid IN (SELECT userid FROM tmp_table_for_user_delete);']
}

sql_count = "SELECT count(*) FROM tmp_table_for_user_delete;"


class MongoWorker(MongoClient):
    def print_cursor(self, db, collection):
        for i in self.get_database(db).get_collection(collection).find():
            print(i)


class PostgresqlWorker(psycopg.cursor):
    @contextmanager
    def transaction_cursor(self):
        try:
            self.execute('begin work;')
            yield self
            self.execute('commit;')
        except:
            self.execute('rollback;')

    def get_columns_list(self, table='users'):
        columns_sql = "SELECT column_name FROM information_schema.columns WHERE table_schema=\'public\' " \
                      "AND table_name=\'{0}\'".format(table)
        self.execute(columns_sql)
        columns = [i[0] for i in self.fetchall()]
        return columns

    def count_limit(self, sql):
        tmp_line_numbers = self.row_numbers(sql)
        sec_to_deadline = time.mktime(datetime.date.today().timetuple()) + 86400
        timeleft = sec_to_deadline - time.time()
        rounds = timeleft / (5 * 60)
        limit = tmp_line_numbers[0][0] / rounds
        limit = math.ceil(limit) + 20
        return limit

    def row_numbers(self, sql):
        self.execute(sql)
        tmp_line_numbers = self.fetchall()
        return tmp_line_numbers

    def docs_for_mongo(self, sql, id_name, table):
        column_list = self.get_columns_list(table)
        column_list[column_list.index(id_name)] = '_id'
        self.execute(sql)
        self.mongo = []
        x = self.fetchall()
        for row in x:
            # doc = {x: y for x, y in zip(column_list, row)}
            doc = dict((x, y) for (x, y) in zip(column_list, row))
            for rec in doc:
                if type(doc[rec]) is Decimal:
                    doc[rec] = float(doc[rec])
            self.mongo.append(doc)
        # print(self.mongo)
        return self.mongo


def pg_backuper(pg_connection, sqls):
    pg = PostgresqlWorker(pg_connection)
    mw = MongoWorker(mongo_uri)
    for table in sorted(sqls):
        log.logwrite('backuping table:', table, ': ', sqls[table][1])
        print(sqls[table][1], sqls[table][0], table)
        mongo_docs = pg.docs_for_mongo(sqls[table][1], sqls[table][0], table)
        try:
            mw.get_database(mdb_db).get_collection(table).insert_many(mongo_docs)
        except TypeError:
            log.logwrite(sys.exc_info())
            exit()
        except pymongo.errors.BulkWriteError:
            for doc in mongo_docs:
                try:
                    mw.get_database(mdb_db).get_collection(table).insert_one(doc)
                except pymongo.errors.DuplicateKeyError:
                    print('DublicateKey')
    mw.close()
    pg.close()


def sql_multi_execution(pg_connection, sqls):
    pg = PostgresqlWorker(pg_connection)
    for sql in sqls:
        pg.execute(sql)
        pg.execute('commit')
        print(sql)
    pg.close()


def users_delete(pg_connection):
    pg = PostgresqlWorker(pg_connection)
    limit = pg.count_limit(sql_count)
    log.logwrite('users limit to delete:', limit)
    user_del_sql = [
        "DELETE FROM tmp_table_for_user_delete2;",

        "INSERT INTO tmp_table_for_user_delete2(userid) SELECT userid "
        "FROM tmp_table_for_user_delete ORDER BY userid LIMIT {0};".format(str(limit)),

        "DELETE FROM users WHERE id IN (SELECT userid FROM tmp_table_for_user_delete2);",

        "DELETE FROM tmp_table_for_user_delete WHERE userid IN (SELECT userid FROM tmp_table_for_user_delete2);"]
    with pg.transaction_cursor() as c:
        for line in user_del_sql:
            c.execute(line)
    if pg.row_numbers(sql_count):
        print(pg.row_numbers(sql_count))
        flag.set_flag(0)


class SuccessFlag:
    def __init__(self):
        self.filename = 'pgflag'

    def set_flag(self, bool_=0):
        f = open(self.filename, 'w')
        f.write(str(bool_))
        f.close()

    def __bool__(self):
        try:
            f = open(self.filename, 'r')
            if f.readlines()[0] is '1':
                return True
            else:
                return False
        except:
            return False


def postgres_connect():
    try:
        return psycopg2.connect(database=pg_database, user=pg_user, password=pg_password, host=pg_host)
    except:
        log.logwrite("Can't connect to postgres")


def pgtest():
    pg1 = PostgresqlWorker(pgc)
    pg1.execute("SELECT * FROM usercash WHERE userid IN (SELECT userid FROM tmp_table_for_user_delete);")
    print(pg1.fetchall())

if __name__ == '__main__':
    arglist = sys.argv[1:]
    flag = SuccessFlag()
    pgc = postgres_connect()
    log = logcreate.Log(logname)
    pgw = PostgresqlWorker(pgc)
    print(pgw.count_limit('SELECT count(*) FROM tmp_table_for_user_delete;'))
    # arglist.append('dbclean')

    if not arglist:
        print("Run script with parameters: daily, backup or dbclean")
        exit()
    elif 'daily' in arglist:
        try:
            log.logwrite('Daily clean started')
            sql_multi_execution(pgc, daily_sql)  # daily clean
            log.logwrite('Daily clean complited') #TODO write time of execution in log
        except:
            log.logwrite('Daily clean failed')

    elif 'backup' in arglist:
        try:
            log.logwrite('Prepare table with users to delete...')
            sql_multi_execution(pgc, tmp_table_prepare_sql)  # fill temp table
            log.logwrite('complite')
            log.logwrite('Backuping...')
            pg_backuper(pgc, sql_for_backup)
            log.logwrite('complite')
            flag.set_flag(1)
        except pymongo.errors.AutoReconnect:
            flag.set_flag(0)
            log.logwrite('ConnectionError')
        except:
            flag.set_flag(0)
            log.logwrite('Backup failed')

    elif 'dbclean' in arglist:
        if flag:
            pgc = postgres_connect()
            log.logwrite('Cleaning database...')
            try:
                users_delete(pgc)
                log.logwrite('Users deleted in')
            except:
                log.logwrite('Database clean failed')
    pgc.close()
