import sqlite3
from datetime import datetime

HEADERS = {
    'entry_table': [
        'ar_user_id',
        'ap_user_id',
        'entry_datetime',
        'record_ts'
    ],
    'user_table': [
        'user_id',
        'name',
        'email',
        'record_ts'
    ]
}

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

class YohmiDB(object):
    def __init__(self, dbfile='yohmi.db'):
        self.dbfile = dbfile
        self.init_db()

    def init_db(self):
        self._make_entry_table()
        self._make_user_table()

    def _make_user_table(self):
        query = '''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(255),
            email VARCHAR(255),
            record_ts DATETIME,
            is_alias BOOLEAN
        )
        '''
        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()
        cur.execute(query)

    def _make_entry_table(self):
        query = '''
        CREATE TABLE IF NOT EXISTS entries (
            record_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ar_user_id INT,
            ap_user_id INT,
            amount DECIMAL(15,2),
            entry_datetime DATETIME,
            record_ts DATETIME
        )
        '''
        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()
        cur.execute(query)

    def add_entry(self, ar_user_id, ap_user_id, amount, datetime):
        query = '''
        INSERT INTO entries
        (ar_user_id, ap_user_id, amount, entry_datetime, record_ts)
        VALUES 
        (?, ?, ?, ?, ?)
        '''
        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()
        record_ts = self.get_current_time()
        cur.execute(query, [ar_user_id, ap_user_id, amount, datetime, record_ts])
        con.commit()
        con.close()

    def add_user(self, name, email, is_alias=False):
        query = '''
        INSERT INTO users
        (name, email, record_ts, is_alias)
        VALUES 
        (?, ?, ?, ?)
        '''
        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()
        record_ts = self.get_current_time()
        cur.execute(query, [name, email,record_ts, is_alias])
        con.commit()
        con.close()
    
    @staticmethod
    def get_current_time():
        return datetime.now().strftime(DATETIME_FORMAT)

    def get_user_entries(self, user_id):
        query = "SELECT * FROM entries WHERE ar_user_id=? OR ap_user_id=?"
        return self._execute_and_read(query, user_id, user_id)
    
    def get_interuser_entries(self, ar_user_id, ap_user_id):
        query = '''
        SELECT * FROM entries 
        WHERE (ar_user_id=? AND ap_user_id=?)
        OR (ar_user_id=? AND ap_user_id=?)
        '''
        return self._execute_and_read(query, ar_user_id, ap_user_id, ap_user_id, ar_user_id)       

    def _execute_and_read(self, query, *args):
        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()
        cur.execute(query, args)
        rows = cur.fetchall()
        con.close()
        return rows
    
    def _execute(self, *args):
        con = sqlite3.connect(self.dbfile)
        cur = con.cursor()
        cur.execute(query, args)
        con.commit()
        con.close()
        return