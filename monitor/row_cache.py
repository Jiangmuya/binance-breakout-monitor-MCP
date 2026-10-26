"""Disk-only prepared rows with stable append order and revision validation."""
import hashlib
import json
import sqlite3
from pathlib import Path


class RowCache:
    def __init__(self,path):
        Path(path).parent.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(path,timeout=30)
        self.db.row_factory=sqlite3.Row
        self.db.executescript('''PRAGMA journal_mode=WAL; PRAGMA cache_size=-4096;
            CREATE TABLE IF NOT EXISTS prepared_rows(
                ordinal INTEGER PRIMARY KEY AUTOINCREMENT,kind TEXT,key TEXT,revision TEXT,
                interval TEXT,start INTEGER,content_hash TEXT,updated_ms INTEGER,format_version INTEGER,
                value TEXT,comparisons TEXT,comparison_count INTEGER,generation TEXT,
                UNIQUE(kind,key));
            CREATE INDEX IF NOT EXISTS prepared_period ON prepared_rows(kind,interval,start,ordinal);
            CREATE TABLE IF NOT EXISTS successful_formats(
                interval TEXT,start INTEGER,revision INTEGER,row_count INTEGER,format_version INTEGER,
                exported_ms INTEGER,PRIMARY KEY(interval,start));''')
        self.hits=0;self.misses=0

    def get(self,kind,key,revision,format_version):
        row=self.db.execute('SELECT * FROM prepared_rows WHERE kind=? AND key=? AND revision=? AND format_version=?',
            (kind,key,str(revision),format_version)).fetchone()
        if row:
            self.hits+=1
            return row
        self.misses+=1
        return None

    def put(self,kind,key,revision,interval,start,value,comparisons,updated,format_version,generation):
        encoded=json.dumps(value,ensure_ascii=False,separators=(',',':'),allow_nan=False)
        previous=json.dumps(comparisons,ensure_ascii=False,separators=(',',':'),allow_nan=False)
        digest=hashlib.sha256((encoded+'\n'+previous).encode('utf-8')).hexdigest()
        self.db.execute('''INSERT INTO prepared_rows(kind,key,revision,interval,start,content_hash,
            updated_ms,format_version,value,comparisons,comparison_count,generation) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(kind,key) DO UPDATE SET revision=excluded.revision,interval=excluded.interval,
            start=excluded.start,content_hash=excluded.content_hash,updated_ms=excluded.updated_ms,
            format_version=excluded.format_version,value=excluded.value,comparisons=excluded.comparisons,
            comparison_count=excluded.comparison_count,generation=excluded.generation''',
            (kind,key,str(revision),interval,start,digest,updated,format_version,encoded,previous,len(comparisons),generation))
        return digest

    def touch(self,kind,key,generation):
        self.db.execute('UPDATE prepared_rows SET generation=? WHERE kind=? AND key=?',(generation,kind,key))

    def period_rows(self,interval,start,generation):
        cursor=self.db.execute('''SELECT * FROM prepared_rows WHERE kind='signal' AND interval=? AND start=?
            AND generation=? ORDER BY ordinal''',(interval,start,generation))
        while True:
            rows=cursor.fetchmany(200)
            if not rows:break
            yield from rows

    def successful(self,interval,start,revision,count,format_version,stamp):
        self.db.execute('INSERT OR REPLACE INTO successful_formats VALUES(?,?,?,?,?,?)',
            (interval,start,revision,count,format_version,stamp))
        self.db.commit()

    def close(self):
        self.db.commit();self.db.close()
