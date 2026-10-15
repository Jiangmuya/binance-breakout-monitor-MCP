CREATE INDEX signal_period ON signals(interval,t);

CREATE TABLE anomalies(id INTEGER PRIMARY KEY AUTOINCREMENT,time INTEGER,symbol TEXT,interval TEXT,code TEXT,detail TEXT);

CREATE TABLE bars(symbol TEXT,interval TEXT,t INTEGER,o REAL,h REAL,l REAL,c REAL,v REAL,close_time INTEGER,q REAL,
 PRIMARY KEY(symbol,interval,t)) WITHOUT ROWID;

CREATE TABLE deliveries(id TEXT,adapter TEXT,sent INTEGER,PRIMARY KEY(id,adapter));

CREATE TABLE evaluations(version TEXT,symbol TEXT,interval TEXT,t INTEGER,payload TEXT NOT NULL,
 PRIMARY KEY(version,symbol,interval,t)) WITHOUT ROWID;

CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);

CREATE TABLE outbox(id TEXT PRIMARY KEY REFERENCES signals(id),payload TEXT NOT NULL,status TEXT NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0,next_attempt INTEGER NOT NULL,sent INTEGER,error TEXT);

CREATE TABLE schema_version(version INTEGER PRIMARY KEY);

CREATE TABLE signals(id TEXT PRIMARY KEY,version TEXT,symbol TEXT,interval TEXT,t INTEGER,event_type TEXT,
 payload TEXT NOT NULL,UNIQUE(version,symbol,interval,t));

CREATE TABLE sqlite_sequence(name,seq);

CREATE TABLE states(symbol TEXT,interval TEXT,state TEXT NOT NULL,PRIMARY KEY(symbol,interval));

CREATE TABLE universe(symbol TEXT PRIMARY KEY,active INTEGER NOT NULL,info TEXT NOT NULL,reason TEXT,updated INTEGER);

CREATE TABLE versions(version TEXT PRIMARY KEY,config TEXT NOT NULL,created INTEGER);
