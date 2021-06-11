## Get started

Create config file:

``` sh
cp .env.example .env
```

### Recover from corrupted db file:

``` sh
# 检查结构
sqlite3 corrupt.db "pragma integrity_check"
# 恢复数据
sqlite3 corrupt.db ".recover" | sqlite3 new.db
# 重新检查结构
sqlite3 new.db "pragma integrity_check"
```

Ref: [Python如何解决sqlite3.DatabaseError: database disk image is malformed_漫步量化-CSDN博客](https://blog.csdn.net/The_Time_Runner/article/details/106590571)

### Migrate from sqlite to postgres

#### Via dump

Error message:
``` log
ERROR:  current transaction is aborted, commands ignored until end of transaction block
```

``` sh
# Create dump
sqlite3 data/sqlite_data/history.db .dump > tmp.sql

# Feed the dump to psql
psql -h localhost -d orderbook_db -U postgres < tmp.sql

# Remove tmp
rm tmp.sql
```

#### Via .csv file

Issue:
``` log
orderbook_db=# \copy orderbook from './orderbook.csv' delimiter ',' csv header;                                            │(4 rows)
ERROR:  relation "orderbook" does not exist
```

**Solution**: create the tables first.

Ref: [How to migrate from SQLite to PostgreSQL? - Questions - n8n](https://community.n8n.io/t/how-to-migrate-from-sqlite-to-postgresql/2170/5)

``` sh
# Connect to sqlite db
sqlite3 data/sqlite_data/history.db

# Inside sqlite3
.headers on
.mode csv
.output orderbook.csv
SELECT * FROM orderbook;
.output depth.csv
SELECT * FROM depth;
.quit

# Connect to psql
psql -h localhost -d orderbook_db -U postgres

# Inside psql
\copy orderbook from './orderbook.csv' delimiter ',' csv header;
\copy depth from './depth.csv' delimiter ',' csv header;
SELECT setval(pg_get_serial_sequence('orderbook', 'id'), MAX(id)) FROM orderbook;
SELECT setval(pg_get_serial_sequence('depth', 'id'), MAX(id)) FROM depth;

# Remove data files
rm orderbook.csv depth.csv
```
