check:
	sqlite3 db/history.db "select * from orderbook order by timestamp desc limit 10;"
	