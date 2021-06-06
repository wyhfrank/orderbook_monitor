check:
	sqlite3 db/history.db "select * from orderbook;"
	