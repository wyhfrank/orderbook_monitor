build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs runner

shell:
	docker-compose run runner /bin/bash

check_db:
	sqlite3 db/history.db "select * from orderbook order by timestamp desc limit 10;"

price_diff:
	docker-compose run runner python check_price_diff.py

recover_db:
	cd db && mv history.db history.bk.db && \
	sqlite3 history.bk.db ".recover" | sqlite3 history.db && \
	sqlite3 history.db "pragma integrity_check"
