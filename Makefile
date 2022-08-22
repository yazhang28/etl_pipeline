.PHONY: init help test reqs

objects = $(wildcard *.in)
outputs := $(objects:.in=.txt)

help:
#	@echo "Hello World!"
	@echo "'make init' to set up environment dependencies and generate data"
	@echo ""
#	@echo "EVERYTHING BELOW IS NOT IMPLEMENTED:"
# @echo "'make config' to change defaults"
# @echo "'make run' to run local webserver version"
# @echo "'make build' to, um, build"
	@echo "'make test' to run tests"
	@echo "'make run' to run against data"
	@echo "'make reset' to reset database"
	@echo "'make finish' to cleanup"

apt:
	sudo apt install awscli python3-pytest sqlite3
	echo "Remove me to rerun apt" > apt

init:
	docker-compose build
	docker-compose up -d

run:
# . put your code here
	docker exec manifold-backend-homework_process_1 python process/process.py

test:
# . put your code here
	pytest	tests/test_process.py

reset:
# . reset the db with cleaned tables
	docker exec -i manifold-backend-homework_db_1 sqlite3 manifold.db < db/reset.sql

finish:
	docker-compose down
