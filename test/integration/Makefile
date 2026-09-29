.PHONY: up down test clean

up:
	bash scripts/mysql-up.sh

down:
	bash scripts/mysql-down.sh

test:
	bash scripts/run-e2e.sh

clean: down
	rm -rf workdir/*
