.PHONY:clean
clean:
	rm -rf db/pg_data/*
	rm -rf db/mongo_data/*

.PHONY:build
build:
	mkdir -p db/mongo_data/
	mkdir -p db/pg_data/
	docker compose build
