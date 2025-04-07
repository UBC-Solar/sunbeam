.PHONY:clean
clean:
	rm -rf db/pg_data/*
	rm -rf db/mongo_data/*
	rm -rf logs/*.log
	rm -rf fs_data/*
	rm -rf db/redis-data/*
	docker images --filter=reference='sunbeam*' -q | xargs -r docker rmi -f

.PHONY:build
build:
	mkdir -p db/mongo_data/
	mkdir -p db/pg_data/
	mkdir -p db/redis-data
	touch pipeline/.env
	docker compose build
