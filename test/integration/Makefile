.PHONY: up down test clean es-up es-down es-integration es-e2e es-load es-scale-normal es-scale-heavy

up:
	bash scripts/mysql-up.sh

down:
	bash scripts/mysql-down.sh

test:
	bash scripts/run-e2e.sh

clean: down
	rm -rf workdir/*

# Elasticsearch 8 suites: `make up es-up` first (MySQL + compose ES8), then one
# of the targets below. Reports: docs/test/reports/mysql-elasticsearch*.md
es-up:
	bash scripts/es-up.sh

es-down:
	bash scripts/es-down.sh

# Go-level sink/runner IT against the real cluster (build tag esintegration).
es-integration:
	bash scripts/run-es-integration.sh

es-e2e:
	bash scripts/run-es-e2e.sh

es-load:
	bash scripts/run-es-load.sh

es-scale-normal:
	bash scripts/run-es-scale.sh normal

# 10 source DBs / 50k rows per hotspot / 45s peak with 8 writers. Opt-in only.
es-scale-heavy:
	bash scripts/run-es-scale.sh heavy
