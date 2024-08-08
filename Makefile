.PHONY: up repl test down

up:
	docker-compose up -d
	source .env && clj -A:dev:storm -m nrepl.cmdline --middleware '[cider.nrepl/cider-middleware]'

repl:
	source .env && clj -M:dev -m nrepl.cmdline --middleware '[cider.nrepl/cider-middleware]'

test:
	clojure -M:dev:kaocha

down:
	docker-compose down
