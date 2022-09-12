.PHONY: test

up:
	docker-compose up
	source .env && clj -A:dev -m nrepl.cmdline --middleware '[cider.nrepl/cider-middleware]'

repl:
	source .env && clj -M:dev -m nrepl.cmdline --middleware '[cider.nrepl/cider-middleware]'

test:
	clojure -M:dev:kaocha
