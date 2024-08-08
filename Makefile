.PHONY: up repl test down

ifeq (repl,up))
  include ./libs/sandata-configuration-project/.env
  export
endif

up:
	docker-compose up -d
	clj -A:dev -m nrepl.cmdline --middleware '[cider.nrepl/cider-middleware]'

repl:
	clj -M:dev -m nrepl.cmdline --middleware '[cider.nrepl/cider-middleware]'

test:
	clojure -M:dev:kaocha

down:
	docker-compose down
