(ns b-clojure-db
  (:require
   [clojure.java.jdbc :as jdbc]
   [jdbc]
   [cheshire.core :as cheshire]
   [dsql.pg :as pg]
   [dsql.core :as ql]
   [clojure.test :as t]))

(defonce db {:dbtype "postgresql"
             :dbname (System/getenv "POSTGRES_DB")
             :host "localhost"
             :port (System/getenv "PGPORT")
             :user (System/getenv "POSTGRES_USER")
             :password (System/getenv "POSTGRES_PASSWORD")})

(defn json [x] (cheshire/generate-string x))

(comment
  db

  (jdbc/query db "select 1")

  (jdbc/query db "select * from information_schema.tables limit 10")

  (jdbc/execute! db "drop table _tmp")

  (jdbc/execute! db "create table if not exists _tmp  (id serial primary key, data jsonb)")

  (jdbc/execute! db "truncate _tmp")

  (for [usr [{:name "Ivan" :age 23}
            {:name "Andrey" :age 41}
            {:name "George" :age 37}]]
    (jdbc/query db ["insert into _tmp (data) values (?::jsonb) returning *"
                    (json usr)]))

  (jdbc/query db "select * from _tmp")

  (->> (jdbc/query db "select * from _tmp")
       (map :data)
       (map :name))

  (jdbc/query db "select data->>'name' from _tmp")
  (jdbc/query db "select jsonb_agg(data) as arr from _tmp")

  ;; coercing - see jdbc
  ;; connection pool


  (jdbc/query db [ "select * from _tmp limit ?" 10])

  ;; it's inconvinient to concat strings to build dynamic queries
  ;; can we represent query as data?
  ;; something like this

  (def q
    {:select :* :from :_tmp}
    )

  (def q2
    (assoc q :limit 10)
    )


  (pg/format q)

  (pg/format q2)

  (defn query [db q]
    (jdbc/query db
     (if (string? q) q (pg/format q))))

  (query db
   {:select :*
    :from :_tmp
    :where {:name [:ilike
                   [:jsonb/->> :data :name]
                   [:pg/param "iv%"]]}})

  (defn search-by-name [db nm]
    (query db
           {:select :* :from :_tmp
            :where {:name [:ilike [:jsonb/->> :data :name]
                           [:pg/param (str nm "%")]]}}))

  (search-by-name db "iv")
  (search-by-name db "ge")

  ;; data composition

  (defn search [db {nm :name limit :limit age :age}]
    (let [q {:select :*
             :from :_tmp
             :where {:name (when nm [:ilike [:jsonb/->> :data :name] [:pg/param (str nm "%")]])
                     :age  (when age [:= [:pg/cast [:jsonb/->> :data :age] :int] age])}
             :limit limit}]
      (query db q)))

  (search db {:name "iv"})
  (search db {:name "iv" :limit 2})
  (search db {:age 37})

  ;; count query

  (defn search [db {nm :name limit :limit age :age}]
    (let [q {:select :*
             :from :_tmp
             :where {:name (when nm [:ilike [:jsonb/->> :data :name] [:pg/param (str nm "%")]])
                     :age  (when age [:= [:pg/cast [:jsonb/->> :data :age] :int] age])}
             :limit limit}]
      {:data (query db q)
       :analyze (->> (query db (assoc q :explain {:analyze true}))
                     (mapv (keyword "query plan")))
       :cnt (-> (query db (assoc q :select [:count*]))
                first
                :count)}))

  (search db {:name "iv" :limit 2})

  ;; extend dsql
  (pg/format [:ilike [:jsonb/->> :data :name] [:pg/param "str%"]])
  (pg/format [:= [:pg/cast [:jsonb/->> :data :age] :int] 45])


  (defmethod ql/to-sql
    :data>>
    [acc opts [_ k]]
    (ql/to-sql acc opts [:jsonb/->> :data k]))

  (defmethod ql/to-sql
    ::int
    [acc opts [_ x]]
    (ql/to-sql acc opts [:pg/cast x :int]))

  (pg/format [:ilike [:data>> :name] [:pg/param "str%"]])

  (pg/format [:= [::int [:data>> :age]] 45])


  ;; model driven

  (pg/format
   {:ql/type :pg/create-table
    :table-name :mytable
    :if-not-exists true
    :columns {:id          {:type "text" :primary-key true}
              :filelds     {:type "jsonb"}
              :match_tags  {:type "text[]"}
              :dedup_tags  {:type "text[]"}}})

  ;; model drive
  (def schemas {:customers
                {:properties {:name {:type "string"}
                              :role {:type "string"}}}
                :orders
                {:properties {:customer {:type "string"}
                              :amount {:type "integer"}}}})

  (def type-map {"string" "text" "integer" "integer"} )

  (defn generate-cols [props]
    (->> props
         (reduce (fn [cols [nm {tp :type}]]
                   (assoc cols nm {:type (get type-map tp)}))
                 {})
         (merge {:id {:type "text" :primary-key true}})))


  (defn generate-tables [schemas]
    (->> schemas
         (mapv (fn [[nm props]]
                 (pg/format
                  {:ql/type :pg/create-table
                   :table-name nm
                   :if-not-exists true
                   :columns (generate-cols (:properties props))})))))

  (for [cmd (generate-tables schemas)]
    (jdbc/execute! db cmd))

  (query db {:select :* :from :orders})


  ;; analyze queries
  ;; idxs from queries

  (def q
    {:select :*
     :from :_tmp
     :where {:name [:ilike [:jsonb/->> :data :name] [:pg/param "str"]]
             :age  [:= [:pg/cast [:jsonb/->> :data :age] :int] 40]}})

  (defn idx [tbl k expr]
    (let [idx-name (str (name tbl) "_" (name k) "_idx")
          idx {:ql/type :pg/index
               :index idx-name
               :if-not-exists true
               :concurrently true
               :on tbl}
          op (when (vector? expr) (first expr))]
      (->
       (cond
         (= op :=) (assoc idx :expr [(second expr)])
         (= op :ilike) (assoc idx
                              :using :GIN
                              :ops :gist_trgm_ops
                              :expr [(second expr)])
         :else (str "Do not know how to build idx for " expr))
       (pg/format))))

  (defn suggest-idxs [q]
    (let [tbl (:from q)]
      (->> (:where q)
           (map (fn [[k expr]] (idx tbl k expr))))))

  (suggest-idxs q)

  )
