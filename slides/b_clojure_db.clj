(ns b-clojure-db
  (:require
   [clojure.java.jdbc :as jdbc]
   [jdbc]
   [cheshire.core :as cheshire]
   [dsql.pg :as pg]
   [dsql.core :as ql]
   [dsql.pg-test]))

(declare start)

"clojure is hosted - so we can use jdbc and other java connectors to postgres"

(defonce db {:dbtype "postgresql"
             :dbname (System/getenv "POSTGRES_DB")
             :host "localhost"
             :port (System/getenv "PGPORT")
             :user (System/getenv "POSTGRES_USER")
             :password (System/getenv "POSTGRES_PASSWORD")})

"see what's insinde connection params"
db

"Lets run a simple query"
(jdbc/query db "select 1")



"Get all tables"
(jdbc/query db "select * from information_schema.tables limit 10")


(comment (jdbc/execute! db "drop table if exists person"))

"create table"
(jdbc/execute! db "create table if not exists person  (id serial primary key, data jsonb)")

(jdbc/execute! db "truncate person")

(defn json [x] (cheshire/generate-string x))

(for [usr [{:name "Ivan" :age 23}
           {:name "Andrey" :age 41}
           {:name "George" :age 37}]]
  (jdbc/query db ["insert into person (data) values (?::jsonb) returning *"
                  (json usr)]))

(jdbc/query db "select * from person")

(->> (jdbc/query db "select * from person")
     (map :data)
     (map :name))

(jdbc/query db "select data->>'name' as nm from person")

(jdbc/query db "select jsonb_agg(data) as arr from person")


"parameters"
(jdbc/query db [ "select * from person limit ?" 10])

"
concat strings are not always good.
In clojure we are trying to convert everything to data (maps/vectors)
"

(def q {:select :* :from :person})
(def q2 (assoc q :limit 10))

(pg/format q)
(pg/format (assoc q :limit 10))
(pg/format (assoc q :limit 10 :offset 20))

(defn query [db q]
  (jdbc/query db (cond (string? q) q
                       (and (vector? q) (string? q (first q))) q
                       :else (pg/format q))))

"build query"
(query db
       {:select :*
        :from :person
        :where {:name [:ilike
                       [:jsonb/->> :data :name]
                       [:pg/param "iv%"]]}})

(defn search-by-name [db nm]
  (query db
         {:select :* :from :person
          :where {:name [:ilike [:jsonb/->> :data :name]
                         [:pg/param (str nm "%")]]}}))

(search-by-name db "iv")
(search-by-name db "ge")


"construction"
(defn search [db {nm :name limit :limit age :age}]
  (let [q {:select :*
           :from :person
           :where {:name (when nm [:ilike [:jsonb/->> :data :name] [:pg/param (str nm "%")]])
                   :age  (when age [:= [:pg/cast [:jsonb/->> :data :age] :int] age])}
           :limit limit}]
    {:query q
     :result (query db q)}))

(search db {:name "iv"})
(search db {:name "iv" :limit 2})
(search db {:age 37})
(search db {:name "iv" :age 23 :limit 2})

"easy to modify query in different places"
(defn search* [db {nm :name limit :limit age :age}]
  (let [q {:select :*
           :from :person
           :where {:name (when nm [:ilike [:jsonb/->> :data :name] [:pg/param (str nm "%")]])
                   :age  (when age [:= [:pg/cast [:jsonb/->> :data :age] :int] age])}
           :limit limit}]
    {:data (query db q)
     :analyze (->> (assoc q :explain {:analyze true})
                   (query db )
                   (mapv (keyword "query plan")))
     :cnt (-> (query db (assoc q :select [:count*]))
              first
              :count)}))

(search* db {:name "iv" :limit 2})

"extend dsql library: macro"

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

(defmethod ql/to-sql
  :ilike>>
  [acc opts [_ k p]]
  (ql/to-sql acc opts [:ilike [:jsonb/->> :data k]
                       [:pg/param (str p "%")]]))

(pg/format [:ilike>> "iv"])

(query db
 {:select :*
  :from :person
  :where {:name [:ilike>> "iv"]}})


"Model driven: generation"

(pg/format
 {:ql/type :pg/create-table
  :table-name :mytable
  :if-not-exists true
  :columns {:id          {:type "text" :primary-key true}
            :filelds     {:type "jsonb"}
            :match_tags  {:type "text[]"}
            :dedup_tags  {:type "text[]"}}})

"json schemas"
(def schemas
  {:customers {:properties {:name {:type "string"}
                            :role {:type "string"}}}
   :orders {:properties {:customer {:type "string"}
                         :amount {:type "integer"}}}})

(def type-map {"string" "text" "integer" "integer"} )

(defn generate-cols [props]
  (->> props
       (reduce (fn [cols [nm {tp :type}]]
                 (assoc cols nm {:type (get type-map tp)}))
               {})
       (merge {:id {:type "text" :primary-key true}})))

(generate-cols {:name {:type "string"}
                :role {:type "string"}})


"now generate all tables"
(defn generate-tables [schemas]
  (->> schemas
       (mapv (fn [[nm props]]
               (pg/format
                {:ql/type :pg/create-table
                 :table-name nm
                 :if-not-exists true
                 :columns (generate-cols (:properties props))})))))

" execute it"
(for [cmd (generate-tables schemas)]
  (jdbc/execute! db cmd))

(query db {:select :* :from :orders})


"
We can analyze queries as data:
 suggest indecies from query

"


(def q
  {:select :*
   :from :person
   :where {:name [:ilike [:jsonb/->> :data :name] [:pg/param "str"]]
           :age  [:= [:pg/cast [:jsonb/->> :data :age] :int] 40]}})

(pg/format
 {:ql/type :pg/index
  :index :myidx
  :if-not-exists true
  :concurrently true
  :on :tbl
  :using :GIN
  :ops :trigramm
  :expr [:a :b]})

(defn suggest-idx [tbl k expr]
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

(defn suggest-idxs [{tbl :from where :where}]
  (->> where
       (map (fn [[k expr]] (suggest-idx tbl k expr)))))

(suggest-idxs q)

"
Real world:

 SQL first class for our systems

"

(defn archive-covs [merge-id type pid exclude-cid]
  {:ql/type :pg/cte
   :with {:_archived {:ql/type :pg/update
                      :update :coverages
                      :set {:resource [:pg/jsonb_set :resource [:archived] [:pg/to_jsonb true]]}
                      :where {:pid [:= [:resource#>> [:patient :id]] pid]
                              :tp [:= [:resource->> :type] (name type)]
                              :excl [:<> :id exclude-cid]}
                      :returning :id}
          :_archive {:ql/type :pg/insert-select
                     :into :patient_merge_log
                     :select {:select {:merge_id [:pg/param merge-id]
                                       :type "archive-coverages"
                                       :resource [:pg/row_to_jsonb :_archived.*]}
                              :from :_archived}}}
   :select {:select {:count [:count*]} :from :_archived}})

(pg/format (archive-covs "mid" "type" "pid" "exid"))

"Something like this:"
(defn assoc-in-sql
  [{id :id resource-type :resourceType} path value]
  (let [table (keyword resource-type)
        [path-before [pattern & path-after]] (split-with (complement map?) path)]
    {:ql/type :pg/update
     :update {:t table}
     :from {:p table :o table}
     :where {:t.id [:= :t.id id]
             :p.id [:= :p.id :t.id]
             :o.id [:= :o.id :t.id]}
     :returning {:ql/type :pg/projection
                 :resource [:|| :t.resource ^:pg/obj{:id :t.id :resourceType :t.resource_type}]
                 :prev-value [:jsonb/#>* :o.resource
                              [:|| [:array_append [:pg/array path-before]
                                    [:pg/cast [:pg/coalesce :i.idx [:jsonb_array_length [:jsonb/#> :t.resource path-before]] 0] ::text]]
                               [:pg/array path-after]]]}
     :set {:resource [:jsonb_deep_set :t.resource
                      [:array_append [:pg/array path-before]
                       [:pg/cast [:pg/coalesce :i.idx [:jsonb_array_length [:jsonb/#> :t.resource path-before]] 0] ::text]]
                      (if (seq path-after)
                        [:jsonb_deep_set [:pg/coalesce :i.val [:pg/param pattern]] [:pg/array path-after] [:pg/param (json value)]]
                        [:|| [:pg/param pattern] [:pg/param (json value)]])]}
     :left-join-lateral {:i {:on true
                             :sql {:ql/type :pg/select
                                   :from {"t(val, idx)" [:pg/with-ordinality [:jsonb_array_elements [:jsonb/#> :p.resource path-before]]]}
                                   :where [:pg/include-op :val [:pg/param pattern]]
                                   :select {:val :val
                                            :idx ^:pg/op [:- :idx 1]}}}}}))

(comment 
  "Go to "
  dsql.pg-test/format=
  )
