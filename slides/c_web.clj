(ns c-web
  (:require
   [org.httpkit.server :as http-kit]
   [clojure.java.jdbc :as jdbc]
   [jdbc]
   [dsql.pg :as pg]
   [ring.util.codec :as codec]
   [clojure.walk]
   [route-map.core :as routes]
   [cheshire.core :as json]))

(defn form-decode [s] (clojure.walk/keywordize-keys (ring.util.codec/form-decode s)))

(defonce db {:dbtype "postgresql"
             :dbname (System/getenv "POSTGRES_DB")
             :host "localhost"
             :port (System/getenv "PGPORT")
             :user (System/getenv "POSTGRES_USER")
             :password (System/getenv "POSTGRES_PASSWORD")})

(defn strip-nulls [m]
  (->> m
       (reduce (fn [m [k v]] (if (nil? v) (dissoc m k) m))
               m)))

(defn query [db q]
  (let [sql (if (string? q) [q] (pg/format q))]
    {:sql sql
     :data (->> (jdbc/query db sql) (mapv strip-nulls))}))

(defn tables [{db :db} req]
  (let [ns (get-in req [:route-params :ns])]
    {:body (query db {:ql/type :pg/select
                      :select :*
                      :from :information_schema.tables
                      :where {:ns (when ns [:= :table_schema [:pg/param ns]])}
                      :limit 100})
     :status 200}))

(defn index [{db :db} req]
  {:body {:message "Welcome"}
   :status 200})

(defn select [{db :db} {params :params rp :route-params}]
  {:body {:data (query db {:ql/type :pg/select
                           :select :*
                           :from (keyword (str (:ns rp) "." (:tbl rp)))
                           :limit 10})
          :params params}
   :status 200})

(def big-tables-q
  (let [tot ^:pg/fn[:pg_total_relation_size :relid]
        sz  ^:pg/fn[:pg_relation_size :relid]]
    {:ql/type :pg/select
     :select {:table_schema :schemaname
              :table_name :relname
              :total_size ^:pg/fn[:pg_size_pretty tot]
              :data_size  ^:pg/fn[:pg_size_pretty sz]
              :external_size ^:pg/op[:- tot sz]}
     :from :pg_catalog.pg_statio_user_tables
     :order-by [:pg/list [:pg/desc tot] [:pg/desc sz]]
     :limit 10}))

(pg/format big-tables-q)


(defn big-tables [{db :db} _]
  {:body (query db big-tables-q)
   :status 200})

(def routes
  {:GET #'index
   "admin" {"big-tables" {:GET #'big-tables}}
   "tables" {:GET #'tables
             [:ns] {:GET #'tables
                    [:tbl] {:GET #'select}}}})

(comment
  (routes/match [:get "/tables"] routes)
  (routes/match [:get "/admin/big-tables"] routes)
  (routes/match [:get "/ups"] routes)

  (-> (routes/match [:get "/tables/public/person"] routes)
      (dissoc :parents))

  )

(defn dispatch [ctx {meth :request-method uri :uri :as req}]
  (if-let [{ctrl :match prms :params} (routes/match [(or meth :get) uri] routes)]
    (ctrl ctx (assoc req :route-params prms))
    {:status 404
     :body {:message (str (or (:request-method req) :get) (:uri req) " not found")}}))

(defn handler [ctx {qs :query-string :as req}]
  (let [req  (cond-> req
               qs (assoc :params (form-decode qs)))
        resp (dispatch ctx req)]
    (if (:body resp)
      (-> (update resp :body #(json/generate-string % {:pretty true}))
          (assoc-in [:headers "content-type"] "application/json"))
      resp)))


(defn start [ctx]
  (http-kit/run-server (fn [req] (handler ctx req)) {:port 8081}))

(defn stop [srv] (srv))

(comment
  (def ctx {:db db})

  (def srv (start ctx))

  (srv)

  (dispatch ctx {:uri "/admin/big-tables"})

  (dispatch ctx {:uri "/tables"})
  (dispatch ctx {:uri "/tables/public/person"})
  (dispatch ctx {:uri "/tables/public/person" :params {:id 10}})

  (for [usr [{:name "Ivan" :age 23}
             {:name "Andrey" :age 41}
             {:name "George" :age 37}]]
    (jdbc/query db ["insert into person (data) values (?::jsonb) returning *"
                    (cheshire.core/generate-string usr)]))


  )

