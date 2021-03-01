(ns d-postgrest
  (:require
   [org.httpkit.server :as http-kit]
   [clojure.java.jdbc :as jdbc]
   [dsql.jdbc]
   [dsql.pg :as pg]
   [route-map.core :as routes]
   [cheshire.core :as json]))

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
    (->> (jdbc/query db sql)
         (mapv strip-nulls))))


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

(defn select [{db :db} {rp :route-params}]
  (let [res (query db {:ql/type :pg/select
                       :select :*
                       :from (keyword (str (:ns rp) "." (:tbl rp)))
                       :limit 100})]
    {:body res
     :status 200}))

(def big-tables-q
  {:ql/type :pg/select
   :select {:table_schema :schemaname
            :table_name :relname
            :total_size [:pg/sql "pg_size_pretty(pg_total_relation_size(relid))"]
            :data_size  [:pg/sql "pg_size_pretty(pg_relation_size(relid))"]
            :external_size [:pg/sql "pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid))"]}
   :from :pg_catalog.pg_statio_user_tables
   :order-by [:pg/sql "pg_total_relation_size(relid) desc, pg_relation_size(relid) desc"]
   :limit 10})


(defn big-tables [{db :db} _]
  {:body (query db big-tables-q)
   :status 200})

(def routes
  {:GET #'index
   "admin" {"big-tables" {:GET #'big-tables}}
   "tables" {:GET #'tables
             [:ns] {:GET #'tables
                    [:tbl] {:GET #'select}}}})

(defn dispatch [ctx req]
  (if-let [{ctrl :match prms :params} (routes/match [(or (:request-method req) :get) (:uri req)] routes)]
    (ctrl ctx (assoc req :route-params prms))
    {:status 404
     :body {:message (str (or (:request-method req) :get) (:uri req) " not found")}}))

(defn handler [ctx req]
  (let [resp (dispatch ctx req)]
    (if (:body resp)
      (-> (update resp :body #(json/generate-string % {:pretty true}))
          (assoc-in [:headers "content-type"] "application/json"))
      resp)))


(defn start [ctx]
  (http-kit/run-server
   (fn [req] (handler ctx req)) {:port 8081}))

(defn stop [srv] (srv))


(comment
  (def ctx {:db db})

  (def srv (start ctx))

  (srv)

  (dispatch ctx {:uri "/tables"})


  )

