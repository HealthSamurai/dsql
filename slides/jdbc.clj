(ns jdbc
  (:require [cheshire.core :refer [generate-string parse-string] :as cheshire]
            [cheshire.generate :as json-gen]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str])
  (:import clojure.lang.IPersistentMap
           clojure.lang.IPersistentVector
           [java.sql Date Timestamp PreparedStatement]
           [org.postgresql.jdbc PgArray]
           org.postgresql.util.PGobject))


(json-gen/add-encoder org.httpkit.server.AsyncChannel json-gen/encode-str)
(json-gen/add-encoder clojure.lang.Var json-gen/encode-str)
#_(json-gen/add-encoder com.zaxxer.hikari.HikariDataSource json-gen/encode-str)
(json-gen/add-encoder org.postgresql.util.PGobject json-gen/encode-str)
(json-gen/add-encoder java.lang.Object json-gen/encode-str)

(def time-fmt
  (->
   (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss")
   (.withZone (java.time.ZoneOffset/UTC))))


(defn- to-date [sql-time]
  (str (.format time-fmt (.toInstant sql-time)) "." (format "%06d"  (/ (.getNanos sql-time) 1000)) "Z"))



(extend-type java.util.Date
  jdbc/ISQLParameter
  (set-parameter [v ^PreparedStatement stmt idx]
    (.setTimestamp stmt idx (java.sql.Timestamp. (.getTime v)))))

(defn parse-int-range [s]
  (let [pair (-> (str/replace s #"\[|\]|\(|\)" "")
                 (str/split #","))]
    (mapv read-string pair)))


(extend-protocol jdbc/IResultSetReadColumn
  Date
  (result-set-read-column [v _ _] (.toString v))

  Timestamp
  (result-set-read-column [v _ _]
    (.toString (.toInstant v)))

  PgArray
  (result-set-read-column [v _ _] (vec (.getArray v)))

  PGobject
  (result-set-read-column [pgobj _metadata _index]
    (let [type  (.getType pgobj)
          value (.getValue pgobj)]
      (case type
        "json"      (parse-string value true)
        "jsonb"     (parse-string value true)
        "int8range" (parse-int-range value)
        "citext" (str value)
        value))))

(extend-type java.util.Date
  jdbc/ISQLParameter
  (set-parameter [v ^PreparedStatement stmt idx]
    (.setTimestamp stmt idx (Timestamp. (.getTime v)))))

(defn to-pg-json [value]
  (doto (PGobject.)
    (.setType "jsonb")
    (.setValue (generate-string value))))

(defn to-pg-array
  ([conn value & [sql-type]]
   (.createArrayOf conn (or sql-type "text") (into-array value)))
  ([value]
   (println "Create array without connection")
   (str "{" (clojure.string/join "," (map #(str "\"" % "\"") value)) "}")))


(extend-protocol jdbc/ISQLValue
  clojure.lang.Keyword
  (sql-value [value] (name value))
  java.util.Date
  (sql-value [value] (java.sql.Timestamp. (.getTime value)))
  IPersistentMap
  (sql-value [value] (to-pg-json value))
  IPersistentVector
  (sql-value [value] (to-pg-array value)))
