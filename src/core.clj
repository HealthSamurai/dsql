(ns dsql.core
  (:require [clojure.string :as str]))

(defn reduce-separated [sep acc f coll]
  (if (empty? coll)
    acc
    (loop [[x & xs] coll
           acc acc]
      (let [acc' (f acc x)]
        (if (empty? xs)
          acc'
          (recur xs (conj acc' sep)))))))

(defn reduce-acc [acc f coll]
  (reduce f acc coll))

(defn get-type [x]
  (or
   (and (map? x) (get x :ql/type))
   (when-let [m (meta x)] (:ql/type m))))

(defn dispatch-sql [opts x]
  (or
   (get-type x)
   (when-let [resolver (:resolve-type opts)]
     (resolver x))
   (and (sequential? x) (first x))
   (type x)))

(defn escape-string [s]
  (str/replace s #"'" "''"))

(defn string-litteral [s]
  (str "'" (escape-string s) "'"))

(defn default-type [node tp]
  (if (and (not (get-type node)) (map? node))
    (assoc node :ql/type tp)
    node))


(defmulti to-sql (fn [_ opts node] (dispatch-sql opts node)))

(defn format [opts node]
  (let [sql-vec (to-sql [] opts node)]
    (loop [[x & xs] sql-vec
           sql-str []
           params []]
      (let [[sql-str params] (if (vector? x)
                               [(conj sql-str (first x)) (conj params (second x))]
                               [(conj sql-str x) params])]
        (if (empty? xs)
          (into [(str/join " " sql-str)] params)
          (recur xs sql-str params))))))

(defmethod to-sql
  clojure.lang.PersistentArrayMap
  [acc opts node]
  (throw (Exception. (pr-str node))))

(defmethod to-sql
  java.lang.Long
  [acc opts node]
  (conj acc (str node)))

(defmethod to-sql
  clojure.lang.Keyword
  [acc opts node]
  (conj acc (name node)))

(defmethod to-sql
  java.lang.String
  [acc opts node]
  (conj acc (string-litteral node)))


