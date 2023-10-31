(ns dsql.core
  (:require [clojure.string :as str])
  (:refer-clojure :exclude [format]))

(defn reduce-separated [sep acc f coll]
  (if (empty? coll)
    acc
    (loop [[x & xs] coll
           acc acc]
      (let [acc' (f acc x)]
        (if (empty? xs)
          acc'
          (recur xs (conj acc' sep)))))))

(defn reduce-separated2 [acc sep f coll]
  (reduce-separated sep acc f coll))


(defn reduce-acc [acc f coll]
  (reduce f acc coll))

(defn get-type [x]
  (or
   (and (map? x) (get x :ql/type))
   (when-let [m (meta x)] (:ql/type m))))

(defn default-meta-type [node tp]
  (if (or (get-type node)
          (not (instance? clojure.lang.IMeta node)))
    node
    (with-meta node
      (merge {:ql/type tp}
             (meta node)))))

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
  (str "'" (escape-string (if (keyword? s) (name s) s)) "'"))

(defn default-type [node tp]
  (if (and (not (get-type node)) (map? node))
    (assoc node :ql/type tp)
    node))


(defmulti to-sql (fn [_ opts node] (dispatch-sql opts node)))


(defmethod to-sql
  clojure.lang.PersistentArrayMap
  [_acc _opts node]
  (throw (Exception. (pr-str node))))

(defmethod to-sql
  java.lang.Long
  [acc _opts node]
  (conj acc (str node)))

(defmethod to-sql
  java.lang.Integer
  [acc _opts node]
  (conj acc (str node)))

(defmethod to-sql
  java.lang.Double
  [acc _opts node]
  (conj acc (str node)))

(defn alpha-num? [s]
  (some? (re-matches #"^[a-zA-Z][a-zA-Z0-9]*$" s)))

(defn safe-identifier? [s]
  (some? (re-matches #"^[_a-zA-Z][_.a-zA-Z0-9]*$" s)))


(defn escape-ident [keywords node]
  (let [norm-name (str/upper-case (name node))]
    (if  (= \" (first (name node)))
      (name node)
      (if (or (not (safe-identifier? norm-name))
              (contains? keywords (keyword norm-name)))
        (str "\"" (name node) "\"")
        (name node)))))

(defn parens [acc body-cb]
  (-> (conj acc "(")
      (body-cb)
      (conj  ")")))

(defn escape-ident-alt [{keywords :keywords} node]
  (let [norm-name (str/upper-case (name node))]
    (if (or (not (safe-identifier? norm-name))
            (contains? keywords (keyword norm-name)))
      (str "\"" (name node) "\"")
      (name node))))

(defmethod to-sql
  clojure.lang.Keyword
  [acc _opts node]
  (conj acc (name node)))

(defmethod to-sql
  java.lang.Boolean
  [acc _opts node]
  (conj acc (if node "true" "false")))

(defmethod to-sql
  java.lang.String
  [acc _opts node]
  (conj acc (string-litteral node)))

(defmethod to-sql
  nil
  [acc _ _]
  (conj acc "NULL"))

(defn format [opts node]
  (let [sql-vec (to-sql [] opts node)]
    (assert (sequential? sql-vec) (pr-str sql-vec))
    (loop [[x & xs] sql-vec
           sql-str []
           params []]
      (let [[sql-str params] (if (vector? x)
                               [(conj sql-str (first x)) (conj params (second x))]
                               [(conj sql-str x) params])]
        (if (empty? xs)
          (into [(str/join " " sql-str)] params)
          (recur xs sql-str params))))))
