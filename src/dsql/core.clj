(ns dsql.core
  (:require [clojure.string :as str])
  (:import  [java.util Iterator])
  (:refer-clojure :exclude [format]))

(defn fast-join [^String sep ^Iterable coll]
  (let [^Iterator iter (.iterator coll)
        builder (StringBuilder.)]
    (loop []
      (when (.hasNext iter)
        (let [s (.next iter)]
          (.append builder (.toString s))
          (when (.hasNext iter)
            (.append builder sep)))
        (recur)))
    (.toString builder)))

(defmacro build-string [& strs]
  (let [w (gensym)]
    `(let [~w (StringBuilder.)]
       ~@(map (fn [arg] `(.append ~w ~arg)) strs)
       (.toString ~w))))

(defn reduce-separated-old [sep acc f  coll]
  (if (empty? coll)
    acc
    (loop [[x & xs] coll
           acc acc]
      (let [acc' (f acc x)]
        (if (empty? xs)
          acc'
          (recur xs (conj acc' sep)))))))

(defn reduce-separated [sep acc f ^Iterable coll]
  (if coll
    (let [^Iterator iter (.iterator coll)]
      (loop [acc acc]
        (if (.hasNext iter)
          (let [x (.next iter)
                acc' (f acc x)]
            (if (.hasNext iter)
              (recur (conj acc' sep))
              acc'))
          acc)))
    acc))

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

(defn escape-string [^String s]
  (when s
    (.replace s "'" "''")))


(defn string-litteral [s]
  (build-string "'" (escape-string (if (keyword? s) (name s) s)) "'"))

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
        (build-string "\"" (name node) "\"")
        (name node)))))

(defn parens [acc body-cb]
  (-> (conj acc "(")
      (body-cb)
      (conj  ")")))

(defn escape-ident-alt [{keywords :keywords} node]
  (let [norm-name (str/upper-case (name node))]
    (if (or (not (safe-identifier? norm-name))
            (contains? keywords (keyword norm-name)))
      (build-string "\"" (name node) "\"")
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
  (conj acc ["?" node]))

(defmethod to-sql
  nil
  [acc _ _]
  (conj acc "NULL"))


(defn format [opts node]
  (let [^Iterable sql-vec (to-sql [] opts node)
        ^Iterator iter (.iterator sql-vec)
        builder (StringBuilder.)]
    (assert (sequential? sql-vec) (pr-str sql-vec))
    (loop [params  []]
      (when (.hasNext iter)
        (let [x (.next iter)
              params (if (vector? x) (conj params (nth x 1)) params)]
          (.append builder (if (vector? x) (nth x 0) x))
          (if (.hasNext iter)
            (do
              (.append builder " ")
              (recur params))
            (into [(.toString builder)] params)
            ))))))
