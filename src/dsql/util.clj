(ns dsql.util
  (:require [clojure.string :as str]
            [clojure.spec.alpha :as s]))

(defn strip-nils [m]
  (filterv (fn [[k v]] (not (nil? v))) m))

(defn map-of-nils? [node]
  (and (map? node) (empty? (strip-nils node))))

(defn to-sql-word
  "Example: :group-by -> 'GROUP BY'"
  [keyword]
  (str/upper-case (str/replace (name keyword) #"-" " ")))

(defmacro specase [x & args]
  `(cond
     ~@(reduce (fn [vec spec]
                 (conj vec `(s/valid? ~spec ~x) spec))
               []
               args)
     :else nil))
