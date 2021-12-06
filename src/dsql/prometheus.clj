(ns dsql.prometheus
  (:require [dsql.core :as ql]
            [cheshire.core]
            [clojure.string :as str])
  (:refer-clojure :exclude [format]))


(defmethod ql/to-sql
  :prom/param
  [acc opts sym]
  (ql/to-sql acc opts (get-in opts [:params sym] (str "<ERROR: could not resolve " sym ">"))))

(defmethod ql/to-sql
  :prom/agg
  [acc opts [f & args]]
  (let [[args group] (if (map? (last args))
                         [(butlast args) (last args)]
                         [args nil])
        acc (-> acc (conj (str (name f) "(")))]
    (-> (->> args
             (ql/reduce-separated
              "," acc
              (fn [acc arg]
                (ql/to-sql acc opts arg))))
        (conj ")")
        (cond-> (:by group)
          (conj " by (" (str/join "," (map name (:by group))) ")")))))

(defmethod ql/to-sql
  :prom/fn
  [acc opts [f & args]]
  (let [acc (-> acc
                (conj (str (name f) "(")))]
    (->
     (->> args
          (ql/reduce-separated
           "," acc
           (fn [acc arg]
             (ql/to-sql acc opts arg))))
     (conj ")"))))

(defmethod ql/to-sql
  :prom/op
  [acc opts [op a b]]
  (-> acc
      (ql/to-sql opts a)
      (into [(name op)])
      (ql/to-sql opts b)))

(defmethod ql/to-sql
  :prom/lables
  [acc opts labels]
  (let [acc (conj acc "{")]
    (->
     (->> labels
          (ql/reduce-separated
           "," acc
           (fn [acc [k v]]
             (let [[op v] (if (sequential? v)
                            [(get {:! "!=" :# "=~" :#! "!~"} (first v)) (second v)]
                            ["=" v])]
               (-> acc
                   (conj (name k))
                   (conj op)
                   (conj (pr-str v)))))))
     (conj "}"))))

(defmethod ql/to-sql
  :prom/vector
  [acc opts [pname & others]]
  (let [acc (-> acc
                (conj (str (name pname))))]
    (->> others
         (reduce (fn [acc o]
                   (ql/to-sql acc opts o))
                 acc))))

(defmethod ql/to-sql
  :prom/range
  [acc opts [num units]]
  (into acc ["[" num (name units) "]"]))

(defmethod ql/to-sql
  :?
  [acc opts data]
  (conj acc (if (keyword? data)
              (name data)
              (str data))))

(def fns
  #{:abs
    :absent
    :absent_over_time
    :ceil
    :changes
    :clamp
    :clamp_max
    :clamp_min
    :day_of_month
    :day_of_week
    :days_in_month
    :delta
    :deriv
    :exp
    :floor
    :histogram_quantile
    :holt_winters
    :hour
    :idelta
    :increase
    :irate
    :label_join
    :label_replace
    :ln
    :log2
    :log10
    :minute
    :month
    :predict_linear
    :rate
    :resets
    :round
    :scalar
    :sgn
    :sort
    :sort_desc
    :sqrt
    :time
    :timestamp
    :vector
    :year})

(def aggs
  #{:sum ;;(calculate sum over dimensions)
    :min ;;(select minimum over dimensions)
    :max ;;(select maximum over dimensions)
    :avg ;;(calculate the average over dimensions)
    :group ;;(all values in the resulting vector are 1)
    :stddev ;;(calculate population standard deviation over dimensions)
    :stdvar ;;(calculate population standard variance over dimensions)
    :count ;;(count number of elements in the vector)
    :count_values ;;(count number of elements with the same value)
    :bottomk ;;(smallest k elements by sample value)
    :topk ;;(largest k elements by sample value)
    :quantile ;;(calculate φ-quantile ;;(0 ≤ φ ≤ 1) over dimensions)
    })

(def ops
  #{:/ :* :+ :- :pow})

(defn resolve-type [x]
  (cond
    (symbol? x)
    :prom/param

    (map? x)
    :prom/lables

    (sequential? x)
    (cond
      (contains? ops (first x)) :prom/op
      (contains? aggs (first x)) :prom/agg
      (contains? fns (first x)) :prom/fn
      (keyword? (first x)) :prom/vector
      (int? (first x)) :prom/range
      :else :?)
    :else :?))

(defn format [ctx node]
  (->>
   (ql/to-sql []  (merge ctx {:resolve-type #'resolve-type}) node)
   (str/join "")))
