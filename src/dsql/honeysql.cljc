(ns dsql.honeysql
  (:require [dsql.core :as ql]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [dsql.util :as util])
  (:refer-clojure :exclude [format]))

(def operators
  #{"=" "<>"
    "+" "-" "*" "/" "%" "mod" "|" "&" "^"
    "and" "or" "xor"
    "in" "not in" "like" "not like" "regexp"})

(def unary-operators
  #{"not"})

(def fn-aliases
  {:is     :=
   :is-not :<>
   :not=   :<>
   :!=     :<>
   :regex  :regexp})

(def operators-in-parens
  #{:and :or :xor :in})

(def null-op-aliases
  {:is     :is
   :=      :is
   :is-not :is-not
   :<>     :is-not
   :not=   :is-not
   :!=     :is-not})

(defn- is-known-operator [clause]
  (let [op (-> clause first name)]
    (or (operators op)
        (unary-operators op)
        (fn-aliases (keyword op)))))


;; Specs

(s/def :hql/symbol (s/or :kwd keyword? :str string? :sym symbol?))

(s/def :hql/projection-value any?)
(s/def :hql/projection-alias :hql/symbol)
(s/def :hql/projection-va (s/cat :value :hql/projection-value
                                 :alias (s/? :hql/projection-alias)))
(s/def :hql/projection-item (s/or :value :hql/projection-value
                                  :va :hql/projection-value))

(s/def :hql/select (s/+ :hql/projection-item))
(s/def :hql/from (s/+ :hql/projection-item))


(s/def :hql/op (s/and vector?
                      #(s/valid? :hql/symbol (first %))
                      is-known-operator))

(s/def :hql/unary-op (s/and :hql/op #(= 2 (count %)) (comp unary-operators name first)))
(s/def :hql/binary-op (s/and :hql/op #(= 3 (count %))))

(s/def :hql/binary-null-op (s/and :hql/binary-op
                                  (comp null-op-aliases #(get % 0))
                                  #(or (nil? (get % 1))
                                       (nil? (get % 2)))))

(s/def :hql/op-alias (s/and :hql/op (comp fn-aliases #(get % 0))))


(s/def :hql/param (s/or :number #(= java.lang.Long (type %))
                        :string #(= java.lang.String (type %))))

(s/def :hql/param-alias (s/and keyword?
                               #(-> (name %) (str/starts-with? "?"))))

(s/def :hql/params (s/and coll? (s/every (s/or :param :hql/param
                                               :alias :hql/param-alias))))

(s/def :hql/condition :hql/op)

(s/def :hql/join-item (s/cat :table :hql/projection-item
                             :condition :hql/condition))

(s/def :hql/join (s/+ :hql/join-item))


(def keys-for-select
  [[:select {:type    :hql/projection
             :add-as? true}]
   [:from {:type :hql/projection}]
   [:join {:type :hql/join
           :name "INNER JOIN"}]
   [:left-join {:type :hql/join}]
   [:right-join {:type :hql/join}]
   [:full-join {:type :hql/join}]
   [:where {:type :hql/condition}]])
;[:group-by {:type :pg/group-by}]
;[:having {:type :pg/having}]
;[:window {:type :pg/window}]
;[:order-by {:type :pg/order-by}]
;[:limit {:type :pg/limit}]
;[:offset {:type :pg/offset}]
;[:fetch {:type :pg/fetch}]
;[:for {:type :pg/for}]])

(defn projection-resolver
  "Resolves type of nodes in an :hql/projection – [:val1 [:val2 :alias]]"
  [x]
  (util/specase
   x
   :hql/projection-va))

(defn operators-resolver
  "Resolves type of nodes when operators are used – [:and [:= :a nil] ...]"
  [x]
  (util/specase
   x
   :hql/binary-null-op
   :hql/op-alias
   :hql/binary-op
   :hql/unary-op
   :hql/op
   :hql/param
   :hql/param-alias
   :hql/params))


;; Main SQL clauses

(defmethod ql/to-sql
  :hql/select
  [acc opts data]
  (reduce
   (fn [acc [k {:keys [type name add-as?]}]]
     (let [sub-node (get data k)]
       (if (and sub-node
                (not (util/map-of-nils? sub-node)))
         (-> acc
             (conj (util/to-sql-word (or name k)))
             (ql/to-sql (assoc opts :add-as? add-as?)
                        (ql/default-meta-type sub-node type)))
         acc)))
   acc keys-for-select))

(defmethod ql/to-sql
  :hql/join
  [acc opts join-clauses]
  (reduce
   (fn [acc [table condition]]
     (-> acc
         (ql/to-sql (assoc opts :resolve-type projection-resolver) table)
         (conj "ON")
         (ql/to-sql opts (ql/default-meta-type condition :hql/condition))))
   acc
   (partition 2 (s/assert :hql/join join-clauses))))

(defmethod ql/to-sql
  :hql/condition
  [acc opts node]
  (ql/to-sql acc
             (assoc opts :resolve-type operators-resolver)
             (vary-meta node dissoc :ql/type)))


;; Projection clauses

(defmethod ql/to-sql
  :hql/projection
  [acc opts data]
  (ql/reduce-separated
   "," acc
   (fn [acc val]
     (ql/to-sql acc
                (assoc opts :resolve-type projection-resolver)
                val))
   data))

(defmethod ql/to-sql
  :hql/projection-va
  [acc opts [value alias]]
  (let [add-as (fn [acc]
                 (if (:add-as? opts)
                   (conj acc "AS")
                   acc))]
    (if (nil? alias)
      (ql/to-sql acc opts value)
      (-> acc
          (ql/to-sql opts value)
          (add-as)
          (conj (ql/escape-ident (:keywords opts) alias))))))


;; Operators

(defmethod ql/to-sql
  :hql/op
  [acc opts [op & args]]
  (ql/parens
   acc
   (fn [acc]
     (->> args
          (remove nil?)
          (ql/reduce-separated
           (util/to-sql-word op) acc
           (fn [acc val]
             (ql/to-sql acc opts val)))))))

(defmethod ql/to-sql
  :hql/op-alias
  [acc opts [op & args]]
  (ql/to-sql acc opts (into [(or (fn-aliases op) op)] args)))

(defmethod ql/to-sql
  :hql/unary-op
  [acc opts [op arg]]
  (-> (conj acc (util/to-sql-word op))
      (ql/to-sql opts arg)))

(defmethod ql/to-sql
  :hql/binary-op
  [acc opts [op left right]]
  (letfn [(format-operation [acc]
            (-> acc
                (ql/to-sql opts left)
                (conj (util/to-sql-word op))
                (ql/to-sql opts right)))]
    (if (operators-in-parens op)
      (ql/parens acc format-operation)
      (format-operation acc))))

(defmethod ql/to-sql
  ;; Transforms "= null" to "is null"
  :hql/binary-null-op
  [acc opts [op left right]]
  (let [op (or (null-op-aliases op) op)]
    (ql/to-sql acc opts
               (ql/default-meta-type
                (if (nil? left)
                  [op right left]
                  [op left right])
                :hql/binary-op))))


;; Function calls

(defmethod ql/to-sql
  :hql/fn
  [acc opts [fname & args]]
  (-> acc
      (ql/to-sql opts fname)
      (ql/parens
       (fn [acc]
         (ql/reduce-separated
          "," acc
          (fn [acc val]
            (ql/to-sql acc opts val))
          args)))))

;; Param clauses

(defmethod ql/to-sql
  :hql/params
  [acc opts node]
  (ql/parens
   acc
   #(ql/reduce-separated
     "," %
     (fn [acc node]
       (ql/to-sql acc opts node))
     node)))

(defmethod ql/to-sql
  :hql/param
  [acc opts v]
  (conj acc ["?" v]))

(defmethod ql/to-sql
  :hql/param-alias
  [acc opts key]
  (let [param-name (-> (name key) (subs 1) (keyword))
        value (param-name (:params-map opts))]
    (if (coll? value)
      (ql/to-sql acc opts value)
      (conj acc ["?" value]))))


;; Custom functions

(defmethod ql/to-sql
  :hql/fn
  [acc opts [fn-name & args]]
  (let [opts (assoc opts :resolve-type operators-resolver)]
    (-> acc
        (conj (name fn-name))
        (ql/parens (fn [acc]
                     (ql/reduce-separated
                      "," acc
                      (fn [acc arg]
                        (ql/to-sql acc opts arg))
                      args))))))


;; Other fns

(defn call [op & args]
  (ql/default-meta-type
   (into [op] args)
   (if (operators (name op))
     :hql/condition
     :hql/fn)))

(defn raw [sql]
  (ql/default-meta-type {:sql sql} :hql/raw))

(defmethod ql/to-sql
  :hql/raw
  [acc opts {:keys [sql]}]
  (if (vector? sql)
    (reduce (fn [acc node]
              (conj acc (str node)))
            acc
            sql)
    (conj acc sql)))


;; Format

(defn reformat-punctuation [sql]
  (-> sql
      (str/replace #"\s(\)|,)" "$1")
      (str/replace #"(\()\s" "$1")))

(defn format [node & [params-map]]
  (binding [s/*compile-asserts* true]
    (let [[sql & params] (ql/format
                          {:params-map params-map}
                          (ql/default-type node :hql/select))
          sql (reformat-punctuation sql)]
      (into [sql] params))))
