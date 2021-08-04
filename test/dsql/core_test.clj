(ns dsql.core-test
  (:require [dsql.core :as sut]
            [clojure.test :refer [deftest is]]
            [clojure.string :as str]))

(defmethod sut/to-sql
  :test/text
  [acc opts [_ data]]
  (conj acc (sut/string-litteral data)))

(defmethod sut/to-sql
  :test/param
  [acc opts [_ data]]
  (conj acc ["?" data]))

(defmethod sut/to-sql
  :test/projection
  [acc opts data]
  (->> (dissoc data :ql/type)
       (sut/reduce-separated "," acc
        (fn [acc [k node]]
          (let [acc (sut/to-sql acc opts node)]
            (conj acc "as" (name k)))))))

(defmethod sut/to-sql
  :test/select
  [acc opts data]
  (->> [[:select :test/projection]
        [:from :test/from]]
       (sut/reduce-acc
        acc
        (fn [acc [k default-type]]
          (if-let [sub-node (get data k)]
            (-> acc
                (conj (str/upper-case (str/replace (name k) #"-" " ")))
                (sut/to-sql opts (sut/default-type sub-node default-type)))
            acc)))))

(defmacro format= [q patt]
  `(let [res# (sut/format {} ~q)]
     (is (= ~patt res#))
     res#))

(deftest test-dsql

  (format=
   {:ql/type :test/select
    :select {:resource :resource :string [:test/text "string"]}
    :from :user}
   ["SELECT resource as resource , 'string' as string FROM user"])

  (is (= ["SELECT resource as resource , ? as string FROM user" "string"]
         (sut/format {} {:ql/type :test/select
                         :select {:resource :resource :string [:test/param "string"]}
                         :from :user}))))
  



