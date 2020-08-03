(ns dsql.honeysql_test
  (:require [dsql.honeysql :as hql]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [honeysql.core :as honey-sql]
            [dsql.core :as ql]
            [clojure.string :as str]))

;; Symbols that don't make sense to keep in the original honeysql format
(def replace-symbols
  {"in" "IN"})

(defn reformat [[sql & params]]
  (let [new-sql (reduce
                  (fn [sql [original replacement]]
                    (str/replace sql original replacement))
                  sql replace-symbols)]
    (into [new-sql] params)))

(defmacro with-namespace
  "Adds a namespace to honeysql functions inside a form"
  [ns form]
  (walk/postwalk
    #(if (#{'call 'raw 'param} %)
       (symbol (name ns) (name %))
       %)
    form))

(defmacro check-produces-same [query & params]
  `(let [hql-res# (-> (with-namespace hql ~query)
                      (hql/format ~@params))
         honey-res# (-> (with-namespace honey-sql ~query)
                        (honey-sql/format ~@params)
                        (reformat))]
     (is (= hql-res# honey-res#))))



(deftest test-aliased-value
  (is (= (ql/to-sql [] {} (ql/default-meta-type [:b :c] :hql/projection-va))
         ["b" "c"]))

  (is (= (ql/to-sql []
                    {:add-as? true}
                    (ql/default-meta-type [:b :c] :hql/projection-va))
         ["b" "AS" "c"])))

(deftest test-projection
  (is (= (ql/to-sql []
                    {} (ql/default-meta-type [:a [:b :c]] :hql/projection))
         ["a" "," "b" "c"]))

  (is (= (ql/to-sql []
                    {:add-as? true}
                    (ql/default-meta-type [:a [:b :c]] :hql/projection))
         ["a" "," "b" "AS" "c"])))

(deftest test-select
  (is (= (hql/format {:select [:a [:b :c]]})
         ["SELECT a, b AS c"])))

(deftest test-from
  (is (= (hql/format {:from [:a [:b :c]]})
         ["FROM a, b c"])))

(deftest test-condition-clause
  (is (= (ql/to-sql []
                    {}
                    (ql/default-meta-type [:not [:= :a :b]] :hql/condition))
         ["NOT" "a" "=" "b"])))

(deftest test-operators
  (is (= (ql/to-sql
           [] {}
           (ql/default-meta-type
             [:= :a nil]
             :hql/condition))
         ["a" "IS" "NULL"]))

  (testing "with plain parameters"
    (is (= (ql/to-sql
             [] {:resolve-type hql/operators-resolver}
             (ql/default-meta-type
               [:and [:= :a 1] [:= :c "abc"]]
               :hql/condition))
           ["(" "a" "=" ["?" 1] "AND" "c" "=" ["?" "abc"] ")"])))

  (testing "with collection parameters"
    (is (= (ql/to-sql
             [] {:resolve-type hql/operators-resolver}
             (ql/default-meta-type
               [:in :coll [1 2 3]]
               :hql/condition))
           ["(" "coll" "IN" "(" ["?" 1] "," ["?" 2] "," ["?" 3] ")" ")"]))))

(deftest test-call
  (testing "as operator"
    (is (= (hql/format {:select [[(hql/call :+ 1 2) :a]]})
           ["SELECT ? + ? AS a" 1 2])))

  (testing "as fn"
    (is (= (hql/format {:select [[(hql/call :func 1 2) :a]]})
           ["SELECT func (?, ?) AS a" 1 2])))

  (testing "nested calls"
    (is (= (hql/format {:select [[(hql/call :func 1 (hql/call :+ 2 3)) :a]]})
           ["SELECT func (?, ? + ?) AS a" 1 2 3]))))

;; ===================
;; compatibility tests
;; ===================

(deftest simple-select
  (check-produces-same
    {:select [:*] :from [:users] :where [:= :id 1]})

  (testing "with aliases"
    (check-produces-same
      {:select [[:id :i] :*] :from [[:users :u]]})))

(deftest conditions-compatibility-check
  (testing "="
    (check-produces-same
      {:where [:= :a :b]})
    (check-produces-same
      {:where [:not [:= :a :b]]})
    (check-produces-same
      {:where [:not [:= :a "123"]]})
    (testing "as str op"
      (check-produces-same
        {:where  ["=" :name 1]})))

  (testing "is"
    (check-produces-same
      {:where [:is :a :b]})
    (check-produces-same
      {:where [:not [:= :a :b]]})
    (check-produces-same
      {:where [:not [:= :a "123"]]}))

  (testing "AND condition"
    (check-produces-same
      {:where [:and [:= :id 1] [:not [:= :name "123"]]]})

    (testing "that includes a single form"
      (check-produces-same
        {:where [:and [:= :id 1]]})
      (check-produces-same
        {:where [:not [:and [:not [:= :id 1]]]]}))

    (testing "that is empty"
      (check-produces-same
        {:select [:*] :from [:users] :where [:and]})))

  (testing ":or condition"
    (check-produces-same
      {:where [:or [:= :id 1] [:= :name "123"]]})

    (testing "that includes a single form"
      (check-produces-same
        {:where [:or [:= :id 1]]}))

    (testing "that is empty"
      (check-produces-same
        {:where [:or]}))))

(deftest fns
  (testing "with honeysql function"
    ;; TODO
    ;(testing "call"
    ;  (testing "operator"
    ;    (check-produces-same
    ;      {:select [:foo (call :+ :a :b)]}))
    ;
    ;  (testing "predefined fn"
    ;    (check-produces-same
    ;      {:select [:foo (call :cast :bar :integer)]})
    ;    (testing "with alias"
    ;      (check-produces-same
    ;        {:select [:foo [(call :cast :bar :integer) :alias]]}))))

    (testing "raw"
      (check-produces-same
        {:select [:foo (raw "current_time()")]})

      (testing "with alias"
        (check-produces-same
          {:select [:foo [(raw "current_time()") :alias]]})))))

        ;; TODO
        ;(check-produces-same
        ;  {:select [:foo [(raw ["cast" "a" "2"]) :alias]]})))))

(deftest select-with-keyword-alias
  (check-produces-same
    {:select [[:u :group]]
     :from   [[:users :select]]}))

(deftest select-with-one-join
  (testing "left join"
    (testing "with alias"
      (check-produces-same
        {:select    [:a.* :b.*]
         :from      [[:table1 :a]]
         :left-join [[:table2 :b] [:= :b.abc :a.abc]]}))

    (testing "without alias"
      (check-produces-same
        {:select    [:*]
         :from      [[:table1 :a]]
         :left-join [:table2 [:= ::table2.abc :a.abc]]})))

  (testing "right join"
    (testing "with alias"
      (check-produces-same
        {:select     [:a.* :b.*]
         :from       [[:table1 :a]]
         :right-join [[:table2 :b] [:= :b.abc :a.abc]]}))

    (testing "without alias"
      (check-produces-same
        {:select     [:*]
         :from       [[:table1 :a]]
         :right-join [:table2 [:= ::table2.abc :a.abc]]})))

  (testing "full join"
    (testing "with alias"
      (check-produces-same
        {:select    [:a.* :b.*]
         :from      [[:table1 :a]]
         :full-join [[:table2 :b] [:= :b.abc :a.abc]]}))

    (testing "without alias"
      (check-produces-same
        {:select    [:*]
         :from      [[:table1 :a]]
         :full-join [:table2 [:= ::table2.abc :a.abc]]})))

  (testing "inner join"
    (testing "with alias"
      (check-produces-same
        {:select [:a.* :b.*]
         :from   [[:table1 :a]]
         :join   [[:table2 :b] [:= :b.abc :a.abc]]}))

    (testing "without alias"
      (check-produces-same
        {:select [:*]
         :from   [[:table1 :a]]
         :join   [:table2 [:= ::table2.abc :a.abc]]}))))

(deftest select-with-all-joins
  (testing "with alias"
    (check-produces-same
      {:select     [:a.* :b.*]
       :from       [[:table1 :a]]
       :join       [[:table2 :b] [:= :b.abc :a.abc]]
       :left-join  [[:table3 :c] [:= :c.abc :a.abc]]
       :right-join [[:table4 :d] [:or
                                  [:= :d.abc :a.abc]
                                  [:= :d.abc :c.abc]]]
       :full-join  [[:table5 :e] [:and
                                  [:= :e.abc :a.abc]
                                  [:= :e.abc :d.abc]]]}))

  (testing "without alias"
    (check-produces-same
      {:select     [:*]
       :from       [:table1]
       :join       [:table2 [:= ::table2.abc :table1.abc]]
       :left-join  [:table3 [:= ::table3.abc :table2.abc]]
       :right-join [:table4 [:or
                             [:= :table4.abc :table1.abc]
                             [:= :table4.abc :table3.abc]]]
       :full-join  [:table5 [:and
                             [:= :table5.abc :table1.abc]
                             [:= :table5.abc :table4.abc]]]})))

(deftest test-conditions-with-nulls
  (check-produces-same
    {:where [:= :a nil]})
  (check-produces-same
    {:where [:= nil :a]})

  (check-produces-same
    {:where [:is :a nil]})
  (check-produces-same
    {:where [:is nil :a]})

  (check-produces-same
    {:where [:is-not :a nil]})
  (check-produces-same
    {:where [:is-not nil :a]})

  (check-produces-same
    {:where [:<> :a nil]})
  (check-produces-same
    {:where [:<> nil :a]})

  (check-produces-same
    {:where [:not= :a nil]})
  (check-produces-same
    {:where [:not= nil :a]})

  (check-produces-same
    {:where [:!= :a nil]})
  (check-produces-same
    {:where [:!= nil :a]}))


;; =======================
;; honeysql original tests
;; =======================

;; TODO: cte
;(deftest select-cte
;  (check-produces-same
;    {:with       [[:cte {:select [:*]
;                         :from   [:example]
;                         :where  [:= :example-column 0]}]]
;     :select     [:f.* :b.baz :c.quux [:b.bla :bla-bla]
;                  :%now (raw "@x := 10")]
;     ;;:un-select :c.quux
;     :modifiers  :distinct
;     :from       [[:foo :f] [:baz :b]]
;     :join       [:draq [:= :f.b :draq.x]]
;     :left-join  [[:clod :c] [:= :f.a :c.d]]
;     :right-join [:bock [:= :bock.z :c.e]]
;     :full-join  [:beck [:= :beck.x :c.y]]
;     :where      [:or
;                  [:and [:= :f.a "bort"] [:not= :b.baz :?param1]]
;                  [:< 1 2 3]
;                  [:in :f.e [1 (param :param2) 3]]
;                  [:between :f.e 10 20]]
;     ;;:merge-where [:not= nil :b.bla]
;     :group-by   :f.a
;     :having     [:< 0 :f.e]
;     :order-by   [[:b.baz :desc] :c.quux [:f.a :nulls-first]]
;     :limit      50
;     :offset     10}))

;; TODO: call
;(deftest test-cast
;  (check-produces-same
;    {:select [:foo (call :cast :bar :integer)]})
;  (check-produces-same
;    {:select [:foo (call :cast :bar 'integer)]}))

(deftest test-operators
  (testing "="
    (testing "with nil"
      (check-produces-same
        {:select [:*]
         :from   [:customers]
         :where  [:= :name nil]})

      (check-produces-same
        {:select [:*]
         :from   [:customers]
         :where  [:= :name :?name]}
        {:name nil})))

  (testing "in"
    (doseq [[cname coll] [[:vector []] [:set #{}] [:list '()]]]
      (testing (str "with values from a " (name cname))
        (let [values (conj coll 1)]
          (check-produces-same
            {:select [:*]
             :from   [:customers]
             :where  [:in :id values]})

          (check-produces-same
            {:select [:*]
             :from   [:customers]
             :where  [:in :id :?ids]}
            {:ids values}))))

    (testing "with more than one integer"
      (let [values [1 2]]
        (check-produces-same
          {:select [:*]
           :from   [:customers]
           :where  [:in :id values]})

        (check-produces-same
          {:select [:*]
           :from   [:customers]
           :where  [:in :id :?ids]}
          {:ids values})))

    (testing "with more than one string"
      (let [values ["1" "2"]]
        (check-produces-same
          {:select [:*]
           :from   [:customers]
           :where  [:in :id values]})

        (check-produces-same
          {:select [:*]
           :from   [:customers]
           :where  [:in :id :?ids]}
          {:ids values})))))

;; TODO: call
;(deftest test-select-case
;  (check-produces-same
;    {:select [(call
;                :case
;                [:< :foo 0] -1
;                [:and [:> :foo 0] [:= (call :mod :foo 2) 0]] (call :/ :foo 2)
;                :else 0)]
;     :from   [:bar]})
;
;  (let [param1 1
;        param2 2
;        param3 "three"]
;    (check-produces-same
;      {:select [(call
;                  :case
;                  [:= :foo :?param1] 0
;                  [:= :foo :bar] (param :param2)
;                  [:= :bar 0] (call :* :bar :?param3)
;                  :else "param4")]
;       :from   [:baz]}
;      {:param1 param1
;       :param2 param2
;       :param3 param3})))

