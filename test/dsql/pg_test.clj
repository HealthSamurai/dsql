(ns dsql.pg-test
  (:require [dsql.pg :as sut]
            [clojure.test :refer [deftest is testing]]))

(defmacro format= [q patt]
  `(let [res# (sut/format ~q)]
     (is (= ~patt res#))
     res#))

(deftest test-dsql-pgi

  (testing "select"
    (format=
     {:ql/type :pg/projection
      :a "string"
      :b 1
      :fn ^:pg/kfn[:substring :col :from 1 :for 3]}
     ["'string' as a , 1 as b , substring( col from 1 for 3 ) as fn"])


    (format=
     {:ql/type :pg/projection
      :sub-q ^:pg/sub-select{:select :* :from :tbl :limit 1}}
     ["( SELECT * FROM tbl LIMIT 1 ) as \"sub-q\""])

    )

  (format=
   {:select :*
    :from :user
    :where ^:pg/op[:= :user.id [:pg/param "u-1"]]
    :limit 100}
   ["SELECT * FROM user WHERE user.id = ? LIMIT 100" "u-1"])

  (format=
   {:select :*
    :from :user
    :where ^:pg/op[:= :user.id "u'-1"]
    :limit 100}
   ["SELECT * FROM user WHERE user.id = 'u''-1' LIMIT 100"])

  (format=
   {:select :*
    :from :user
    :where ^:pg/op[:= :user.id "u-1"]
    :limit 100}
   ["SELECT * FROM user WHERE user.id = 'u-1' LIMIT 100"])

  (format=
   (merge
    {:ql/type :pg/and
     :by-id ^:pg/op[:= :id [:pg/param "u-1"]]}
    {:by-status ^:pg/op[:= :status "active"]})
   ["/*by-id*/ id = ? AND /*by-status*/ status = 'active'" "u-1"])

  (format=
   {:ql/type :pg/projection
    :id :id
    :raw [:pg/sql "current_time()"]
    :fn ^:pg/fn[:current_time "1" [:pg/param "x"]]}
   ["id as id , current_time() as raw , current_time( '1' , ? ) as fn" "x"])

  (format=
   {:select {:resource ^:pg/op[:|| :resource ^:pg/obj{:id :id :resourceType "Patient"}]}
    :from {:pt :patient}
    :where {:match
            ^:pg/op[:&& ^:pg/fn[:knife_extract_text :resource ^:pg/jsonb[["match"]]]
                    [:pg/array-param :text ["a-1" "b-2"]]]}
    :limit 10}
   ["SELECT resource || jsonb_build_object( 'id' , id , 'resourceType' , 'Patient' ) as resource FROM patient pt WHERE /*match*/ knife_extract_text( resource , '[[\"match\"]]' ) && ?::text[] LIMIT 10" "{\"a-1\",\"b-2\"}"])

  (format=
   {:select :*
    :from :patient
    :where nil
    :limit 10}
   ["SELECT * FROM patient LIMIT 10"])

  (format=
   {:select :* 
    :from :patient
    :where {}
    :limit 10}
   ["SELECT * FROM patient LIMIT 10"])

  (format=
   {:select :* 
    :from :patient
    :where {:nil nil
            :null nil}
    :limit 10}
   ["SELECT * FROM patient LIMIT 10"])

  (format=
   {:select :* 
    :from :patient
    :group-by {:name :name, :same :same}
    :limit 10}
   ["SELECT * FROM patient GROUP BY /*name*/ name , /*same*/ same LIMIT 10"])



  (format=
   {:ql/type :pg/index
    :index :users_id_idx
    :if-not-exists true
    :concurrently true
    :on :users
    :using :GIN
    :expr {:first {:expr ^:pg/fn[:lower :user.name]
                   :collate :de_DE
                   :opclass :gin_opts
                   :nulls :first}}
    :tablespace :mytbs
    :with {:fillfactor 70}
    :where ^:pg/op[:= :user.status "active"]}


   ["CREATE INDEX users_id_idx IF NOT EXISTS ON users USING GIN ( EXPR??? ) WITH ( fillfactor = 70 ) TABLESPACE mytbs WHERE user.status = 'active'"]
   )

  (format= nil ["NULL"])

  (format= [:jsonb/-> :resource :name]
           ["resource -> 'name'"])

  (format= [:resource-> :name]
           ["resource->'name'"])

  (format= ^:pg/op[:|| :col [:pg/param "string"]]
           ["col || ?" "string"])

  (format= ^:pg/op[:|| {:ql/type :pg/sub-select :select :* :from :user} [:pg/param "string"]]
           ["( SELECT * FROM user ) || ?" "string"])

  (format= [:jsonb/->> :resource :name]
           ["resource ->> 'name'"])

  (format= [:jsonb/#> :resource [:name 0 :given]]
           ["resource #> '{name,0,given}'"])

  (format= [:jsonb/#>> :resource [:name 0 :given]]
           ["resource #>> '{name,0,given}'"])

  (format= {:ql/type :jsonb/obj
            :value [:jsonb/#>> :resource [:clearing_house :advance_md :payer-id]]
            :system [:pg/param "amd"]}

           ["jsonb_build_object( 'value' , resource #>> '{\"clearing_house\",\"advance_md\",\"payer-id\"}' , 'system' , ? )"
            "amd"])

  (format= {:ql/type :pg/update
            :update :healthplan
            :set {:resource ^:pg/op[:|| :resource ^:jsonb/obj{:identifier ^:jsonb/array[^:jsonb/obj{:value [:jsonb/#>> :resource [:clearing_house :advance_md :payer-id]]
                                                                                                    :system "amd"}
                                                                                        ^:jsonb/obj{:value [:jsonb/#>> :resource [:clearing_house :change_healthcare :payer-id]]
                                                                                                    :system "change_healthcare"}
                                                                                        ^:jsonb/obj{:value [:jsonb/#>> :resource [:clearing_house :omega :payer-id]]
                                                                                                    :system "omega"}]}]}
            :where {:clearing_house ^:pg/op[:is [:jsonb/-> :resource :clearing_house] [:pg/sql "NOT NULL"]]}}
           ["UPDATE healthplan SET resource = resource || jsonb_build_object( 'identifier' , jsonb_build_array( jsonb_build_object( 'value' , resource #>> '{\"clearing_house\",\"advance_md\",\"payer-id\"}' , 'system' , 'amd' ) , jsonb_build_object( 'value' , resource #>> '{\"clearing_house\",\"change_healthcare\",\"payer-id\"}' , 'system' , 'change_healthcare' ) , jsonb_build_object( 'value' , resource #>> '{\"clearing_house\",omega,\"payer-id\"}' , 'system' , 'omega' ) ) ) WHERE /*clearing_house*/ resource -> 'clearing_house' is NOT NULL"])

  (format= {:ql/type :pg/delete
            :from :healthplan
            :where ^:pg/op[:= :id "111"]}
           ["DELETE FROM healthplan WHERE id = '111'"])

  (format=
   {:ql/type :pg/select
    :select {:count [:pg/count*]}
    :from :dft
    :left-join {:d {:table :document
                    :on {:by-id ^:pg/op[:= :dft.id [:jsonb/->> :d.resource "caseNumber"]]
                         :front ^:pg/op[:= [:jsonb/->> :d.resource :name] "front"]}}}
    :where {:no-scan ^:pg/op[:is :d.id nil]}}

   ["SELECT count(*) as count FROM dft LEFT JOIN document d ON /*by-id*/ dft.id = d.resource ->> 'caseNumber' AND /*front*/ d.resource ->> 'name' = 'front' WHERE /*no-scan*/ d.id is NULL"])

)
