(ns dsql.pg-test
  (:require [dsql.pg :as sut]
            [clojure.test :refer [deftest is testing]]
            [dsql.core :as ql]))

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
     ["( SELECT * FROM tbl LIMIT 1 ) as \"sub-q\""]))

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
   (merge
    {:ql/type :pg/or
     :by-id ^:pg/op[:= :id [:pg/param "u-1"]]}
    {:by-status ^:pg/op[:= :status "active"]})
   ["( /*by-id*/ id = ? OR /*by-status*/ status = 'active' )" "u-1"])


  (format=
   {:ql/type :pg/projection
    :id :id
    :raw [:pg/sql "current_time()"]
    :fn ^:pg/fn[:current_time "1" [:pg/param "x"]]}
   ["current_time( '1' , ? ) as fn , id as id , current_time() as raw" "x"])

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
    :expr [[:resource-> :a] [:resource-> :b]]
    :tablespace :mytbs
    :with {:fillfactor 70}
    :where ^:pg/op[:= :user.status "active"]}

   ["CREATE INDEX IF NOT EXISTS users_id_idx ON users USING GIN ( ( resource->'a' ) , ( resource->'b' ) ) WHERE user.status = 'active'"]
   )

  (format=
    {:ql/type   :pg/drop-index
     :index     :my_table_index
     :if-exists true}
    ["DROP INDEX IF EXISTS my_table_index"])

  (format=
    {:ql/type :pg/drop-index
     :index   :my_table_index}
    ["DROP INDEX my_table_index"])

  (format= nil ["NULL"])

  (format= [:jsonb/-> :resource :name]
           ["resource -> 'name'"])

  (format= [:jsonb/-> :resource 0]
           ["resource -> 0"])

  (format= [:resource-> :name]
           ["resource->'name'"])

  (format= [:is [:resource-> :name] nil]
           ["resource->'name' IS NULL"])

  (format= [:is-not [:resource-> :name] nil]
           ["resource->'name' IS NOT NULL"])


  (format= [:= 1 1]
           ["1 = 1"])

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
            :where ^:pg/op[:= :id "111"]
            :returning :id}
           ["DELETE FROM healthplan WHERE id = '111' RETURNING id"])

  (format=
   {:ql/type :pg/select
    :select {:count [:pg/count*]}
    :from :dft
    :left-join {:d {:table :document
                    :on {:by-id ^:pg/op[:= :dft.id [:jsonb/->> :d.resource "caseNumber"]]
                         :front ^:pg/op[:= [:jsonb/->> :d.resource :name] "front"]}}}
    :where {:no-scan ^:pg/op[:is :d.id nil]}}

   ["SELECT count(*) as count FROM dft LEFT JOIN document d ON /*by-id*/ dft.id = d.resource ->> 'caseNumber' AND /*front*/ d.resource ->> 'name' = 'front' WHERE /*no-scan*/ d.id is NULL"])


  (format=
   {:ql/type :pg/select
    :explain {:analyze true}
    :select {:count [:pg/count*]}
    :from :dft
    :left-join {:d {:table :document
                    :on {:by-id ^:pg/op[:= :dft.id [:jsonb/->> :d.resource "caseNumber"]]
                         :front ^:pg/op[:= [:jsonb/->> :d.resource :name] "front"]}}}
    :where {:no-scan ^:pg/op[:is :d.id nil]}}

   ["EXPLAIN ANALYZE SELECT count(*) as count FROM dft LEFT JOIN document d ON /*by-id*/ dft.id = d.resource ->> 'caseNumber' AND /*front*/ d.resource ->> 'name' = 'front' WHERE /*no-scan*/ d.id is NULL"])

  (format= [:pg/params-list [1 2 3]]
           ["( ? , ? , ? )" 1 2 3])

  (= {:ql/type :pg/params-list :code :params-required}
   (try (sut/format [:pg/params-list nil])
        (catch clojure.lang.ExceptionInfo e (ex-data e))))

  (= {:ql/type :pg/params-list :code :params-required}
   (try (sut/format [:pg/params-list []])
        (catch clojure.lang.ExceptionInfo e (ex-data e))))

  (= {:ql/type :pg/params-list :code :params-required}
   (try (sut/format [:pg/params-list '()])
          (catch clojure.lang.ExceptionInfo e (ex-data e))))

  (format= [:in :column [:pg/params-list [1 2 3]]]
           ["column IN ( ? , ? , ? )" 1 2 3])

  (format= [:not-in :column [:pg/params-list [1 2 3]]]
           ["column NOT IN ( ? , ? , ? )" 1 2 3])

  (format= [:not [:pg/include-op [:resource->> :tags]
                  (into ^:jsonb/array[] ["a" "b" "c"])]]

           ["NOT resource->>'tags' @> jsonb_build_array( 'a' , 'b' , 'c' )"])

  (format= [:not ["@@" [:resource->> :tags]
                  [:pg/cast "$.foo[*]" :jsonpath]]]
           ["NOT resource->>'tags' @@ ( '$.foo[*]' )::jsonpath"])

  (format= [:not ["@@::jp" [:resource->> :tags]
                  "$.foo[*]"]]
           ["NOT resource->>'tags' @@ ( '$.foo[*]' )::jsonpath"])

  (format= ["@?::jp" :resource "$.\"active\"[*] ? (@ == true)"]
           ["resource @? ( '$.\"active\"[*] ? (@ == true)' )::jsonpath"])

  (format= ["@?" :resource [:pg/cast "$.\"active\"[*] ? (@ == true)" :jsonpath]]
           ["resource @? ( '$.\"active\"[*] ? (@ == true)' )::jsonpath"])

  (format= [:or
            [:ilike [:pg/sql "resource::text"] [:pg/param "%a%"]]
            [:ilike :id [:pg/param "%a%"]]]
           ["( resource::text ILIKE ? OR id ILIKE ? )" "%a%" "%a%"])

  (format=
   [:pg/coalesce [:resource->> :primary_payer] "MISSED"]
   ["COALESCE( resource->>'primary_payer' , 'MISSED' )"])

  (format=
   [:pg/cast [:resource->> :date] ::date]
   ["( resource->>'date' )::date"])

  (format=
   [:jsonb/array_elements_text [:resource-> :tags]]
   ["jsonb_array_elements( resource->'tags' )"])

  (format=
   [:count*]
   ["count(*)"])

  (format=
   [:= 1 1]
   ["1 = 1"])

  (format=
   [:not-with-parens ^:pg/op[:and
                      ^:pg/op[:>= :id [:pg/param "start"]]
                             ^:pg/op[:<= :id [:pg/param "end"]]]]
   ["NOT ( id >= ? and id <= ? )" "start" "end"])

  (format=
   {:ql/type :pg/update
    :update :AmdExport
    :set {:resource ^:pg/op[:|| :resource ^:jsonb/obj{:status "pending"}]}
    :where ^:pg/op[:is [:resource-> :status] nil]}
   ["UPDATE AmdExport SET resource = resource || jsonb_build_object( 'status' , 'pending' ) WHERE resource->'status' is NULL"])

  (format= [:or
            [:ilike [:pg/sql "resource::text"] [:pg/param "%a%"]]
            [:ilike :id [:pg/param "%a%"]]]
           ["( resource::text ILIKE ? OR id ILIKE ? )" "%a%" "%a%"])

  (format=
   {:ql/type :pg/select
    :select [:pg/sql "id,resource"]
    :from :healthcareservices
    :where (with-meta
             (into [:and]
                   (mapv (fn [i]
                           [:or
                            [:ilike [:jsonb/#>> :resource [:name]] i]
                            [:ilike [:jsonb/#>> :resource [:type 0 :coding 0 :code]] i]
                            [:ilike [:jsonb/#>> :resource [:type 0 :coding 1 :code]] i]])
                         ["a" "b"]))
             {:pg/op true})
    :order-by :id}
   ["SELECT id,resource FROM healthcareservices WHERE ( resource #>> '{name}' ILIKE 'a' OR resource #>> '{type,0,coding,0,code}' ILIKE 'a' OR resource #>> '{type,0,coding,1,code}' ILIKE 'a' ) and ( resource #>> '{name}' ILIKE 'b' OR resource #>> '{type,0,coding,0,code}' ILIKE 'b' OR resource #>> '{type,0,coding,1,code}' ILIKE 'b' ) ORDER BY id"])

  (format=
   {:ql/type :pg/select
    :select [:count*]
    :from :oru
    :where ^:pg/op[:and
                   ^:pg/op[:> [:pg/cast [:jsonb/#>> :resource [:message :datetime]] ::timestamp] [:pg/sql "now() - interval '1 week'"]]
                   [:ilike :id "%Z%.CV"]]}
   ["SELECT count(*) FROM oru WHERE ( resource #>> '{message,datetime}' )::timestamp > now() - interval '1 week' and id ILIKE '%Z%.CV'"])

  (format=
   {:ql/type :pg/obj :id :id}
   ["jsonb_build_object( 'id' , id )"])

  (format=
   [:= 1 ^:pg/obj{:id :id}]
   ["1 = jsonb_build_object( 'id' , id )"])

  (format=
   [:= 1 ^:pg/obj^:eval-key{:id :id}]
   ["1 = jsonb_build_object( id , id )"])

  (format=
   {:ql/type :pg/select
    :select {:resource ^:pg/op [:|| :resource ^:pg/obj {:id :id}]
             :pr ^:pg/op [:|| :p.resource ^:pg/obj {:id :p.id}]}
    :from :oru
    :where [:ilike :id "%Z38886%"]
    :left-join {:p {:table :practitioner
                    :on {:by-id ^:pg/op [:= :practitioner.id [:jsonb/#>> :p.resource [:patient_group :order_group 0 :order :requester :provider 0 :identifier :value]]]}}
                :org {:table :organization
                      :on {:by-id
                           ^:pg/op [:= :organization.id [:jsonb/#>> :p.resource [:patient_group :order_group 0 :order :contact :phone 0 :phone]]]}}}
    :order-by :id
    :limit 5}
   ["SELECT p.resource || jsonb_build_object( 'id' , p.id ) as pr , resource || jsonb_build_object( 'id' , id ) as resource FROM oru LEFT JOIN practitioner p ON /*by-id*/ practitioner.id = p.resource #>> '{\"patient_group\",\"order_group\",0,order,requester,provider,0,identifier,value}' LEFT JOIN organization org ON /*by-id*/ organization.id = p.resource #>> '{\"patient_group\",\"order_group\",0,order,contact,phone,0,phone}' WHERE id ILIKE '%Z38886%' ORDER BY id LIMIT 5"]
   
   )

  (format=
   {:ql/type :pg/update
    :update :ORU
    :set {:resource ^:pg/op[:|| :resource ^:jsonb/obj{:status "some-val"}]}
    :where ^:pg/op[:= :id "some-id"]}
   ["UPDATE ORU SET resource = resource || jsonb_build_object( 'status' , 'some-val' ) WHERE id = 'some-id'"]
   )

  (format=
   {:ql/type :pg/update
    :update :xinvoice
    :set {:resource
          ^:pg/op [:||
                   :resource
                   ^:jsonb/obj {:status "sent"
                                :history ^:pg/op[:||
                                                 [:jsonb/#>> :resource [:history]]
                                                 ^:jsonb/array[^:jsonb/obj{:status "sent"
                                                                           :user   ^:jsonb/obj{:id "id"}
                                                                           :date   ^:pg/fn[:now]}]]}]}
    :returning :id}
   ["UPDATE xinvoice SET resource = resource || jsonb_build_object( 'status' , 'sent' , 'history' , resource #>> '{history}' || jsonb_build_array( jsonb_build_object( 'status' , 'sent' , 'user' , jsonb_build_object( 'id' , 'id' ) , 'date' , now( ) ) ) ) RETURNING id"])

  (format=
   {:ql/type :pg/select
    :select :*
    :from :billingcase
    :where [:similar-to [:resource#>> [:patient :display]] "smth"]}
   ["SELECT * FROM billingcase WHERE resource#>>'{patient,display}' SIMILAR TO 'smth'"])

  (format=
   {:ql/type :pg/select
    :select :id
    :from :oru
    :where ^:pg/op[:or
                   ^:pg/op[:is [:resource->> :status] nil]
                   ^:pg/op[:!= [:resource->> :status] "processed"]
                   ]}
   ["SELECT id FROM oru WHERE resource->>'status' is NULL or resource->>'status' != 'processed'"])

  (format=
   {:ql/type :pg/update
    :update  :BillingCase
    :set     {:resource ^:pg/op[:|| :resource ^:jsonb/obj{:report_no ^:pg/op[:|| :id ".CV"]}]}
    :where   [:ilike :id [:pg/param "%Z%"]]}
   ["UPDATE BillingCase SET resource = resource || jsonb_build_object( 'report_no' , id || '.CV' ) WHERE id ILIKE ?" "%Z%"])

  (format=
   {:ql/type :pg/select
    :select {:resource ^:pg/op[:|| :d.resource
                               ^:jsonb/obj{:id :d.id
                                           :partOf ^:pg/op[:|| :_d.resource
                                                           ^:jsonb/obj{:id :_d.id}]}]}
    :from {:d :department}
    :left-join {:_d {:table :department
                     :on ^:pg/op[:= :_d.id [:jsonb/#>> :d.resource [:partOf :id]]]}}
    :where [:ilike [:pg/cast :d.resource ::text] [:pg/param "%%"]]
    :limit [:pg/param 10]}

   ["SELECT d.resource || jsonb_build_object( 'id' , d.id , 'partOf' , _d.resource || jsonb_build_object( 'id' , _d.id ) ) as resource FROM department d LEFT JOIN department _d ON _d.id = d.resource #>> '{partOf,id}' WHERE ( d.resource )::text ILIKE ? LIMIT ?" "%%" 10])

  (let [query-fn (fn [id q] {:ql/type :pg/select
                             :select {:resource ^:pg/op [:|| :resource ^:jsonb/obj {:id :id}]}
                             :from :department
                             :where ^:pg/op[:and
                                            (if id ^:pg/op[:<> :id [:pg/param id]] [:pg/sql true])
                                            (if q [:ilike [:jsonb/->> :resource :name] [:pg/param (str "%" q "%")]] [:pg/sql true])]})]
    (format=
     (query-fn nil nil)
     ["SELECT resource || jsonb_build_object( 'id' , id ) as resource FROM department WHERE true and true"])
    (format=
     (query-fn "1" nil)
     ["SELECT resource || jsonb_build_object( 'id' , id ) as resource FROM department WHERE id <> ? and true" "1"])
    (format=
     (query-fn "1" "cyto")
     ["SELECT resource || jsonb_build_object( 'id' , id ) as resource FROM department WHERE id <> ? and resource ->> 'name' ILIKE ?" "1" "%cyto%"]))

  (format=
   {:ql/type :pg/select
    :select {:resource ^:pg/op[:|| :d.resource ^:jsonb/obj{:id :d.id
                                                           :partOf ^:pg/op[:|| :_d.resource ^:jsonb/obj{:id :_d.id}]}]}
    :from {:d :department}
    :left-join {:_d {:table :department
                     :on ^:pg/op [:= :_d.id [:jsonb/#>> :d.resource [:partOf :id]]]}}
    :where ^:pg/op [:= :d.id [:pg/param "10"]]}

   ["SELECT d.resource || jsonb_build_object( 'id' , d.id , 'partOf' , _d.resource || jsonb_build_object( 'id' , _d.id ) ) as resource FROM department d LEFT JOIN department _d ON _d.id = d.resource #>> '{partOf,id}' WHERE d.id = ?", "10"])

  (format=
   ^:pg/op[:|| :resource ^:pg/jsonb{:a 1 :b 2}]
   ["resource || '{\"a\":1,\"b\":2}'"]
   )

  (format=
   {:ql/type :pg/select
    :select {:resource ^:pg/op[:|| :resource ^:jsonb/obj{:id :id}]}
    :from :xdiagnosis
    :where [:and
            [:ilike [:pg/cast :resource ::text] [:pg/param "%10%"]]
            [:pg/sql "case when (resource->>'valid_through' is not null) then resource->>'valid_through' > 2020-10-10 else true"]
            [:pg/sql "case when (resource->>'valid_since' is not null) then resource->>'valid_since' < 2020-10-10 else true"]]
    :order-by [:resource->> :icd_10]}
   ["SELECT resource || jsonb_build_object( 'id' , id ) as resource FROM xdiagnosis WHERE ( ( resource )::text ILIKE ? AND case when (resource->>'valid_through' is not null) then resource->>'valid_through' > 2020-10-10 else true AND case when (resource->>'valid_since' is not null) then resource->>'valid_since' < 2020-10-10 else true ) ORDER BY resource->>'icd_10'" "%10%"])

  (format=
   {:ql/type :pg/update
    :update :Document
    :set {:resource
          ^:pg/op [:||
                   :resource
                   ^:pg/obj{:caseNumber [:pg/param "new-case-id"]}]}
    :where ^:pg/op [:= :id [:pg/param "doc-id"]]}
   ["UPDATE Document SET resource = resource || jsonb_build_object( 'caseNumber' , ? ) WHERE id = ?"            
    "new-case-id"
    "doc-id"])
  {:ql/type :pg/projection
   :sub-q ^:pg/sub-select{:select :* :from :tbl :limit 1}}

  "select * from billingcase where resource#>>'{location, id}' in (select id from Department where resource#>>'{part_of, id}' = 'dep-1')"

  (format=
   {:ql/type :pg/select
    :select :*
    :from :BillingCase
    :where [:in
            [:resource#>> [:location :id]]
            {:ql/type :pg/projection
             :sub-q ^:pg/sub-select{:select :id
                                    :from :Department
                                    :where ^:pg/op [:= [:resource#>> [:part_of :id]] "dep-1"]}}
            ]}
   ["SELECT * FROM BillingCase WHERE resource#>>'{location,id}' IN ( SELECT id FROM Department WHERE resource#>>'{\"part_of\",id}' = 'dep-1' ) as \"sub-q\""])

  (format=
   {:ql/type :pg/select
    :select {:resource ^:pg/op[:|| :d.resource ^:jsonb/obj{:id :d.id
                                                           :partOf ^:pg/op[:|| :_d.resource ^:jsonb/obj{:id :_d.id}]}]}
    :from {:d :department}
    :left-join {:_d {:table :department
                     :on ^:pg/op [:= :_d.id [:jsonb/#>> :d.resource [:partOf :id]]]}}
    :where ^:pg/op [:= :d.id [:pg/param "10"]]}

   ["SELECT d.resource || jsonb_build_object( 'id' , d.id , 'partOf' , _d.resource || jsonb_build_object( 'id' , _d.id ) ) as resource FROM department d LEFT JOIN department _d ON _d.id = d.resource #>> '{partOf,id}' WHERE d.id = ?", "10"])

  (format=
   {:ql/type :pg/select
    :select     {:resource ^:pg/op[:|| :e.resource ^:pg/jsonb{:id :e.id}]}
    :from       {:e :MawdEorder}
    :left-join  {:_e {:table :BillingCase
                      :on
                      ^:pg/op [:=
                               [:jsonb/->> :_e.resource :order_id]
                               :e.id]}}
    :where      ^:pg/op [:<>
                         [:jsonb/->> :e.resource :processed]
                         "full"]}
   ["SELECT e.resource || '{\"id\":\"e.id\"}' as resource FROM MawdEorder e LEFT JOIN BillingCase _e ON _e.resource ->> 'order_id' = e.id WHERE e.resource ->> 'processed' <> 'full'"])
  (format=
    [:cond
     [:= [:resource->> :date] "123"] [:jsonb/->> :e.resource :processed]
     [:= [:resource->> :date] "456"] [:jsonb/->> :e.resource :order_id]
     [:jsonb/#>> :d.resource [:partOf :id]]]
    ["( case when ( resource->>'date' = '123' ) then ( e.resource ->> 'processed' ) when ( resource->>'date' = '456' ) then ( e.resource ->> 'order_id' ) else ( d.resource #>> '{partOf,id}' ) end )"])
  (format=
    [:case [:resource->> :date]
     "123" [:jsonb/->> :e.resource :processed]
     "456" [:jsonb/->> :e.resource :order_id]
     [:jsonb/#>> :d.resource [:partOf :id]]]
    ["( case ( resource->>'date' ) when ( '123' ) then ( e.resource ->> 'processed' ) when ( '456' ) then ( e.resource ->> 'order_id' ) else ( d.resource #>> '{partOf,id}' ) end )"])


  (format=
   {:ql/type :pg/create-table
    :table-name "mytable"
    :if-not-exists true
    :unlogged true
    :columns {:id          {:type "text" :primary-key true}
              :filelds     {:type "jsonb"}
              :match_tags  {:type "text[]"}
              :dedup_tags  {:type "text[]"}}}
   ["CREATE UNLOGGED TABLE IF NOT EXISTS mytable ( id text PRIMARY KEY , filelds jsonb , match_tags text[] , dedup_tags text[] )"])

  (format=
   {:ql/type :pg/drop-table
    :table-name "mytable"
    :if-exists true}
   ["DROP TABLE IF EXISTS mytable"])




  (format=
   {:ql/type :pg/insert-select
    :into :mytable
    :select {:select {:z :z :a "a" :b :b} :from :t}}
   ["INSERT INTO mytable ( a, b, z ) ( SELECT 'a' as a , b as b , z as z FROM t )"])

  (format=
   {:ql/type :pg/insert-select
    :into "mytable"
    :select {:select {:z :z :a "a" :b :b} :from :t}}
   ["INSERT INTO mytable ( a, b, z ) ( SELECT 'a' as a , b as b , z as z FROM t )"])

  (format=
   {:ql/type :pg/insert-select
    :into :mytable
    :select {:select {:z :z :a "a" :b :b} :from :t}
    :on-conflict {:on [:id]
                  :do {:set {:a :excluded.a}
                       :where [:= 1 2]}}}

   ["INSERT INTO mytable ( a, b, z ) ( SELECT 'a' as a , b as b , z as z FROM t ) ON CONFLICT ( id ) DO UPDATE SET a = excluded.a WHERE 1 = 2"])

  (format=
   {:ql/type :pg/insert-select
    :into :mytable
    :select {:select {:z :z :a "a" :b :b} :from :t}
    :on-conflict {:on [:id] :do :nothing}}
   ["INSERT INTO mytable ( a, b, z ) ( SELECT 'a' as a , b as b , z as z FROM t ) ON CONFLICT ( id ) DO NOTHING"])

  (format=
   {:ql/type :pg/insert-select
    :into :mytable
    :select {:select {:z :z :a "a" :b :b} :from :t}
    :returning :*}
   ["INSERT INTO mytable ( a, b, z ) ( SELECT 'a' as a , b as b , z as z FROM t ) RETURNING *"])

  (format=
    {:ql/type :pg/cte
     :with {:_ctp {:ql/type :pg/select
                   :select {:a 1
                            :b 2}}
            :_ctp2 {:ql/type :pg/select
                    :select {:a 1
                             :b 2}}}
     :select {:ql/type :pg/select
              :select :*
              :from :_ctp}}
    ["WITH _ctp AS ( SELECT 1 as a , 2 as b ) , _ctp2 AS ( SELECT 1 as a , 2 as b ) SELECT * FROM _ctp"])

  (format=
   {:ql/type :pg/cte-recursive
    :with {:_ctp {:ql/type :pg/select
                  :select {:a 1
                           :b 2}}
           :_ctp2 {:ql/type :pg/select
                  :select {:a 1
                           :b 2}}}
    :select {:ql/type :pg/select
             :select :*
             :from :_ctp}}
   ["WITH RECURSIVE _ctp AS ( SELECT 1 as a , 2 as b ) , _ctp2 AS ( SELECT 1 as a , 2 as b ) SELECT * FROM _ctp"])


  (format= 
   [:|| :resource ^:pg/obj{:a 1}]
   ["( resource ) || ( jsonb_build_object( 'a' , 1 ) )"])

  (format= 
   [:|| "a" :b "c"]
   ["( 'a' ) || ( b ) || ( 'c' )"])

  (format= 
   [:pg/jsonb_set :resource [:a :b :c] "value" true]
   ["jsonb_set( resource , '{a,b,c}' , 'value' , true )"])

  (format= 
   [:pg/jsonb_set :resource [:a :b :c] [:pg/param "pid"]]
   ["jsonb_set( resource , '{a,b,c}' , ? )" "pid"]
   )

  (format= 
   [:pg/jsonb_string "String"]
   ["(jsonb_build_object('s', 'String' )->'s')"])

  (format= 
   [:pg/row_to_json :p.*]
   ["row_to_json( p.* )"])


  (format= 
   [:<> :a :b]
   ["a <> b"])

  (format= 
   [:pg/jsonb {:a 1 :b 2}]
   ["'{\"a\":1,\"b\":2}'"])

  (format= 
   [:pg/jsonb_strip_nulls :obj]
   ["jsonb_strip_nulls( obj )"])

  ;; (format= 
  ;;  {:select ^:pg/jsonb{:a 1 :b 2}}
  ;;  ["'{\"a\":1,\"b\":2}'"])


  (format= {:ql/type :pg/update
            :update :healthplan
            :set {:a :tbl.b}
            :from {:tbl :some_table}
            :where [:= :healthplan.id "ups"]}
           ["UPDATE healthplan SET a = tbl.b FROM some_table tbl WHERE healthplan.id = 'ups'"])

  (format= {:ql/type :pg/update
            :update :healthplan
            :set {:a :tbl.b}
            :from {:tbl :some_table}
            :where [:= :healthplan.id "ups"]
            :returning :*}
           ["UPDATE healthplan SET a = tbl.b FROM some_table tbl WHERE healthplan.id = 'ups' RETURNING *"])

  (format= {:ql/type :pg/insert
            :into :healthplan
            :value {:a 1 :b :x :c "str"}
            :returning :*}
           ["INSERT INTO healthplan ( a, b, c ) VALUES ( 1 , x , 'str' ) RETURNING *"])

  (format= [:resource|| ^:pg/obj{:a 1 :b 2}]
           ["resource || jsonb_build_object( 'a' , 1 , 'b' , 2 )"])


  (format= 
   [:min :col]
   ["min( col )"])

  (format= 
   [:jsonb_agg :col]
   ["jsonb_agg( col )"])

  (format=
   {:ql/type :pg/select
    :select  :id
    :from    {:b :BillingCase}
    :where   [:and
              [:= [:jsonb/->> :b.resource :status] "client.draft"]
              [:= [:jsonb/#>> :b.resource [:billing_master :id]] "132"]
              ^:pg/op[:<=
                      [:pg/cast [:jsonb/->> :b.resource :date] :date]
                      [:pg/cast "2020-11-03" :date]]]
    :for :update}
   ["SELECT id FROM BillingCase b WHERE ( b.resource ->> 'status' = 'client.draft' AND b.resource #>> '{\"billing_master\",id}' = '132' AND ( b.resource ->> 'date' )::date <= ( '2020-11-03' )::date ) FOR update"])


  (format= 
   [:->> :d.resource :key]
   ["( d.resource ->> 'key' )"])

  (format= 
   [:#>> :d.resource [:a :b]]
   ["( d.resource #>> '{a,b}' )"])

  (format= 
   [:pg/desc [:resource-> :date]]
   ["resource->'date' DESC"])

  (format= 
   [:pg/asc [:resource-> :date]]
   ["resource->'date' ASC"])

  (format=
   [:pg/parens [:#>> :d.resource [:a :b]]]
   ["( ( d.resource #>> '{a,b}' ) )"])

  (format=
   [:pg/nulls-last [:resource-> :date]]
   ["resource->'date' NULLS LAST"])
  
  (format=
   [:pg/nulls-first [:resource-> :date]]
   ["resource->'date' NULLS FIRST"])

  (format=
   [:+ 1 1]
   ["( 1 ) + ( 1 )"])

  (format=
   [:pg/list [:pg/desc [:resource-> :date]] [:resource-> :status]]
   ["resource->'date' DESC , resource->'status'"])

  (format=
   (let [unit [:pg/coalesce [:int [:->> :b.resource :units]] 1]]
     {:ql/type :pg/obj
      :id :b.id
      :case {:ql/type :pg/obj :id :b.id :resourceType "BillingCase"}
      :cpt [:#>> :b.resource [:codes :cpt :code]]
      :ordering_organization_name [:#>> :ordering_org.resource [:name]]
      :specimen [:#>> :b.resource [:codes :cpt :display]]
      :date [:->> :b.resource :date]
      :unit unit
      :order_id [:->> :b.resource :order_id]
      :price [:numeric [:->> :b.resource :price]]
      :patient_mrn [:pg/nth ^:pg/fn[:knife_extract_text :pt.resource [:pg/jsonb [[:identifier {:system "mrn"} :value]]]] 1]
      :charge [:* [:numeric [:->> :p.resource :price]] unit]
      :ref_prov_npi  [:#>> :b.resource [:referring_provider :id]]
      :ref_prov_name [:#>> :b.resource [:referring_provider :display]]
      :patient_name [:#>> :b.resource [:patient :display]]})
   
   ["jsonb_build_object( 'date' , ( b.resource ->> 'date' ) , 'unit' , COALESCE( ( ( b.resource ->> 'units' ) )::int , 1 ) , 'specimen' , ( b.resource #>> '{codes,cpt,display}' ) , 'patient_mrn' , ( knife_extract_text( pt.resource , '[[\"identifier\",{\"system\":\"mrn\"},\"value\"]]' ) )[ 1 ] , 'ref_prov_name' , ( b.resource #>> '{\"referring_provider\",display}' ) , 'charge' , ( ( ( p.resource ->> 'price' ) )::numeric ) * ( COALESCE( ( ( b.resource ->> 'units' ) )::int , 1 ) ) , 'ordering_organization_name' , ( ordering_org.resource #>> '{name}' ) , 'order_id' , ( b.resource ->> 'order_id' ) , 'patient_name' , ( b.resource #>> '{patient,display}' ) , 'id' , b.id , 'case' , jsonb_build_object( 'id' , b.id , 'resourceType' , 'BillingCase' ) , 'price' , ( ( b.resource ->> 'price' ) )::numeric , 'cpt' , ( b.resource #>> '{codes,cpt,code}' ) , 'ref_prov_npi' , ( b.resource #>> '{\"referring_provider\",id}' ) )"]
   )

  (format=
   {:ql/type :pg/select
    :select 1
    :union {:test {:ql/type :pg/sub-select
                   :select "2"}
            :best {:ql/type :pg/sub-select
                   :select "5"}}}
   ["SELECT 1 UNION ( SELECT '2' )  /* test */  UNION ( SELECT '5' )  /* best */ "])

  (format=
    {:ql/type :pg/select
     :select 1
     :union-all {:test {:ql/type :pg/sub-select
                        :select "2"}
                 :best {:ql/type :pg/sub-select
                        :select "5"}}}
    ["SELECT 1 UNION ALL ( SELECT '2' )  /* test */  UNION ALL ( SELECT '5' )  /* best */ "])

  (format=
   {:ql/type :pg/select
    :select-distinct :test
    :from :best}
   ["SELECT DISTINCT test FROM best"])


  (format=
   {:ql/type :pg/select
    :select-distinct :test
    :from :best}
   ["SELECT DISTINCT test FROM best"])

  (format=
   {:ql/type :pg/select
    :select [:distinct :id]
    :from :best}
   ["SELECT DISTINCT( id ) FROM best"])

  (format=
    {:ql/type :pg/select
     :select
     ^{:pg/projection {:distinct-on [:id :txid]}}
     {:id       :id
      :resource :resource
      :txid     :txid}
     :from :best}
    ["SELECT DISTINCT ON ( id , txid ) id as id , resource as resource , txid as txid FROM best"])

  (format=
    {:ql/type :pg/select
     :select
     ^{:pg/projection {:distinct-on [:id]}}
     {:id       :id
      :resource :resource
      :txid     :txid}
     :from :best}
    ["SELECT DISTINCT ON ( id ) id as id , resource as resource , txid as txid FROM best"])

  (format=
    {:ql/type :pg/select
     :select
     ^{:pg/projection {:distinct-on [[:#>> :resource [:id]]]}}
     {:id       :id
      :resource :resource
      :txid     :txid}
     :from :best}
    ["SELECT DISTINCT ON ( ( resource #>> '{id}' ) ) id as id , resource as resource , txid as txid FROM best"])

  (format=
    {:ql/type :pg/select
     :select
     ^{:pg/projection :distinct}
     {:id       :id
      :resource :resource
      :txid     :txid}
     :from :best}
    ["SELECT DISTINCT id as id , resource as resource , txid as txid FROM best"])

  (format=
    {:ql/type :pg/select
     :select
     ^{:pg/projection :all}
     {:id       :id
      :resource :resource
      :txid     :txid}
     :from :best}
    ["SELECT ALL id as id , resource as resource , txid as txid FROM best"])


  (format=
   {:ql/type :pg/select
    :select :*
    :from [:pg/identifier "best"]}
   ["SELECT * FROM best"])

  (format=
   {:ql/type :pg/select
    :select :*
    :from :Patient
    :where [:= [:pg/extract
                [:pg/identifier "YEAR"]
                [:pg/cast [:-> :resource :birthDate] :timestamp]]
            "1980"]}
   ["SELECT * FROM Patient WHERE EXTRACT( YEAR from ( ( resource -> 'birthDate' ) )::timestamp ) = '1980'"])


  (format=
   {:ql/type :pg/select
    :select  :*
    :from :Patient
    :order-by {[:||
                [:#> :resource [:name 0 :family]]
                [:#> :resource [:name 0 :given 0]]] :asc
               :id :desc}}
   ["SELECT * FROM Patient ORDER BY ( ( resource #> '{name,0,family}' ) ) || ( ( resource #> '{name,0,given,0}' ) ) asc , id desc"])

  (format=
   {:ql/type :pg/select
    :select  :*
    :from :Patient
    :order-by [:||
               [:#> :resource [:name 0 :family]]
               [:#> :resource [:name 0 :given 0]]]}
   ["SELECT * FROM Patient ORDER BY ( ( resource #> '{name,0,family}' ) ) || ( ( resource #> '{name,0,given,0}' ) )"])

  (format=
   {:ql/type :pg/select
    :select  :*
    :from :Patient
    :order-by {:id :desc}}
   ["SELECT * FROM Patient ORDER BY id desc"])


  (format=
    {:ql/type :pg/values
     :values [1 2 3]}
    ["VALUES ( 1 ) , ( 2 ) , ( 3 )"])

  (format=
    {:ql/type :pg/values
     :keys [:foo :baz]
     :values [{:foo 1
               :baz 2}
              {:foo 3
               :baz 4}]}
    ["VALUES ( 1 , 2 ) , ( 3 , 4 )"])

  (format=
    {:select :*
     :from {:_values {:ql/type :pg/values
                      :keys [:foo :baz]
                      :values [{:foo 1
                                :baz 2}
                               {:foo 3
                                :baz 4}]}}}
    ["SELECT * FROM ( VALUES ( 1 , 2 ) , ( 3 , 4 ) ) _values ( foo , baz )"])

  (format=
   {:ql/type :pg/insert-many
    :into    :conceptmaprule
    :values   {:keys [:id :txid :resource :status]
               :values [{:id 1 :resource "1" :status "ready"}
                        {:id 2 :status "failure"}]}
    :returning :*}
   ["INSERT INTO conceptmaprule ( id, txid, resource, status ) VALUES ( 1 , NULL , '1' , 'ready' ) , ( 2 , NULL , NULL , 'failure' ) RETURNING *"])

  (format=
   {:ql/type :pg/select
    :select  :*
    :from    [:pg/escape-ident "group"]}
   ["SELECT * FROM \"group\""])


  (format=
   [:in :id [:pg/inplace-params-list ["Hello'y World'y"]]]
   ["id IN ( 'Hello''y World''y' )"])


  (format=
    [:pg/jsonb-path-query-array :resource [:pg/cast [:pg/param "$.foo[*]"] :jsonpath]]
    ["jsonb_path_query_array( resource , ( ? )::jsonpath )"
     "$.foo[*]"])


  (format=
    [:pg/call :foobar]
    ["foobar( )"])

  (format=
    [:pg/call :foobar :resource]
    ["foobar( resource )"])

  (format=
    [:pg/call :foobar :resource "baz" "quux"]
    ["foobar( resource , 'baz' , 'quux' )"])

  (format=
   {:ql/type :pg/select
    :select [:- :price :diff]
    :from :my_db}
   ["SELECT ( price ) - ( diff ) FROM my_db"])

  (format=
    [:pg/build-sql-str ["" :resource "#>>" "'{foo, bar}' =" [:pg/param "baz"]]]
    [" resource#>>'{foo, bar}' = ?" "baz"])

  (format=
    {:select [:pg/build-sql-str ["ARRAY[" :resource "#>>" "'{foo, bar}' =" [:pg/param "baz"] "]"]]}
    ["SELECT ARRAY[ resource#>>'{foo, bar}' = ? ]" "baz"])

  (format=
   {:ql/type :pg/create-extension
    :name "jsonknife"
    :schema "ext"
    :if-not-exists true}
   ["CREATE EXTENSION IF NOT EXISTS jsonknife SCHEMA ext"])

  (format=
   {:ql/type :pg/create-extension
    :name "jsonknife"
    :schema "ext"}
   ["CREATE EXTENSION jsonknife SCHEMA ext"])

  (format=
   {:ql/type :pg/create-extension
    :name "jsonknife"
    :if-not-exists true}
   ["CREATE EXTENSION IF NOT EXISTS jsonknife"])

  (format=
   {:ql/type :pg/create-extension
    :name "jsonknife"}
   ["CREATE EXTENSION jsonknife"])

  (format=
   {:ql/type :pg/create-extension
    :name "jsonknife"
    :schema "ext"
    :cascade true
    :version "1337"}
   ["CREATE EXTENSION jsonknife SCHEMA ext VERSION 1337 CASCADE"])

  (format=
   {:ql/type :pg/primary-key
    :table "table1"
    :constraint "table2_pkey"
    :columns [:a :b]}
   ["ALTER TABLE table1 ADD CONSTRAINT table2_pkey PRIMARY KEY ( a , b )"])

  (format=
   {:ql/type :pg/create-table-as
    :table "table1"
    :select {:ql/type :pg/select
             :select 1}}
   ["CREATE TABLE table1 AS SELECT 1"])

  (format=
    {:ql/type :pg/create-table-as
     :table "table1"
     :if-not-exists true
     :select {:ql/type :pg/select
              :select 1}}
    ["CREATE TABLE IF NOT EXISTS table1 AS SELECT 1"])

  (format=
    {:ql/type :pg/index
     :index   :sdl_src_dst
     :unique  true
     :on      :sdl_src_dst
     :expr    [:src :dst]
     }
    ["CREATE UNIQUE INDEX IF NOT EXISTS sdl_src_dst ON sdl_src_dst ( ( src ) , ( dst ) )"])


  )
