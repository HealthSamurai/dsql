(ns dsql.prometheus-test
  (:require [dsql.prometheus :as sut]
            [clojure.test :refer [deftest is testing]]
            [dsql.core :as ql]))

(defmacro format= [ctx q patt]
  `(let [res# (sut/format ~ctx ~q)]
     (is (= ~patt res#))
     res#))

(deftest test-dsql-prometheus
  (format= {}
           [:sum [:rate [:metric_name {:match "value"
                                       :not [:! "value"]
                                       :regex [:# "a|b|c"]
                                       :not-regex [:#! "get"]}
                         [5 :m]]]]
           "sum(rate(metric_name{match=\"value\",not!=\"value\",regex=~\"a|b|c\",not-regex!~\"get\"}[5m]))")

  (format= {}
           [:/ [:metric1 [5 :m]] [:metric2 [5 :m]]]
          "(metric1[5m]/metric2[5m])")

  (format= {}
           [:sum [:metric1] {:by [:le]}]
           "sum(metric1) by (le)")

  (format= {:params {'range [5 :m]}}
           [:/ [:metric1 'range] [:metric2 'range]]
           "(metric1[5m]/metric2[5m])")

  (format= {:params {}}
           [:/ [:metric1 '?range] [:metric2 '?range]]
           "(metric1<ERROR: could not resolve ?range>/metric2<ERROR: could not resolve ?range>)")

  (format= {}
           [:!=
            [:sum [:kube_deployment_status_replicas_unavailable] {:by [:namespace :deployment]}]
            0]
           "(sum(kube_deployment_status_replicas_unavailable) by (namespace,deployment)!=0)")

  (format= {}
           [:>
            [:*
             [:/
              [:container_fs_usage_bytes {:device [:# "^/dev/[sv]d[a-z][1]$"] :id "/"}]
              [:container_fs_limit_bytes {:device [:# "^/dev/[sv]d[a-z][1]$"] :id "/"}]]
             100]
            60]
           "(((container_fs_usage_bytes{device=~\"^/dev/[sv]d[a-z][1]$\",id=\"/\"}/container_fs_limit_bytes{device=~\"^/dev/[sv]d[a-z][1]$\",id=\"/\"})*100)>60)")

  (format= {}
           [:>
            [:*
             100
             [:/
              [:kubelet_volume_stats_used_bytes]
              [:kubelet_volume_stats_capacity_bytes]]]
            60]
           "((100*(kubelet_volume_stats_used_bytes/kubelet_volume_stats_capacity_bytes))>60)")


  (format= {}
           [:/ [:rate [:pg_requests_total [1 :h]]]
            [:+ [:rate [:pg_updates_total [1 :h]]] [:rate [:pg_inserts_total [1 :h]]] [:rate [:pg_deletes_total [1 :h]]]]]
           "(rate(pg_requests_total[1h])/(rate(pg_updates_total[1h])+rate(pg_inserts_total[1h])+rate(pg_deletes_total[1h])))")


  )
