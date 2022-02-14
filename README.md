# dsql - datastructure query language

![test badge](https://github.com/HealthSamurai/dsql/actions/workflows/main.yml/badge.svg)

Pure Clojure SQL constructor from data structures.

## Motivation

[honeysql](https://github.com/seancorfield/honeysql) is an awesome idea, but....

- composability - it should be easy compose expressions into sql query
- extendibility - to extend - just add one multi-method ql.method/to-sql
- pure functional implementation - sql generation as a tree reduction
- implicit params - manage params style jdbc, postgres, inline
- use namespaced keywords
- validation by clojure.spec
- prefer hash-map over vector (support both, where vector is just sugar)
- dsl's on top of it

Previous experiment is [HealthSamurai/ql](https://github.com/HealthSamurai/ql)

Composable and extendable version of honeysql, 
implemented as pure functions.

## Resources

Support:
- Join [#health-samurai on Clojurians Slack](https://clojurians.slack.com/archives/C03290L32QP) (grab invite at [http://clojurians.net/](http://clojurians.net/))

Talks:
- [Introdution talk](https://storage.googleapis.com/samurai-files/honey-ql-seminar.mp4) by @niquola. (Russian)
- [how we wrote dsql](https://www.youtube.com/watch?v=oqlddGlTJOM) by @apostaat (Russian)


## Usage

This section provides you few expamples: how to use dsql for constructing your SQL and how to extend the standard dsql.

Dsql supports different dialects of SQL, and not only SQL (PromQL support is in progress). For PostgreSQL dialect you have to use format function from `dsql.pg` namespace

``` clj
(require '[dsql.pg :as dsql])
```

Dsql only transforms your datastructure into jdbc-compatible, parameterized SQL. It doesn't execute it. 

``` clj
(dsql/format
 {:ql/type :pg/select ;; :pg/select is a default :ql/type for dsql.pg/format
  :select :*
  :from :product})
;; => ["SELECT * FROM product"]
```

More real world example:

```clj
(dsql.pg/format
 {:select {:resource ^:pg/op[:|| :resource ^:pg/obj{:id :id :resourceType "Patient"}]}
  :from {:pt :patient}
  :where {:match ^:pg/op[:&& ^:pg/fn[:knife_extract_text :resource ^:pg/jsonb[["match"]]]
                         [:pg/array-param :text ["a-1" "b-2"]]]}
  :limit 10})
;; => ["SELECT resource || jsonb_build_object( 'id' , id , 'resourceType' , 'Patient' ) as resource FROM patient pt WHERE /*match*/ knife_extract_text( resource , '[[\"match\"]]' ) && ?::text[] LIMIT 10"
;;     "{\"a-1\",\"b-2\"}"]
```

## Extending dsql syntax

Dsql recursively goes through your datastructure and calls `dsql.core/to-sql` multimethod for each node. And you may specify your own constructor

```clj
(defmethod ql/to-sql
  :<key>
  [acc opts node]

  )
```

Dispatch function for to-sql looks for 

1. :ql/type as key in map - `{:ql/type :node/type}`
2. :ql/type in meta - `^{:ql/type :node/type}{....}`or ^{:ql/type ..}[,,,]
3. call resolver function from opts
4. first element in vector - `[:node/type ...]`
5. type of java object. e.g. `java.lang.String`, `clojure.lang.Keyword`.

Each impl of to-sql accumulate sql string in acc vector.

```clj
["select" "*" "from" "table" "where" "id" "=" ["?" "id"]]

(conj acc "current_timestamp()")

(conj acc ['?' 'param'])
```

Params are represented as vector in accumulator:

Function `format` call recursive to-sql and join acc string extracting params

## Example

```clj

(ql/format
  {:select ^:pg/fn[:function "arg" {:select :* from :user :limit 1}]})
  
;;=> SELECT function('arg', (select * from user limit 1))  

```


## License

Distributed under the Eclipse Public License.
