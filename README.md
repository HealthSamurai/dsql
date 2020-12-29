[![Build Status](https://travis-ci.org/HealthSamurai/dsql.svg?branch=master)](https://travis-ci.org/HealthSamurai/dsql)
# dsql - datastructure query language

Previous experiment - https://github.com/HealthSamurai/ql

Composable and extendable version of honeysql, 
implemented as pure functions.

Query is represented as datastructure.

```clj
{:select {:resource ^:pg/op[:|| :resource ^:pg/obj{:id :id :resourceType "Patient"}]}
 :from {:pt :patient}
 :where {:match ^:pg/op[:&& ^:pg/fn[:knife_extract_text :resource ^:pg/jsonb[["match"]]]
                  [:pg/array-param :text ["a-1" "b-2"]]]}
    :limit 10}
```

Each node has a type, which is handled by to-sql type multimethod

```clj
(defmethod ql/to-sql
  :<key>
  [acc opts node]

  )
```

Dispatch function for to-sql looks for 

1. :ql/type in meta - `^{:ql/type ..}{....}`
2. :ql/type as key in map - `{:ql/type ...}`
3. call resolver function from opts
4. first element in vector - `[:type ...]`
5. type of node

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

## More

* See intro seminar in Russian - https://storage.googleapis.com/samurai-files/honey-ql-seminar.mp4
