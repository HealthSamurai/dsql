(ns a-clojure-intro
  (:require [b-clojure-db]))

"Clojure is functional lisp hosted on jvm and js"

"Clojure is your dynamic language --"

"
- syntax (lisp)
- mutability (pesistent datastructures)
- objects/classes (generic data)
"

"
Reading LISP: S-EXPRESSION

(OP ARG, ARG)
"

"In c like lang you call function:
subs('string',1,4)
"
(subs "string" 1 4)

"
operators:
1 + 2 => + 1 2 => (+ 1 2)
"
(+ 1 2 3 4)

"
 // statements
 var x = 2;
 if ( x = 1 ) { return 'match' } else { return 'nop' }
"
(let [x 2]
  (if (= x 1) "match" "nop"))

"
//fn definition

function myfn(a, b) { return a + b }

"
(defn myfn [a b] (+ a b))

(myfn 1 3)


"
 clj is hosted language
 uses host primitives primitives
"

(type 1)

(type 1.1)

(type true)

(type "str")

"
SPECIAL STRINGS - keys and symbols
"

"Keyword is a string to be a key in a map"
(type :mykey)

"Symbol is a string to be name of function"
(type 'symbol)


"
Datastructures:
* vector
* hash-map
* set
* list
"

" Vector:"
(def v [1 2, 3 4])

(nth v 3)

(conj v 5)

(rest v)

(butlast v)

"Lisp list for code"

(def l (or '(1 2 3) (list 1 2 3)))

(nth l 2)

(conj l 4)

(rest l)

(list '+ 1 2)

(eval (list '+ 1 2))

"sets"

(def s #{1 2 3})

(conj s 4)

(conj s 3)

(disj s 3)


"hash-map: dict, object "

(def m
  {:a 1 :b "2"})

(assoc m :c 3)

(dissoc m :b)

(merge m {:c 3 :d 4})


"immutability: copy semantic, but efficient and cheap"

(assoc m :c 3)

m

"It is better to have 100 functions operate on one data structure
 than to have 10 functions operate on 10 data structures.

from Alan Perlis' Epigrams on Programming (1982)"

"Clojure: 4 datastructures & ~500 functions & ~100 macro"

(->> (range 100)
     (filter odd?)
     (take 20))

;; macroexpand
(take 20 (filter odd? (range 100)))

(-> {:name {:given "x"}
     :age 40}
    (assoc :address {:city "SBb"})
    (dissoc :age)
    (update-in [:name :given] str "_postfix"))


"REPL: ns & vars"

"this is macro"
(defn somevar [x]
  (str "Hello " x))

"=>"
(def somevar (fn [x] (str "Hello " x)))

"somevar is var - mutable var which refers immutable value"
(type #'somevar)
(var-get #'somevar)

(somevar "Ivan")

"lets redefine and reload function"
(defn somevar [x]
  (str "Привет " x))

(somevar "Ivan")

"if fn referenced in other fn it will be reloaded"

(defn other-fn [x]
  (str (somevar x) "!"))

(other-fn "Ivan")

"redefine"
(defn somevar [x] (str "Hello " x))

(other-fn "Ivan")


" Macro:
  code is just a data
"
(->> '(defn somevar [x] (str "Привет " x))
     (map (fn [x] (str "*" x " : " (type x)))))

(conj '(somevar  [x] (str "Привет " x)) 'defn)

"
In emacs
  emacs -> send code -> nrepl server -> execute -> response
"

 b-clojure-db/start
