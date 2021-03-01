(ns a-clojure-intro)

;; S-EXPRESSION and functions

;; c like functions to lisp:
;; myfn(arg, arg, arg) => (myfn arg arg arg)
;; function call

(+ 1 2 3 4)

(str "a" "b" "c")

;; clj is hosted language
;; uses host primitives primitives

(type 1)
;;=>java.lang.Long

(type 1.1)
;; => java.lang.Double

(type true)
;; java.lang.Boolean


(type "str")
;; java.lang.String

;; SPECIAL STRINGS
;; special string to be a key in a map
(type :mykey)
;;=> clojure.lang.Keyword

;; special string to name functions and other objects
(type 'symbol)
;;=> clojure.lang.Symbol


;; datastructures

;; vector

(def v [1 2 3 4])

(nth v 3)

(conj v 5)

(rest v)

(butlast v)

;; hash-map

(def m {:a 1 :b "2"})

(assoc m :c 3)

(dissoc m :b)

(merge m {:c 3 :d 4})

;; sets

(def s #{1 2 3})

(conj s 4)

(conj s 3)

(disj s 3)

;; lists

(def l '(1 2 3))

(nth l 2)

(conj l 4)

(rest l)

;; all structures are immutable
;; immutability is cheap!!!

(def m {:a 1 :b "2"})

(assoc m :c 3)

m

;; function definition
;; function myfn (x, y, z) { return str(x, y, z); }

(defn myfn [x y z] ;;< args vector
  (str x y z))

;; function application

(myfn 1 2 3)


;; 1 datastructure + 100 functions

(->> (range 100)
     (filter odd?)
     (take 20))

(-> {:name {:given "x"}
     :age 40}
    (assoc :address {:city "SBb"})
    (dissoc :age)
    (update-in [:name :given] str "_postfix"))


;; REPL ns & vars; reload

;; Clojure app consists of namespaces and vars

;; (ns myns)


(defn somevar [x]
  (str "Hello " x))

(def somevar (fn [x] (str "Hello " x)))

(somevar "Ivan")

;; redefine fuction
(defn somevar [x]
  (str "Привет " x))

(somevar "Ivan")

(type #'somevar)

(var-get #'somevar)

(defn other-fn [x]
  (str (somevar x) "!"))

(other-fn "Ivan")

(defn somevar [x]
  (str "Hello " x))

(other-fn "Ivan")

(defn somevar [x]
  (str "Привет " x))

;; code is just a data
(->> '(defn somevar [x] (str "Привет " x))
     (map (fn [x] (println "*" x "-" (type x)))))

(conj '(somevar  [x] (str "Привет " x)) 'defn)

;; emacs -> send code -> nrepl -> response

;; multimethod & polymorphism


