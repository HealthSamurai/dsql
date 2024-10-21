(ns dsql.pg
  (:require [dsql.core :as ql]
            [jsonista.core :as json]
            [clojure.string :as str])
  (:import [com.fasterxml.jackson.databind.node  ObjectNode ArrayNode TextNode IntNode BooleanNode DoubleNode LongNode]
           [com.fasterxml.jackson.databind       JsonNode ObjectMapper]
           [java.util     Iterator])
  (:refer-clojure :exclude [format]))

(def keywords
  #{:A :ABORT :ABS :ABSENT :ABSOLUTE :ACCESS :ACCORDING :ACOS
    :ACTION :ADA :ADD :ADMIN :AFTER :AGGREGATE :ALL :ALLOCATE
    :ALSO :ALTER :ALWAYS :ANALYSE :ANALYZE :AND :ANY :APPLICATION
    :ARE :ARRAY :ARRAY_AGG :ARRAY_MAX_CARDINALITY :AS :ASC
    :ASENSITIVE :ASIN :ASSERTION :ASSIGNMENT :ASYMMETRIC :AT
    :ATAN :ATOMIC :ATTACH :ATTRIBUTE :ATTRIBUTES :AUTHORIZATION
    :AVG :BACKWARD :BASE64 :BEFORE :BEGIN :BEGIN_FRAME :BEGIN_PARTITION
    :BERNOULLI :BETWEEN :BIGINT :BINARY :BIT :BIT_LENGTH :BLOB
    :BLOCKED :BOM :BOOLEAN :BOTH :BREADTH :BY :C :CACHE :CALL
    :CALLED :CARDINALITY :CASCADE :CASCADED :CASE :CAST :CATALOG
    :CATALOG_NAME :CEIL :CEILING :CHAIN :CHAINING :CHAR :CHARACTER
    :CHARACTERISTICS :CHARACTERS :CHARACTER_LENGTH :CHARACTER_SET_CATALOG
    :CHARACTER_SET_NAME :CHARACTER_SET_SCHEMA :CHAR_LENGTH :CHECK
    :CHECKPOINT :CLASS :CLASSIFIER :CLASS_ORIGIN :CLOB :CLOSE :CLUSTER
    :COALESCE :COBOL :COLLATE :COLLATION :COLLATION_CATALOG :COLLATION_NAME
    :COLLATION_SCHEMA :COLLECT :COLUMN :COLUMNS :COLUMN_NAME :COMMAND_FUNCTION
    :COMMAND_FUNCTION_CODE :COMMENT :COMMENTS :COMMIT :COMMITTED :CONCURRENTLY
    :CONDITION :CONDITIONAL :CONDITION_NUMBER :CONFIGURATION :CONFLICT :CONNECT
    :CONNECTION :CONNECTION_NAME :CONSTRAINT :CONSTRAINTS :CONSTRAINT_CATALOG :CONSTRAINT_NAME
    :CONSTRAINT_SCHEMA :CONSTRUCTOR :CONTAINS :CONTENT :CONTINUE :CONTROL
    :CONVERSION :CONVERT :COPY :CORR :CORRESPONDING :COS :COSH :COST :COUNT :COVAR_POP :COVAR_SAMP
    :CREATE :CROSS :CSV :CUBE :CUME_DIST :CURRENT :CURRENT_CATALOG :CURRENT_DATE :CURRENT_DEFAULT_TRANSFORM_GROUP :CURRENT_PATH
    :CURRENT_ROLE :CURRENT_ROW :CURRENT_SCHEMA :CURRENT_TIME :CURRENT_TIMESTAMP :CURRENT_TRANSFORM_GROUP_FOR_TYPE
    :CURRENT_USER :CURSOR :CURSOR_NAME :CYCLE :DATA :DATABASE :DATALINK :DATE :DATETIME_INTERVAL_CODE
    :DATETIME_INTERVAL_PRECISION :DAY :DB :DEALLOCATE :DEC :DECFLOAT :DECIMAL :DECLARE :DEFAULT
    :DEFAULTS :DEFERRABLE :DEFERRED :DEFINE :DEFINED :DEFINER :DEGREE :DELETE :DELIMITER
    :DELIMITERS :DENSE_RANK :DEPENDS :DEPTH :DEREF :DERIVED :DESC :DESCRIBE :DESCRIPTOR :DETACH
    :DETERMINISTIC :DIAGNOSTICS :DICTIONARY :DISABLE :DISCARD :DISCONNECT :DISPATCH :DISTINCT :DLNEWCOPY
    :DLPREVIOUSCOPY :DLURLCOMPLETE :DLURLCOMPLETEONLY :DLURLCOMPLETEWRITE :DLURLPATH :DLURLPATHONLY :DLURLPATHWRITE :DLURLSCHEME :DLURLSERVER :DLVALUE
    :DO :DOCUMENT :DOMAIN :DOUBLE :DROP :DYNAMIC :DYNAMIC_FUNCTION :DYNAMIC_FUNCTION_CODE :EACH
    :ELEMENT :ELSE :EMPTY :ENABLE :ENCODING :ENCRYPTED :END :END-EXEC :END_FRAME :END_PARTITION :ENFORCED :ENUM :EQUALS :ERROR
    :ESCAPE :EVENT :EVERY :EXCEPT :EXCEPTION :EXCLUDE :EXCLUDING :EXCLUSIVE :EXEC :EXECUTE :EXISTS :EXP :EXPLAIN :EXPRESSION :EXTENSION
    :EXTERNAL :EXTRACT :FALSE :FAMILY :FETCH :FILE :FILTER :FINAL :FINISH :FIRST :FIRST_VALUE :FLAG :FLOAT
    :FLOOR :FOLLOWING :FOR :FORCE :FOREIGN :FORMAT :FORTRAN :FORWARD :FOUND :FRAME_ROW :FREE :FREEZE :FROM :FS
    :FULFILL :FULL :FUNCTION :FUNCTIONS :FUSION :G :GENERAL :GENERATED :GET :GLOBAL :GO
    :GOTO :GRANT :GRANTED :GREATEST :GROUP :GROUPING :GROUPS :HANDLER :HAVING :HEADER :HEX :HIERARCHY :HOLD :HOUR
    :ID :IDENTITY :IF :IGNORE :ILIKE :IMMEDIATE :IMMEDIATELY :IMMUTABLE :IMPLEMENTATION :IMPLICIT :IMPORT :IN :INCLUDE :INCLUDING :INCREMENT
    :INDENT :INDEX :INDEXES :INDICATOR :INFINITELY :INHERIT :INHERITS :INITIAL :INITIALLY :INLINE
    :INNER :INOUT :INPUT :INSENSITIVE :INSERT :INSTANCE :INSTANTIABLE :INSTEAD :INT :INTEGER :INTEGRITY :INTERSECT :INTERSECTION :INTERVAL
    :INTO :INVOKER :IS :ISNULL :ISOLATION :JOIN :JSON :JSON_ARRAY :JSON_ARRAYAGG
    :JSON_EXISTS :JSON_OBJECT :JSON_OBJECTAGG :JSON_QUERY :JSON_TABLE :JSON_TABLE_PRIMITIVE :JSON_VALUE :K :KEEP :KEY :KEYS
    :KEY_MEMBER :KEY_TYPE :LABEL :LAG :LANGUAGE :LARGE :LAST :LAST_VALUE :LATERAL :LEAD :LEADING
    :LEAKPROOF :LEAST :LEFT :LENGTH :LEVEL :LIBRARY :LIKE :LIKE_REGEX :LIMIT :LINK :LISTAGG :LISTEN
    :LN :LOAD :LOCAL :LOCALTIME :LOCALTIMESTAMP :LOCATION :LOCATOR :LOCK :LOCKED :LOG :LOG10
    :LOGGED :LOWER :M :MAP :MAPPING :MATCH :MATCHED :MATCHES :MATCH_NUMBER :MATCH_RECOGNIZE :MATERIALIZED :MAX :MAXVALUE
    :MEASURES :MEMBER :MERGE :MESSAGE_LENGTH :MESSAGE_OCTET_LENGTH :MESSAGE_TEXT :METHOD :MIN :MINUTE :MINVALUE :MOD :MODE :MODIFIES
    :MODULE :MONTH :MORE :MOVE :MULTISET :MUMPS :NAME :NAMES :NAMESPACE :NATIONAL :NATURAL :NCHAR :NCLOB :NESTED
    :NESTING :NEW :NEXT :NFC :NFD :NFKC :NFKD :NIL :NO :NONE :NORMALIZE :NORMALIZED :NOT :NOTHING :NOTIFY
    :NOTNULL :NOWAIT :NTH_VALUE :NTILE :NULL :NULLABLE :NULLIF :NULLS :NUMBER :NUMERIC :OBJECT :OCCURRENCES_REGEX :OCTETS :OCTET_LENGTH :OF :OFF :OFFSET
    :OIDS :OLD :OMIT :ON :ONE :ONLY :OPEN :OPERATOR :OPTION :OPTIONS :OR :ORDER :ORDERING
    :ORDINALITY :OTHERS :OUT :OUTER :OUTPUT :OVER :OVERFLOW :OVERLAPS :OVERLAY :OVERRIDING :OWNED :OWNER :P :PAD
    :PARALLEL :PARAMETER :PARAMETER_MODE :PARAMETER_NAME :PARAMETER_ORDINAL_POSITION :PARAMETER_SPECIFIC_CATALOG :PARAMETER_SPECIFIC_NAME :PARAMETER_SPECIFIC_SCHEMA :PARSER :PARTIAL :PARTITION :PASCAL :PASS :PASSING
    :PASSTHROUGH :PASSWORD :PAST :PATH :PATTERN :PER :PERCENT :PERCENTILE_CONT :PERCENTILE_DISC :PERCENT_RANK :PERIOD :PERMISSION
    :PERMUTE :PLACING :PLAN :PLANS :PLI :POLICY :PORTION :POSITION :POSITION_REGEX :POWER :PRECEDES :PRECEDING :PRECISION :PREPARE :PREPARED :PRESERVE :PRIMARY
    :PRIOR :PRIVATE :PRIVILEGES :PROCEDURAL :PROCEDURE :PROCEDURES :PROGRAM :PRUNE :PTF
    :PUBLIC :PUBLICATION :QUOTE :QUOTES :RANGE :RANK :READ :READS :REAL :REASSIGN :RECHECK :RECOVERY
    :RECURSIVE :REF :REFERENCES :REFERENCING :REFRESH :REGR_AVGX :REGR_AVGY :REGR_COUNT :REGR_INTERCEPT :REGR_R2
    :REGR_SLOPE :REGR_SXX :REGR_SXY :REGR_SYY :REINDEX :RELATIVE :RELEASE :RENAME :REPEATABLE :REPLACE :REPLICA
    :REQUIRING :RESET :RESPECT :RESTART :RESTORE :RESTRICT :RESULT :RETURN :RETURNED_CARDINALITY :RETURNED_LENGTH :RETURNED_OCTET_LENGTH :RETURNED_SQLSTATE :RETURNING
    :RETURNS :REVOKE :RIGHT :ROLE :ROLLBACK :ROLLUP :ROUTINE :ROUTINES :ROUTINE_CATALOG :ROUTINE_NAME
    :ROUTINE_SCHEMA :ROW :ROWS :ROW_COUNT :ROW_NUMBER :RULE :RUNNING :SAVEPOINT :SCALAR :SCALE
    :SCHEMA :SCHEMAS :SCHEMA_NAME :SCOPE :SCOPE_CATALOG :SCOPE_NAME :SCOPE_SCHEMA :SCROLL :SEARCH
    :SECOND :SECTION :SECURITY :SEEK :SELECT :SELECTIVE :SELF :SENSITIVE :SEQUENCE :SEQUENCES :SERIALIZABLE
    :SERVER :SERVER_NAME :SESSION :SESSION_USER :SET :SETOF :SETS :SHARE :SHOW :SIMILAR :SIMPLE
    :SIN :SINH :SIZE :SKIP :SMALLINT :SNAPSHOT :SOME :SOURCE :SPACE :SPECIFIC :SPECIFICTYPE :SPECIFIC_NAME
    :SQL :SQLCODE :SQLERROR :SQLEXCEPTION :SQLSTATE :SQLWARNING :SQRT :STABLE :STANDALONE
    :START :STATE :STATEMENT :STATIC :STATISTICS :STDDEV_POP :STDDEV_SAMP :STDIN :STDOUT :STORAGE :STORED
    :STRICT :STRING :STRIP :STRUCTURE :STYLE :SUBCLASS_ORIGIN :SUBMULTISET :SUBSCRIPTION :SUBSET :SUBSTRING :SUBSTRING_REGEX :SUCCEEDS :SUM
    :SUPPORT :SYMMETRIC :SYSID :SYSTEM :SYSTEM_TIME :SYSTEM_USER :T :TABLE :TABLES :TABLESAMPLE :TABLESPACE :TABLE_NAME
    :TAN :TANH :TEMP :TEMPLATE :TEMPORARY :TEXT :THEN :THROUGH :TIES :TIME :TIMEOUT :TIMESTAMP
    :TIMEZONE_HOUR :TIMEZONE_MINUTE :TO :TOKEN :TOP_LEVEL_COUNT :TRAILING :TRANSACTION :TRANSACTIONS_COMMITTED :TRANSACTIONS_ROLLED_BACK :TRANSACTION_ACTIVE :TRANSFORM
    :TRANSFORMS :TRANSLATE :TRANSLATE_REGEX :TRANSLATION :TREAT :TRIGGER :TRIGGER_CATALOG :TRIGGER_NAME :TRIGGER_SCHEMA :TRIM
    :TRIM_ARRAY :TRUE :TRUNCATE :TRUSTED :TYPE :TYPES :UESCAPE :UNBOUNDED :UNCOMMITTED :UNCONDITIONAL :UNDER
    :UNENCRYPTED :UNION :UNIQUE :UNKNOWN :UNLINK :UNLISTEN :UNLOGGED :UNMATCHED :UNNAMED :UNNEST :UNTIL :UNTYPED
    :UPDATE :UPPER :URI :USAGE :USER :USER_DEFINED_TYPE_CATALOG :USER_DEFINED_TYPE_CODE :USER_DEFINED_TYPE_NAME :USER_DEFINED_TYPE_SCHEMA :USING :UTF16 :UTF32 :UTF8 :VACUUM
    :VALID :VALIDATE :VALIDATOR :VALUE :VALUES :VALUE_OF :VARBINARY :VARCHAR :VARIADIC :VARYING :VAR_POP :VAR_SAMP :VERBOSE
    :VERSION :VERSIONING :VIEW :VIEWS :VOLATILE :WAITLSN :WHEN :WHENEVER :WHERE :WHITESPACE :WIDTH_BUCKET :WINDOW :WITH
    :WITHIN :WITHOUT :WORK :WRAPPER :WRITE :XML :XMLAGG :XMLATTRIBUTES :XMLBINARY :XMLCAST :XMLCOMMENT :XMLCONCAT :XMLDECLARATION :XMLDOCUMENT
    :XMLELEMENT :XMLEXISTS :XMLFOREST :XMLITERATE :XMLNAMESPACES :XMLPARSE :XMLPI :XMLQUERY :XMLROOT :XMLSCHEMA
    :XMLSERIALIZE :XMLTABLE :XMLTEXT :XMLVALIDATE :YEAR :YES :ZONE})

;; SELECT [ ALL | DISTINCT [ ON ( выражение [, ...] ) ] ]
;; [ * | выражение [ [ AS ] имя_результата ] [, ...] ]
;; [ FROM элемент_FROM [, ...] ]
;; [ WHERE условие ]
;; [ GROUP BY элемент_группирования [, ...] ]
;; [ HAVING условие [, ...] ]
;; [ WINDOW имя_окна AS ( определение_окна ) [, ...] ]
;; [ { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] выборка ]
;; [ ORDER BY выражение [ ASC | DESC | USING оператор ] [ NULLS { FIRST | LAST } ] [, ...] ]
;; [ LIMIT { число | ALL } ]
;; [ OFFSET начало [ ROW | ROWS ] ]
;; [ FETCH { FIRST | NEXT } [ число ] { ROW | ROWS } ONLY ]
;; [ FOR { UPDATE | NO KEY UPDATE | SHARE | KEY SHARE } [ OF имя_таблицы [, ...] ] [ NOWAIT | SKIP LOCKED ] [...] ]


(defonce ^ObjectMapper object-mapper (ObjectMapper.))

(defn acc-identity [acc & _]
  acc)

(defn identifier [acc opts id]
  (conj acc (ql/escape-ident (:keywords opts) id)))

(defn join-vec [separator coll]
  (pop (reduce (fn [acc value] (into [] (concat acc value [separator]))) [] coll)))

(def keys-for-select
  [[:explain :pg/explain]
   [:select :pg/projection]
   [:select-distinct :pg/projection]
   [:from :pg/from]
   [:left-join-lateral :pg/join-lateral]
   [:left-join :pg/left-join]
   [:left-outer-join :pg/left-outer-join]
   [:join :pg/join]
   [:where :pg/and]
   [:group-by :pg/group-by]
   [:having :pg/having]
   [:window :pg/window]
   [:union :pg/union] #_"NOTE: should it be after :where? (or even lower?)"
   [:union-all :pg/union-all]
   [:order-by :pg/order-by]
   [:limit :pg/limit]
   [:offset :pg/offset]
   [:fetch :pg/fetch]
   [:for :pg/for]])

(defmethod ql/to-sql
  :pg/order-by
  [acc opts data]
  (if (map? data)
    (->> (dissoc data :ql/type)
         (ql/reduce-separated ","
                              acc
                              (fn [acc [expr dir]]
                                (-> acc
                                    (ql/to-sql opts expr)
                                    (conj (name dir))))))
    (ql/to-sql opts data)))

(defmethod ql/to-sql
  :pg/explain
  [acc opts data]
  (cond->  acc
    (:analyze data) (conj "ANALYZE")))

(defmethod ql/to-sql
  :pg/group-by
  [acc opts data]
  (->> (dissoc data :ql/type)
       (ql/reduce-separated "," acc
                            (fn [acc [k node]]
                              (-> acc
                                  (conj (str "/*" (name k) "*/"))
                                  (ql/to-sql opts node))))))

(defmethod ql/to-sql
  :pg/union
  [acc opts data]
  (->> (dissoc data :ql/type)
       (ql/reduce-separated "UNION" acc
                            (fn [acc [k node]]
                              (-> acc
                                  (ql/to-sql opts node)
                                  (conj (str " /* " (name k) " */ ")))))))

(defmethod ql/to-sql
  :pg/union-all
  [acc opts data]
  (->> (dissoc data :ql/type)
       (ql/reduce-separated "UNION ALL" acc
                            (fn [acc [k node]]
                              (-> acc
                                  (ql/to-sql opts node)
                                  (conj (str " /* " (name k) " */ ")))))))

(defmethod ql/to-sql
  :pg/sql
  [acc opts [_ data]]
  (conj acc data))

(defmethod ql/to-sql
  :pg/and
  [acc opts data]
  (->> (dissoc data :ql/type)
       (filter (fn [[k v]] (not (nil? v))))
       (ql/reduce-separated
        "AND" acc
        (fn [acc [k v]]
          (-> acc
              (conj (str "/*" (name k) "*/"))
              (ql/to-sql opts v))))))

(defmethod ql/to-sql
  :pg/or
  [acc opts data]
  (-> acc
      (conj "(")
      (into (->> (dissoc data :ql/type)
                 (filter (fn [[k v]] (not (nil? v))))
                 (ql/reduce-separated
                   "OR" []
                   (fn [acc [k v]]
                     (-> acc
                         (conj (str "/*" (name k) "*/"))
                         (ql/to-sql opts v))))))
      (conj ")")))

(defmethod ql/to-sql
  :or
  [acc opts [_ & data]]
  (let [acc (conj acc "(")]
    (-> (->> data
          (filter (fn [v] (not (nil? v))))
          (ql/reduce-separated
           "OR" acc
           (fn [acc v]
             (-> acc (ql/to-sql opts v)))))
        (conj ")"))))

(defmethod ql/to-sql
  :and
  [acc opts [_ & data]]
  (let [acc (conj acc "(")]
    (-> (->> data
             (filter (fn [v] (not (nil? v))))
             (ql/reduce-separated
              "AND" acc
              (fn [acc v]
                (-> acc (ql/to-sql opts v)))))
        (conj ")"))))

(defn operator [acc opts op args]
  (->> args
       (ql/reduce-separated2 acc op
                             (fn [acc expr]
                               (-> acc
                                   (conj "(")
                                   (ql/to-sql opts expr)
                                   (conj ")"))))))

(defmethod ql/to-sql
  :||
  [acc opts [_ & args]]
  (operator acc opts "||" args))

(defmethod ql/to-sql
  :&&
  [acc opts [_ & args]]
  (operator acc opts "&&" args))

(defmethod ql/to-sql
  :pg/left-join
  [acc opts data]
  (->> (dissoc data :ql/type)
       (filter (fn [[k v]] (not (nil? v))))
       (ql/reduce-separated
        "LEFT JOIN" acc
        (fn [acc [k v]]
          (-> acc
              (conj (name (:table v)))
              (conj (name k) "ON")
              (ql/to-sql opts (ql/default-type (:on v) :pg/and)))))))

(defmethod ql/to-sql
  :pg/left-outer-join
  [acc opts data]
  (->> (dissoc data :ql/type)
       (filter (fn [[k v]] (not (nil? v))))
       (ql/reduce-separated
        "LEFT OUTER JOIN" acc
        (fn [acc [k v]]
          (-> acc
              (conj (name (:table v)))
              (conj (name k) "ON")
              (ql/to-sql opts (ql/default-type (:on v) :pg/and)))))))

(defmethod ql/to-sql
  :pg/join
  [acc opts data]
  (->> (dissoc data :ql/type)
       (filter (fn [[k v]] (not (nil? v))))
       (ql/reduce-separated
        "JOIN" acc
        (fn [acc [k v]]
          (-> acc
              (conj (name (:table v)))
              (conj (name k) "ON")
              (ql/to-sql opts (ql/default-type (:on v) :pg/and)))))))

(defmethod ql/to-sql
  :pg/join-lateral
  [acc opts data]
  (->> (dissoc data :ql/type)
       (filter (fn [[k v]] (not (nil? v))))
       (ql/reduce-separated
        "LEFT JOIN LATERAL" acc
        (fn [acc [k v]]
          (-> acc
              (conj "(")
              (ql/to-sql opts (:sql v))
              (conj ")")
              (conj (name k) "ON")
              (ql/to-sql opts (ql/default-type (:on v) :pg/and)))))))

(defmethod ql/to-sql
  :pg/count*
  [acc _ _]
  (conj acc "count(*)"))

(defn strip-nils [m]
  (filterv (fn [[k v]] (not (nil? v))) m))

(defn pg-select
  [acc opts data]
  (->> keys-for-select
       (ql/reduce-acc
         acc
         (fn [acc [k default-type]]
           (let [sub-node (get data k)]
             (if (and sub-node
                      (not (and (map? sub-node) (empty? (strip-nils sub-node)))))
               (-> acc
                   (conj (str/upper-case (str/replace (name k) #"-" " ")))
                   (ql/to-sql opts (ql/default-type sub-node default-type)))
               acc))))))

(defmethod ql/to-sql
  :pg/select
  [acc opts data]
  (pg-select acc opts data))

(defmethod ql/to-sql
  :pg/sub-select
  [acc opts data]
  (-> acc
      (conj "(")
      (pg-select opts data)
      (conj ")")))

(defmethod ql/to-sql
  :pg/op-wrapped
  [acc opts [op l r]]
  (-> acc
      (conj "(")
      (ql/to-sql opts l)
      (conj (name op))
      (ql/to-sql opts r)
      (conj ")")))

(defmethod ql/to-sql
  :pg/op
  [acc opts [op l r]]
  (-> acc
      (ql/to-sql opts l)
      (conj (name op))
      (ql/to-sql opts r)))

(defmethod ql/to-sql
  :pg/fn
  [acc opts [f & args]]
  (let [acc (-> acc
                (conj (str (name f) "(")))]
    (->
     (->> args
          (ql/reduce-separated
           "," acc
           (fn [acc arg]
             (ql/to-sql acc opts arg))))
     (conj ")"))))

(defmethod ql/to-sql
  :pg/from
  [acc opts node]
  (->> (dissoc node :ql/type)
       (ql/reduce-separated
         "," acc
         (fn [acc [k sub-node]]
           (if-let [ks (and (= :pg/values (:ql/type sub-node))
                            (:keys sub-node))]
             (-> acc
                 (conj "(")
                 (ql/to-sql opts sub-node)
                 (conj ")")
                 (conj (name k))
                 (conj "(")
                 (conj (ql/fast-join " , " (map name ks)))
                 (conj ")"))
             (-> acc
                 (ql/to-sql opts sub-node)
                 (conj (name k))))))))

(defmethod ql/to-sql
  :pg/param
  [acc opts [_ v]]
  (conj acc ["?" v]))

(defmethod ql/to-sql
  :pg/jsonb
  [acc opts v]
  (if (= :pg/jsonb (first v))
    (conj acc (ql/string-litteral (json/write-value-as-string (second v))))
    (conj acc (ql/string-litteral (json/write-value-as-string v)))))

(defn to-json-string [^JsonNode json]
  (.writeValueAsString object-mapper json))

(defn jackson-param [acc v] (conj acc ["?" (to-json-string v)]))
(defn jackson-param-as-text [acc ^JsonNode v]
  (conj acc ["?" (.asText v)]))

(defmethod ql/to-sql ObjectNode  [acc opts v] (jackson-param acc v))
(defmethod ql/to-sql ArrayNode   [acc opts v] (jackson-param acc v))
(defmethod ql/to-sql TextNode    [acc opts v] (jackson-param-as-text acc v))
(defmethod ql/to-sql IntNode     [acc opts v] (jackson-param-as-text acc v))
(defmethod ql/to-sql BooleanNode [acc opts v] (jackson-param-as-text acc v))
(defmethod ql/to-sql DoubleNode  [acc opts v] (jackson-param-as-text acc v))
(defmethod ql/to-sql LongNode    [acc opts v] (jackson-param-as-text acc v))

(defmethod dsql.core/to-sql
  :pg/obj
  [acc opts obj]
  (let [{:keys [eval-key]} (meta obj)
        acc (conj acc "jsonb_build_object(")
        acc (->> (dissoc obj :ql/type)
                 (dsql.core/reduce-separated
                  "," acc
                  (fn [acc [k sub-node]]
                    (-> acc
                        (conj (if eval-key
                                (name k)
                                (dsql.core/string-litteral (name k))))
                        (conj ",")
                        ((fn [acc']
                           (if (string? sub-node)
                             (conj acc' ["?::text", sub-node])
                             (dsql.core/to-sql acc' opts sub-node))))))))]
    (conj acc ")")))

(defmethod dsql.core/to-sql
  :jsonb/array
  [acc opts arr]
  (let [acc (conj acc "jsonb_build_array(")
        acc (->> arr
                 (dsql.core/reduce-separated
                  "," acc
                  (fn [acc sub-node]
                    (-> acc
                        ((fn [acc']
                           (if (string? sub-node)
                             (conj acc' ["?::text", sub-node])
                             (dsql.core/to-sql acc' opts sub-node))))))))]
    (conj acc ")")))

(defmethod dsql.core/to-sql
  :clj-arr->jsonb-arr
  [acc opts [_ & [els]]]
  (let [acc (conj acc "jsonb_build_array(")
        acc (->> els
                 (dsql.core/reduce-separated
                  "," acc
                  (fn [acc sub-node]
                    (-> acc
                        ((fn [acc']
                           (if (string? sub-node)
                             (conj acc' ["?::text", sub-node])
                             (dsql.core/to-sql acc' opts sub-node))))))))]
    (conj acc ")")))

(defmethod dsql.core/to-sql
  :jsonb/obj
  [acc opts obj]
  (let [acc (conj acc "jsonb_build_object(")
        acc (->> (dissoc obj :ql/type)
                 (dsql.core/reduce-separated
                  "," acc
                  (fn [acc [k sub-node]]
                    (-> acc
                        (conj (dsql.core/string-litteral (name k)))
                        (conj ",")
                        ((fn [acc']
                           (if (string? sub-node)
                             (conj acc' ["?::text", sub-node])
                             (dsql.core/to-sql acc' opts sub-node))))))))]
    (conj acc ")")))

(defn alpha-num? [s]
  (some? (re-matches #"^[a-zA-Z][a-zA-Z0-9]*$" s)))

;; (alpha-num? "a123")
;; (alpha-num? "1a123")
;; (alpha-num? "a b123")
;; (alpha-num? "hp.type")

(defn- to-array-list [arr]
  (->> arr
       (mapv (fn [x]
               (let [x (if (keyword? x) (name x) x)]
                 (if (string? x)
                   (if (alpha-num? x) x (str "\"" x "\""))
                   (if (number? x)
                     x
                     (assert false (pr-str x)))))))
       (ql/fast-join ",")))


(defn- to-array-value [arr]
  (str "{" (to-array-list arr) "}"))

(defn- to-array-litteral [arr]
  (str "'" (to-array-value arr) "'"))

(defmethod ql/to-sql
  :pg/array
  [acc opts [_ arr]]
  (conj acc (to-array-litteral arr)))

(defmethod ql/to-sql
  :pg/kfn
  [acc opts [f arg & args]]
  (let [acc (-> (conj acc (str (name f) "("))
                (ql/to-sql opts arg))]
    (->
     (loop [[k v & kvs] args
            acc acc]
       (let [acc (-> acc (conj (name k))
                     (ql/to-sql opts v))]
         (if (empty? kvs)
           acc
           (recur kvs acc))))
     (conj ")"))))

(defmethod ql/to-sql
  :pg/array-param
  [acc opts [_ tp arr]]
  (conj acc
        [(str "?::" (name tp) "[]")
         (to-array-value arr)]))

(defmethod ql/to-sql
  :pg/index-expr
  [acc opts node]
  (-> acc
      (conj "(")
      (conj "EXPR???")
      (conj ")")))

(defmethod ql/to-sql
  :pg/index-with
  [acc opts node]
  (ql/parens
   acc (fn [acc]
         (->> (dissoc node :ql/type)
              (ql/reduce-separated
               "," acc
               (fn [acc [k node]]
                 (-> acc
                     (conj (ql/escape-ident opts k))
                     (conj "=")
                     (ql/to-sql opts node))))))))

(defmethod ql/to-sql
  :pg/projection
  [acc opts data]
  (let [{proj-params :pg/projection, :as data-meta} (meta data)

        acc (cond
              (= :distinct proj-params)
              (conj acc "DISTINCT")

              (= :everything proj-params)
              (conj acc "*,")

              (= :all proj-params)
              (conj acc "ALL")

              (and (map? proj-params) (:distinct-on proj-params))
              (-> (ql/reduce-separated "," (conj acc "DISTINCT ON (")
                                       (fn [acc node] (ql/to-sql acc opts node))
                                       (:distinct-on proj-params))
                  (conj ")"))

              :else acc)

        data (with-meta data (dissoc data-meta :pg/projection))]
    (->> (dissoc data :ql/type)
         (sort-by first)
         (ql/reduce-separated "," acc
                              (fn [acc [k node]]
                                (-> acc
                                    (ql/to-sql opts node)
                                    (conj "as" (ql/escape-ident opts k))))))))

(def index-keys
  [[:unique acc-identity]
   [:index identifier]
   [:if-not-exists acc-identity]
   [:on identifier]
   [:on-only identifier]
   [:using]
   [:expr :pg/index-expr]
   [:with :pg/index-with]
   [:tablespace identifier]
   [:where :pg/and]])

(defmethod ql/to-sql
  :pg/index
  [acc opts {idx :index unique :unique tbl :on using :using expr :expr where :where :as node}]
  (when-not (and idx tbl expr)
    (throw (Exception. (str ":index :on :expr are required! Got " (pr-str node)))))
  (-> acc
      (conj "CREATE")
      (cond-> unique (conj "UNIQUE"))
      (conj "INDEX")
      (conj "IF NOT EXISTS")
      (conj (name idx))
      (conj "ON")
      (conj (name tbl))
      (cond-> using (conj (str "USING " (name using))))
      (conj "(")
      (ql/reduce-separated2 ","
                            (fn [acc exp]
                              (-> acc
                                  (conj "(")
                                  (ql/to-sql opts exp)
                                  (conj ")"))) expr)
      (conj ")")
      (cond-> where
        (->
         (conj "WHERE")
         (ql/to-sql opts where)))))

(defmethod ql/to-sql
  :pg/drop-index
  [acc opts {idx :index ife :if-exists}]
  (-> acc
      (conj "DROP")
      (conj "INDEX")
      (cond-> ife (conj "IF EXISTS"))
      (identifier opts idx)))

(defmethod ql/to-sql
  :pg/primary-key
  [acc opts {table :table constraint-name :constraint columns :columns :as node}]
  (when-not (and table constraint-name columns)
    (throw (Exception. (str ":table, :constraint, :columns are required! Got " (pr-str node)))))
  (-> acc
      (conj "ALTER TABLE")
      (conj (name table))
      (conj "ADD CONSTRAINT")
      (conj (name constraint-name))
      (conj "PRIMARY KEY")
      (conj "(")
      (ql/reduce-separated2 ","
                            (fn [acc exp]
                              (conj acc (name exp)))
                            columns)
      (conj ")")))

(defmethod ql/to-sql
  :pg/alter-table
  [acc opts {table :table {pk :primary-key :as add} :add :as node}]
  (-> acc
      (conj "ALTER TABLE")
      (conj (name table))
      (cond-> add (conj "ADD"))
      (cond-> pk
        (-> (conj "PRIMARY KEY")
            (conj "(")
            (ql/reduce-separated2 "," (fn [acc exp] (conj acc (str "\"" (name exp) "\""))) pk)
            (conj ")")))
      ))

(defmethod ql/to-sql
  :pg/create-table-as
  [acc opts {table :table select :select ifne :if-not-exists :as node}]
  (when-not (and table select)
    (throw (Exception. (str ":table, :select are required! Got " (pr-str node)))))
  (-> acc
      (conj "CREATE TABLE")
      (cond-> ifne (conj "IF NOT EXISTS"))
      (conj (name table))
      (conj "AS")
      (ql/to-sql opts select)))

(defn resolve-type [x]
  (if-let [m (meta x)]
    (cond
      (:pg/op m)         :pg/op
      (:pg/fn m)         :pg/fn
      (:pg/obj m)        :pg/obj
      (:jsonb/obj m)     :jsonb/obj
      (:jsonb/array m)   :jsonb/array
      (:pg/jsonb m)      :pg/jsonb
      (:pg/kfn m)        :pg/kfn
      (:pg/select m)     :pg/select
      (:pg/sub-select m) :pg/sub-select
      :else nil)))

(defmethod ql/to-sql
  :jsonb/?
  [acc opts [_ l r]]
  (-> acc
      (ql/to-sql opts l)
      (conj "??")
      (ql/to-sql opts r)))

(defmethod ql/to-sql
  :jsonb/->
  [acc opts [_ col k]]
  (-> acc
      (ql/to-sql opts col)
      (conj "->")
      (conj (if (integer? k)
              k
              (ql/string-litteral k)))))

(defmethod ql/to-sql
  :jsonb/->>
  [acc opts [_ col k]]
  (-> acc
      (ql/to-sql opts col)
      (conj "->>")
      (conj (ql/string-litteral k))))

(defmethod ql/to-sql
  :jsonb/#>
  [acc opts [_ col path]]
  (-> acc
      (ql/to-sql opts col)
      (conj "#>")
      (conj (to-array-litteral path))))

(defmethod ql/to-sql
  :jsonb/#>>
  [acc opts [_ col path]]
  (-> acc
      (ql/to-sql opts col)
      (conj "#>>")
      (conj (to-array-litteral path))))

(defmethod ql/to-sql
  :jsonb/#>*
  [acc opts [_ col path]]
  (-> acc
      (ql/to-sql opts col)
      (conj "#> (")
      (ql/to-sql opts path)
      (conj ")")))

(defmethod ql/to-sql
  :jsonb/#>>*
  [acc opts [_ col path]]
  (-> acc
      (ql/to-sql opts col)
      (conj "#>> (")
      (ql/to-sql opts path)
      (conj ")")))

(defn format [node]
  (ql/format {:resolve-type #'resolve-type :keywords keywords} (ql/default-type node :pg/select)))


(def keys-for-update
  [[:set :pg/set]
   [:from :pg/from]
   [:left-join-lateral :pg/join-lateral]
   [:where :pg/and]
   [:returning]])

(defn pg-update
  [acc opts data]
  (assert (:update data) "Key :update is required!")
  (let [acc (conj acc "UPDATE")
        acc (conj acc (if (map? (:update data))
                        (ql/fast-join " " (reverse (map name (first (:update data)))))
                        (name (:update data))))]
    (->> keys-for-update
        (ql/reduce-acc
         acc
         (fn [acc [k default-type]]
           (let [sub-node (get data k)]
             (if (and sub-node
                      (not (and (map? sub-node) (empty? (strip-nils sub-node)))))
               (-> acc
                   (conj (str/upper-case (str/replace (name k) #"-" " ")))
                   (ql/to-sql opts (ql/default-type sub-node default-type)))
               acc)))))))

(defmethod ql/to-sql
  :pg/update
  [acc opts data]
  (pg-update acc opts data))

(defmethod ql/to-sql
  :pg/set
  [acc opts data]
  (->> (dissoc data :ql/type)
       (ql/reduce-separated "," acc
                            (fn [acc [k node]]
                              (-> acc
                                  (conj (ql/escape-ident opts k) "=")
                                  (ql/to-sql opts node))))))

(def keys-for-delete
  [[:from :pg/from]
   [:where :pg/and]
   [:returning]])

(defn pg-delete
  [acc opts data]
  (let [acc (conj acc "DELETE")]
    (->> keys-for-delete
        (ql/reduce-acc
         acc
         (fn [acc [k default-type]]
           (let [sub-node (get data k)]
             (if (and sub-node
                      (not (and (map? sub-node) (empty? (strip-nils sub-node)))))
               (-> acc
                   (conj (str/upper-case (str/replace (name k) #"-" " ")))
                   (ql/to-sql opts (ql/default-type sub-node default-type)))
               acc)))))))

(defmethod ql/to-sql
  :pg/delete
  [acc opts data]
  (pg-delete acc opts data))

(defmethod ql/to-sql
  :resource->
  [acc opts [_ k]]
  (conj acc (str "resource->" (ql/string-litteral (name k)))))

(defmethod ql/to-sql
  :resource->
  [acc opts [_ k]]
  (conj acc (str "resource->" (ql/string-litteral (name k)))))

(defmethod ql/to-sql
  :resource->>
  [acc opts [_ k]]
  (conj acc (str "resource->>" (ql/string-litteral (name k)))))

(defmethod ql/to-sql
  :resource#>
  [acc opts [_ path]]
  (conj acc (str "resource#>" (to-array-litteral path))))

(defmethod ql/to-sql
  :resource#>>
  [acc opts [_ path]]
  (conj acc (str "resource#>>" (to-array-litteral path))))

(defmethod ql/to-sql
  :is
  [acc opts [_ l r]]
  (-> acc
      (ql/to-sql opts l)
      (conj "IS")
      (ql/to-sql opts r)))

(defmethod ql/to-sql
  :is-not
  [acc opts [_ l r]]
  (-> acc
      (ql/to-sql opts l)
      (conj "IS NOT")
      (ql/to-sql opts r)))

(defmethod ql/to-sql
  :ilike
  [acc opts [_ l r]]
  (-> acc
      (ql/to-sql opts l)
      (conj "ILIKE")
      (ql/to-sql opts r)))

(defmethod ql/to-sql
  :similar-to
  [acc opts [_ l r]]
  (-> acc
      (ql/to-sql opts l)
      (conj "SIMILAR TO")
      (ql/to-sql opts r)))

(defmethod ql/to-sql
  :in
  [acc opts [_ l r]]
  (-> acc
      (ql/to-sql opts l)
      (conj "IN")
      (ql/to-sql opts r)))

(defmethod ql/to-sql
  :not-in
  [acc opts [_ l r]]
  (-> acc
      (ql/to-sql opts l)
      (conj "NOT IN")
      (ql/to-sql opts r)))

(defmethod ql/to-sql
  :pg/with-ordinality
  [acc opts [_ x]]
  (-> acc
      (ql/to-sql opts x)
      (conj "WITH ORDINALITY")))

(defn params-list [acc params]
  (if (seq params)
    (->> params
         (ql/reduce-separated "," acc
                              (fn [acc p]
                                (conj acc ["?" p]))))
    (throw (ex-info ":pg/params-list can not be empty"
                    {:ql/type :pg/params-list
                     :code :params-required}))))

(defmethod ql/to-sql
  :pg/params-list
  [acc opts [_ params]]
  (-> (conj acc "(")
      (params-list params)
      (conj  ")")))

(defmethod ql/to-sql
  :pg/unwrapped-params-list
  [acc opts [_ params]]
  (params-list acc params))

(defmethod ql/to-sql
  :pg/inplace-params-list
  [acc opts [_ params]]
  (let [acc (conj acc "(")]
    (->
     (->> params
          (ql/reduce-separated "," acc
                               (fn [acc p]
                                 (conj acc (ql/string-litteral p)))))
     (conj  ")"))))

(defmethod ql/to-sql
  :not
  [acc opts [_ expr]]
  (-> acc
      (conj "NOT")
      (ql/to-sql opts expr)))

(defmethod ql/to-sql
  :not-with-parens
  [acc opts [_ expr]]
  (-> acc
      (conj "NOT (")
      (ql/to-sql opts expr)
      (conj ")")))

(defmethod ql/to-sql
  :pg/include-op
  [acc opts [_ l r]]
  (-> acc
      (ql/to-sql opts l)
      (conj "@>")
      (ql/to-sql opts r)))

(defmethod ql/to-sql
  "@@"
  [acc opts [_ l r]]
  (-> acc
      (ql/to-sql opts l)
      (conj "@@")
      (ql/to-sql opts r)))

(defmethod ql/to-sql
  "@?"
  [acc opts [_ l r]]
  (-> acc
      (ql/to-sql opts l)
      (conj "@?")
      (ql/to-sql opts r)))

(defmethod ql/to-sql
  "@@::jp"
  [acc opts [_ l r]]
  (-> acc
      (ql/to-sql opts l)
      (conj "@@")
      (ql/to-sql opts [:pg/cast r :jsonpath])))

(defmethod ql/to-sql
  "@?::jp"
  [acc opts [_ l r]]
  (-> acc
      (ql/to-sql opts l)
      (conj "@?")
      (ql/to-sql opts [:pg/cast r :jsonpath])))


(defmethod ql/to-sql
  :pg/coalesce
  [acc opts [_ & args]]
  (-> (reduce
        (fn [acc x]
          (conj (ql/to-sql acc opts x) ","))
        (conj acc "COALESCE(")
        args)
      (drop-last)
      (vec)
      (conj ")")))

(defmethod ql/to-sql
  :=
  [acc opts [_ l r]]
  (-> acc
      (ql/to-sql opts l)
      (conj "=")
      (ql/to-sql opts r)))

(defmethod ql/to-sql
  :!=
  [acc opts [_ l r]]
  (-> acc
      (ql/to-sql opts l)
      (conj "!=")
      (ql/to-sql opts r)))

(defmethod ql/to-sql
  :pg/cast
  [acc opts [_ expr tp]]
  (-> acc
      (conj "(")
      (ql/to-sql opts expr)
      (conj (str ")::" (name tp)))))

(defmethod ql/to-sql
  :distinct
  [acc opts [_ expr]]
  (-> acc
      (conj "DISTINCT(")
      (ql/to-sql opts expr)
      (conj ")")))

(defmethod ql/to-sql
  :jsonb/array_elements_text
  [acc opts [_ expr]]
  (-> acc
      (conj "jsonb_array_elements(")
      (ql/to-sql opts expr)
      (conj ")")))

(defmethod ql/to-sql
  :count*
  [acc opts [_ expr]]
  (conj acc "count(*)"))


(defmethod ql/to-sql
  :cond
  [acc opts [_ & pairs]]
  (into
    acc
    (concat
      ["( case"]
      (->> (partition-all 2 pairs)
           (mapcat (fn [pair]
                     (if (= 1 (count pair))
                       (-> ["else ("]
                           (ql/to-sql opts (first pair))
                           (conj ")"))
                       (-> ["when ("]
                           (ql/to-sql opts (first pair))
                           (conj ") then (")
                           (ql/to-sql opts (second pair))
                           (conj ")"))))))
      ["end )"])))


;; *** Simple PostgreSQL CASE expression ***
;;
;; CASE expression
;;     WHEN value_1 THEN result_1
;;     WHEN value_2 THEN result_2
;; [WHEN ...]
;; ELSE
;;     else_result
;; END
(defmethod ql/to-sql
  :case
  [acc opts [_ expression & pairs]]
  (into
    acc
    (concat
      (-> ["( case ("]
          (ql/to-sql opts expression)
          (conj ")"))
      (->> (partition-all 2 pairs)
           (mapcat (fn [pair]
                     (if (= 1 (count pair))
                       (-> ["else ("]
                           (ql/to-sql opts (first pair))
                           (conj ")"))
                       (-> ["when ("]
                           (ql/to-sql opts (first pair))
                           (conj ") then (")
                           (ql/to-sql opts (second pair))
                           (conj ")"))))))
      ["end )"])))


;; *** A general CASE example ***
;;
;; CASE
;;    WHEN length> 0 AND length <= 50 THEN 'Short'
;;    WHEN length > 50 AND length <= 120 THEN 'Medium'
;;    WHEN length> 120 THEN 'Long'
;; END duration
(defmethod ql/to-sql
  :pg/case
  [acc opts [_ & pairs]]
  (into
   acc
   (concat
    ["( case "]
    (->> (partition-all 2 pairs)
         (mapcat (fn [pair]
                   (if (= 1 (count pair))
                     (-> ["else ("]
                         (ql/to-sql opts (first pair))
                         (conj ")"))
                     (-> ["when ("]
                         (ql/to-sql opts (first pair))
                         (conj ") then (")
                         (ql/to-sql opts (second pair))
                         (conj ")"))))))
    ["end )"])))

(defn mk-columns [opts columns]
  (->> columns
       (reduce-kv
         (fn [acc column {:keys [type primary-key not-null default] :as val}]
           (cond
             (vector? val)
             (conj acc (str "\"" (name column) "\" " (str/join " " (map name val))))

             (map? val)
             (conj acc
                   (cond-> (str "\"" (name column) "\" " type)
                     primary-key (str " PRIMARY KEY")
                     not-null    (str " NOT NULL")
                     default     (str " DEFAULT " (first (ql/to-sql [] opts default)))))))
         [])
       (str/join " , ")))

(defn mk-options [options]
  (->> options
       (reduce-kv
        (fn [acc opt val]
          (conj acc (str (name opt) " '" (name val) "'")))
        [])
       (ql/fast-join ", ")))

(defn mk-table-constraint [{:keys [primary-key] :as constraint}]
  (str/join
   ","
   (cond-> []
     primary-key
     (conj (str "PRIMARY KEY (" (str/join ", " (map (fn [x] (str "\"" (name x) "\"")) primary-key)) ")")))))


(defmethod ql/to-sql
  :pg/create-table
  [acc opts {:keys [foreign unlogged table-name columns server partition-by
                    constraint
                    partition-of for options] not-ex :if-not-exists :as node}]
  (-> acc
      (conj "CREATE")
      (cond-> unlogged (conj "UNLOGGED"))
      (cond-> foreign (conj "FOREIGN"))
      (conj "TABLE")
      (cond-> not-ex (conj "IF NOT EXISTS"))
      (identifier opts table-name)

      (cond-> partition-of (conj "partition of" (name partition-of) ))

      (cond-> (or columns constraint) (conj "("))
      (cond-> columns    (conj (mk-columns opts columns)))
      (cond-> (and columns constraint) (conj ","))
      (cond-> constraint (conj (mk-table-constraint constraint)))
      (cond-> (or columns constraint) (conj ")"))

      (cond-> for (conj "for values"
                        (when-let [f (:from for)] (str "from (" f ")"))
                        (when-let [t (:to   for)] (str "to (" t ")"))))
      (cond-> partition-by (conj "partition by" (name (:method partition-by)) "(" (name (:expr partition-by)) ")"))
      (cond-> server (conj "SERVER" (name server)))
      (cond-> options (conj "OPTIONS ("  (mk-options options) ")"))))

(defmethod ql/to-sql
  :pg/drop-table
  [acc opts {ex :if-exists tbl :table-name}]
  (-> acc
      (conj "DROP")
      (conj "TABLE")
      (cond-> ex (conj "IF EXISTS"))
      (identifier opts tbl)))

(defmethod ql/to-sql
  :pg/create-extension
  [acc opts {:keys [if-not-exists name schema cascade version]}]
  (assert name "extension :name is required")
  (cond-> acc
      :always (conj "CREATE EXTENSION")
      (true? if-not-exists) (conj "IF NOT EXISTS")
      :always (conj name)
      schema (into ["SCHEMA" schema])
      version (into ["VERSION" version])
      (true? cascade) (conj "CASCADE")))


(def keys-for-conflict-update
  [[:set :pg/set]
   [:where :pg/and]])

(defmethod ql/to-sql
  :pg/conflict-update
  [acc opts {on-expr :on do-expr :do}]
  (let [acc (-> acc
                (conj "(")
                (ql/reduce-separated2 "," (fn [acc exp] (ql/to-sql acc opts exp)) on-expr)
                (conj ")" "DO"))]
    (cond
      (map? do-expr)
      (->> keys-for-conflict-update
           (ql/reduce-acc
            (conj acc "UPDATE")
            (fn [acc [k default-type]]
              (let [sub-node (get do-expr k)]
                (if (and sub-node
                         (not (and (map? sub-node) (empty? (strip-nils sub-node)))))
                  (-> acc
                      (conj (str/upper-case (str/replace (name k) #"-" " ")))
                      (ql/to-sql opts (ql/default-type sub-node default-type)))
                  acc)))))
      (= :nothing do-expr)
      (conj acc "NOTHING")
      :else (assert false "Unexpected!"))))

(defmethod ql/to-sql
  :pg/insert-select
  [acc opts {tbl :into {proj :select :as sel} :select ret :returning on-conflict :on-conflict}]
  (-> acc
      (conj "INSERT INTO")
      (identifier opts tbl)
      (cond->  (map? proj)
        (-> (conj "(")
            (conj (->> (keys proj) (sort) (mapv #(str "\"" (name %) "\""))  (ql/fast-join ", ")))
            (conj ")")))
      (ql/to-sql opts (assoc sel :ql/type :pg/sub-select))
      (cond->
          on-conflict (-> (conj "ON CONFLICT")
                          (ql/to-sql opts (assoc on-conflict :ql/type :pg/conflict-update)))
          ret (-> (conj "RETURNING")
                  (ql/to-sql opts ret)))))

(defmethod ql/to-sql
  :pg/cte
  [acc opts {with :with sel :select}]
  (-> acc
      (conj "WITH")
      (ql/reduce-separated2 "," (fn [acc [k v]]
                                  (-> acc
                                      (into [(name k) "AS" "("])
                                      (ql/to-sql opts (update v :ql/type (fn [x] (or x :pg/select))))
                                      (conj ")")))
                            (if (sequential? with) (partition 2 with) with))
      (ql/to-sql opts
                 (update sel :ql/type (fn [x] (or x :pg/select))))))

(defmethod ql/to-sql
  :pg/cte-recursive
  [acc opts {with :with sel :select}]
  (-> acc
      (conj "WITH RECURSIVE")
      (ql/reduce-separated2 "," (fn [acc [k v]]
                                  (-> acc
                                      (into [(name k) "AS" "("])
                                      (ql/to-sql opts (update v :ql/type (fn [x] (or x :pg/select))))
                                      (conj ")")))
                            (if (sequential? with) (partition 2 with) with))
      (ql/to-sql opts
                 (update sel :ql/type (fn [x] (or x :pg/select))))))

(defmethod ql/to-sql
  :pg/jsonb_set
  [acc opts [_ doc pth val & [create-missed]]]
  (-> acc
      (conj "jsonb_set(")
      (ql/to-sql opts doc)
      (conj ",")
      (conj (to-array-litteral pth))
      (conj ",")
      (ql/to-sql opts val)
      (cond-> create-missed (conj ", true"))
      (conj ")")))

(defmethod ql/to-sql
  :pg/jsonb_string
  [acc opts [_ str]]
  (-> acc
      (conj "(jsonb_build_object('s',")
      (ql/to-sql opts str)
      (conj ")->'s')")))

(defmethod ql/to-sql
  :pg/to_jsonb
  [acc opts [_ expr]]
  (-> acc
      (conj "to_jsonb(")
      (ql/to-sql opts expr)
      (conj ")")))

(defmethod ql/to-sql
  :pg/row_to_json
  [acc opts [_ v]]
  (-> acc
      (conj "row_to_json(")
      (ql/to-sql opts v)
      (conj ")")))

(defmethod ql/to-sql
  :min
  [acc opts [_ v]]
  (-> acc
      (conj "min(")
      (ql/to-sql opts v)
      (conj ")")))

(defmethod ql/to-sql
  :max
  [acc opts [_ v]]
  (-> acc
      (conj "max(")
      (ql/to-sql opts v)
      (conj ")")))

(defmethod ql/to-sql
  :pg/row_to_jsonb
  [acc opts [_ v]]
  (-> acc
      (conj "row_to_json(")
      (ql/to-sql opts v)
      (conj ")::jsonb")))

(defmethod ql/to-sql
  :pg/jsonb_strip_nulls
  [acc opts [_ v]]
  (-> acc
      (conj "jsonb_strip_nulls(")
      (ql/to-sql opts v)
      (conj ")")))

(defmethod ql/to-sql
  :pg/array-strip-nulls
  [acc opts [_ expr]]
  (-> acc
      (conj "(select array_agg(a) from unnest(")
      (ql/to-sql opts expr)
      (conj ") a where a is not null)")))

(defmethod ql/to-sql
  :pg/alphanum
  [acc opts [_ expr]]
  (-> acc
      (conj "regexp_replace(")
      (ql/to-sql opts expr)
      (conj ",'[^a-zA-Z0-9]','', 'g')")))

(defmethod ql/to-sql
  :pg/parens
  [acc opts [_ expr]]
  (-> acc
      (conj "(")
      (ql/to-sql opts expr)
      (conj ")")))

(defmethod ql/to-sql
  :json_agg
  [acc opts [_ v]]
  (-> acc
      (conj "json_agg(")
      (ql/to-sql opts v)
      (conj ")")))

(defmethod ql/to-sql
  :jsonb_agg
  [acc opts [_ v]]
  (-> acc
      (conj "jsonb_agg(")
      (ql/to-sql opts v)
      (conj ")")))


(defmethod ql/to-sql
  :<>
  [acc opts [_ a b]]
  (-> acc
      (ql/to-sql opts a)
      (conj "<>")
      (ql/to-sql opts b)))


(defn concat-columns [^Iterable coll]
  (let [^String sep ", "
        ^Iterator iter (.iterator coll)
        builder (StringBuilder.)]
    (loop []
      (when (.hasNext iter)
        (let [s (.next iter)]
          (.append builder "\"") (.append builder (name s)) (.append builder "\"")
          (when (.hasNext iter)
            (.append builder sep)))
        (recur)))
    (.toString builder)))


(defmethod ql/to-sql
  :pg/insert-many
  [acc opts {tbl :into vls :values ret :returning on-conflict :on-conflict}]
  (let [cols (->> vls :keys)]
    (-> acc
       (conj "INSERT INTO")
       (conj (name tbl))
       (conj "(")
       (conj (concat-columns cols ))
       (conj ")")
       (ql/to-sql opts (with-meta vls {:ql/type :pg/values}))
       (cond->
           on-conflict (-> (conj "ON CONFLICT")
                           (ql/to-sql opts (assoc on-conflict :ql/type :pg/conflict-update)))
           ret (-> (conj "RETURNING")
                   (ql/to-sql opts ret))))))


(defn jackson-get-keys [^ObjectNode object]
  (let [it (.fieldNames object)]
    (loop [keys []]
      (if (.hasNext it)
        (recur (conj keys (.next it)))
        keys))))

(defn get-by-key [^ObjectNode node ^String key]
  (.get node key))

(defmethod ql/to-sql
  :pg/insert
  [acc opts {tbl :into as :as vls :value  ret :returning on-conflict :on-conflict}]
  (let [cols (if (= ObjectNode (type vls))
               (jackson-get-keys vls)
               (keys vls))]
    (-> acc
        (conj "INSERT INTO")
        (conj (name tbl))
        (cond->
            as (-> (conj "AS" (name as))))
        (conj "(")
        (conj (concat-columns cols ))
        (conj ")")
        (conj "VALUES")
        (conj "(")
        (ql/reduce-separated2
         ","
         (if (= ObjectNode (type vls))
           (fn [acc c] (ql/to-sql acc opts (get-by-key vls c)))
           (fn [acc c] (ql/to-sql acc opts (get vls c))))
         cols)
        (conj ")")
        (cond->
            on-conflict (-> (conj "ON CONFLICT")
                            (ql/to-sql opts (assoc on-conflict :ql/type :pg/conflict-update)))
            ret (-> (conj "RETURNING")
                    (ql/to-sql opts ret))))))

(defmethod ql/to-sql
  :resource||
  [acc opts [_ & exprs]]
  (-> acc
      (conj "resource ||")
      (ql/reduce-separated2 "||" (fn [acc expr] (ql/to-sql acc opts expr)) exprs)))

(defmethod ql/to-sql
  :as
  [acc opts [_ expr alias]]
  (-> acc
      (ql/to-sql opts expr)
      (conj "AS")
      (conj alias)))

(defmethod ql/to-sql
  :->>
  [acc opts [_ col k]]
  (-> acc
      (conj "(")
      (ql/to-sql opts col)
      (conj (str "->> " (ql/string-litteral (name k))))
      (conj ")")))

(defmethod ql/to-sql
  :->
  [acc opts [_ col k]]
  (-> acc
      (conj "(")
      (ql/to-sql opts col)
      (conj (str "-> " (ql/string-litteral (name k))))
      (conj ")")))

(defmethod ql/to-sql
  :#>>
  [acc opts [_ col path]]
  (-> acc
      (conj "(")
      (ql/to-sql opts col)
      (conj (str "#>> " (to-array-litteral path)))
      (conj ")")))

(defmethod ql/to-sql
  :#>
  [acc opts [_ col path]]
  (-> acc
      (conj "(")
      (ql/to-sql opts col)
      (conj (str "#> " (to-array-litteral path)))
      (conj ")")))

(defmethod ql/to-sql
  :pg/desc
  [acc opts [_ expr]]
  (-> acc
      (ql/to-sql opts expr)
      (conj "DESC")))

(defmethod ql/to-sql
  :pg/asc
  [acc opts [_ expr]]
  (-> acc
      (ql/to-sql opts expr)
      (conj "ASC")))

(defmethod ql/to-sql
  :pg/nulls-last
  [acc opts [_ expr]]
  (-> acc
      (ql/to-sql opts expr)
      (conj "NULLS LAST")))

(defmethod ql/to-sql
  :pg/nulls-first
  [acc opts [_ expr]]
  (-> acc
      (ql/to-sql opts expr)
      (conj "NULLS FIRST")))

(defmethod ql/to-sql
  :pg/array-agg
  [acc opts [_ expr]]
  (-> acc
      (conj "array_agg(")
      (ql/to-sql opts expr)
      (conj ")")))

(defmethod ql/to-sql
  :pg/array-length
  [acc opts [_ expr dim]]
  (-> acc
      (conj "array_length(")
      (ql/to-sql opts expr)
      (conj (str ", " dim ")"))))

(defmethod ql/to-sql
  :pg/jsonb-array-length
  [acc opts [_ expr]]
  (-> acc
      (conj "jsonb_array_length(")
      (ql/to-sql opts expr)
      (conj ")")))

(defmethod ql/to-sql
  :pg/jsonb-path-query-array
  [acc opts [_ & exprs]]
  (conj (ql/reduce-separated ","
                             (conj acc "jsonb_path_query_array(")
                             (fn [acc expr]
                               (-> acc
                                   (ql/to-sql opts expr)))
                             exprs)
        ")"))

(defmethod ql/to-sql
  :pg/call
  [acc opts [_ f & exprs]]
  (conj (ql/reduce-separated ","
                             (conj acc (str (name f) "("))
                             (fn [acc expr]
                               (-> acc
                                   (ql/to-sql opts expr)))
                             exprs)
        ")"))

(defmethod ql/to-sql
  :pg/sum
  [acc opts [_ expr]]
  (-> acc
      (conj "sum( ")
      (ql/to-sql opts expr)
      (conj ")")))

(defmethod ql/to-sql
  :pg/list
  [acc opts data]
  (->> (rest data)
       (ql/reduce-separated "," acc
                            (fn [acc expr]
                              (-> acc
                                  (ql/to-sql opts expr))))))

(defmethod ql/to-sql
  :int
  [acc opts [_ expr]]
  (-> acc
      (conj "(")
      (ql/to-sql opts expr)
      (conj ")::int")))

(defmethod ql/to-sql
  :numeric
  [acc opts [_ expr]]
  (-> acc
      (conj "(")
      (ql/to-sql opts expr)
      (conj ")::numeric")))

(defmethod ql/to-sql
  :date
  [acc opts [_ expr]]
  (-> acc
      (conj "(")
      (ql/to-sql opts expr)
      (conj ")::date")))

(defmethod ql/to-sql
  :pg/nth
  [acc opts [_ expr idx]]
  (-> acc
      (conj "(")
      (ql/to-sql opts expr)
      (conj ")[")
      (ql/to-sql opts idx)
      (conj "]")))

(defmethod ql/to-sql
  :pg/any
  [acc opts [_ expr]]
  (-> acc
      (conj "ANY(")
      (ql/to-sql opts expr)
      (conj ")")))

(defmethod ql/to-sql :/
  [acc opts [_ & args]]
  (operator acc opts "/" args))

(defmethod ql/to-sql :*
  [acc opts [_ & args]]
  (operator acc opts "*" args))

(defmethod ql/to-sql :+
  [acc opts [_ & args]]
  (operator acc opts "+" args))

(defmethod ql/to-sql :<
  [acc opts [_ & args]]
  (operator acc opts "<" args))

(defmethod ql/to-sql :<=
  [acc opts [_ & args]]
  (operator acc opts "<=" args))

(defmethod ql/to-sql :>
  [acc opts [_ & args]]
  (operator acc opts ">" args))

(defmethod ql/to-sql :>=
  [acc opts [_ & args]]
  (operator acc opts ">=" args))

(defmethod ql/to-sql :pg/identifier
  [acc opts [_ id & args]]
  (conj acc (ql/escape-ident opts id)))

(defmethod ql/to-sql :pg/escape-ident
  [acc opts [_ id & args]]
  (conj acc (ql/escape-ident-alt opts id)))

(defmethod ql/to-sql :pg/extract
  [acc opts [_ field-expr from]]
  (-> acc
      (conj "EXTRACT(")
      (ql/to-sql opts field-expr)
      (conj "from")
      (ql/to-sql opts from)
      (conj ")")))

(defmethod ql/to-sql :-
  [acc opts [_ & args]]
  (dsql.pg/operator acc opts "-" args))

(defmethod ql/to-sql :pg/values
  [acc opts {vls :values, ks :keys}]
  (if ks
    (let [extract-keys (apply juxt ks)]
      (-> acc
          (conj "VALUES")
          (conj "(")
          (ql/reduce-separated2
            ") , ("
            (fn [acc c]
              (ql/reduce-separated2
                acc ","
                (fn [acc c] (ql/to-sql acc opts c))
                (extract-keys c)))
            vls)
          (conj ")")))
    (-> acc
        (conj "VALUES")
        (conj "(")
        (ql/reduce-separated2 ") , ("
                              (fn [acc c] (ql/to-sql acc opts c))
                              vls)
        (conj ")"))))

(defmethod ql/to-sql
  :pg/build-sql-str
  [acc opts [_ exprs]]
  (into acc
        (ql/reduce-acc []
                       (fn [acc expr]
                         (if (string? expr)
                           (if (string? (peek acc))
                             (conj (pop acc)
                                   (str (peek acc) expr))
                             (conj acc expr))
                           (ql/to-sql acc opts expr)))
                       exprs)))

(defmethod ql/to-sql :pg/create-server
  [acc opts {:keys [fdw options] ifne :if-not-exists :as node}]
  (-> acc
      (conj "CREATE SERVER")
      (cond-> ifne (conj "IF NOT EXISTS"))
      (conj (name (:name node)))
      (cond-> fdw (conj "FOREIGN DATA WRAPPER" fdw ))
      (cond-> options (conj "OPTIONS (" (mk-options options) ")"))))

(defmethod ql/to-sql :pg/create-user-mapping
  [acc opts {:keys [user server options] ifne :if-not-exists  :as node}]
  (-> acc
      (conj "CREATE USER MAPPING")
      (cond-> ifne (conj "IF NOT EXISTS"))
      (conj "FOR"  (name user))
      (conj "SERVER" (name server))
      (cond-> options (conj "OPTIONS" "(" (mk-options options) ")"))))

(defn parse-param
  "Use keyword as a value if you don't want to make it a parameter."
  [[alias value]]
  (if (keyword? value)
    [(name value) (name alias)]
    [["?" value] (name alias)]))

(defn parse-column [column]
  (cond
    (keyword? column) [(name column)]
    (vector? column) (parse-param column)))

(defmethod ql/to-sql
  :pg/columns
  [acc _opts node]
  (into [] (concat acc (join-vec "," (->> (rest node) (mapv parse-column))))))
