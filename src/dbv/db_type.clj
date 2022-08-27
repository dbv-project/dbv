(ns dbv.db-type
  (:require [clojure.edn :as edn]))

(def db-types
  {:db.type/boolean
   {:column-type "boolean"
    :serialize identity
    :deserialize identity}

   :db.type/double
   {:column-type "float(16)"
    :serialize double
    :deserialize double}

   :db.type/float
   {:column-type "float(8)"
    :serialize float
    :deserialize float}

   ;; :db.type/fn ; not supported

   :db.type/instant
   {:column-type "timestamp"
    :serialize (fn [date]
                 (java.sql.Timestamp. (.getTime date)))
    :deserialize (fn [sql-timestamp]
                   (java.util.Date. (.getTime sql-timestamp)))}

   :db.type/keyword
   {:column-type "text"
    :serialize pr-str
    :deserialize edn/read-string}

   :db.type/long
   {:column-type "bigint"
    :serialize long
    :deserialize long}

   :db.type/ref
   {:column-type "uuid"
    :serialize identity
    :deserialize identity}

   :db.type/string
   {:column-type "text"
    :serialize identity
    :deserialize identity}

   :db.type/symbol
   {:column-type "text"
    :serialize pr-str
    :deserialize edn/read-string}

   ;; :db.type/tuple ; not supported yet

   :db.type/uuid
   {:column-type "uuid"
    :serialize identity
    :deserialize identity}})
