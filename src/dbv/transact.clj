(ns dbv.transact
  (:require [next.jdbc :as jdbc]
            [honey.sql :as sql]
            ))

(defn get-basis-t
  [{:keys [connectable table]}]
  (or
   (-> (jdbc/execute!
        connectable
        (sql/format {:select [[[:max :t]]]
                     :from [table]}))
       (first)
       (:max))
   -1))

(defn to-schema-by-uuid
  [schema]
  (into {}
        (map (fn [[db-ident entity-map]]
               [(:db/uuid entity-map)
                (assoc entity-map
                       :db/ident
                       db-ident)]))
        schema))

(defn prepare-transaction
  [{:keys [schema data]}]
  (mapcat
   (fn [x]
     (if (map? x)
       (mapcat
        (fn [[a v]]
          (map
           (fn [v*]
             {:e (:db/uuid x)
              :a (get-in schema
                         [a
                          :db/uuid])
              :v v*
              :added true})
           (case (get-in schema
                         [a
                          :db/cardinality])
             :db.cardinality/one
             [v]
             :db.cardinality/many
             (do
               (assert (coll? v))
               v))))
        (dissoc x
                :db/uuid))))
   data))

(defn datom-active?
  [{:keys [connectable table schema]} {:keys [e a v]}]
  (assert schema)
  (let [schema-by-uuid (to-schema-by-uuid schema)
        v-column (name (get-in schema-by-uuid
                               [a :db/valueType]))
        ]
    (boolean
     (seq
      (jdbc/execute!
       connectable
       (sql/format {:select [:e]
                    :from   [table]
                    :where  [:and
                             [:= :e e]
                             [:= :a a]
                             [:= (keyword v-column) v]
                             [:= :r nil]
                             ]}))))))

(defn inactive-datom!
  [{:keys [connectable table t]} {:keys [e a]}]
  (jdbc/execute!
   connectable
   (sql/format {:update table
                :set {:r t}
                :where [:and
                        [:= :e e]
                        [:= :a a]
                        [:= :r nil]]})))

(defn insert-datom!
  [{:keys [connectable table t schema]} {:keys [e a v]}]
  (let [schema-by-uuid (to-schema-by-uuid schema)]
    (jdbc/execute!
     connectable
     (sql/format
      {:insert-into [table]
       :columns [:e
                 :a
                 (keyword (name (get-in schema-by-uuid
                                        [a
                                         :db/valueType])))
                 :t
                 ]
       :values [[e
                 a
                 v
                 t]]}))))


(defn transact!
  [{:keys [schema data connectable] :as params}]
  (jdbc/with-transaction [tx
                          connectable
                          {:isolation :serializable}]
    (let [schema-by-uuid (to-schema-by-uuid schema)
          next-t (inc (get-basis-t params))]
      (doseq [datom (prepare-transaction params)]
        (let [{:keys [e a v added]} datom
              cardinality (get-in schema-by-uuid
                                  [a
                                   :db/cardinality])
              params* (assoc params
                             :connectable tx
                             :t next-t)]
          (case added
            true
            (case cardinality
              :db.cardinality/one
              (when-not (datom-active? params*
                                       datom)
                (inactive-datom! params*
                                 datom)
                (insert-datom! params*
                               datom)
                )
              :db.cardinality/many
              (when-not (datom-active? params*
                                       datom)
                (insert-datom! params*
                               datom)
                )
              )
            )))))
  )
