(ns dbv.example-bootstrap
  (:require [dbv.create-table :as create-table]
            [dbv.transact :as transact]
            [dbv.query :as query]
            [next.jdbc :as jdbc]
            [honey.sql :as sql]
            ))

(defn create-table!
  [{:keys [connectable] :as params}]
  (jdbc/execute-one!
   connectable
   (sql/format (create-table/create-table params))))

(comment
  (def params
    {:connectable (jdbc/get-connection {:dbtype "postgres"
                                        :dbname "testdb"
                                        :host "localhost"
                                        :port 5432
                                        :user "postgres"
                                        :password "postgres"})
     :table :dbv}
    )

  (create-table! params)

  (def schema
    {:release/name
     {:db/uuid #uuid "a1d35657-daac-4935-99a4-fcf86171e590"
      :db/valueType :db.type/string
      :db/cardinality :db.cardinality/one
      :db/doc "The name of the release"}

     :release/year
     {:db/uuid #uuid "992d20b1-4ce3-44f6-961c-a8f5f7bced51"
      :db/valueType :db.type/long
      :db/cardinality :db.cardinality/one
      :db/doc "The year of the release"}

     :release/artists
     {:db/uuid #uuid "1ea4ad6e-6046-4453-8eb0-d2b0cf797fe6"
      :db/valueType :db.type/ref
      :db/cardinality :db.cardinality/many
      :db/doc "The set of artists contributing to the release"}

     :artist/name
     {:db/uuid #uuid "941b15d2-6746-4c59-88ba-f0e1083e734c"
      :db/valueType :db.type/string
      :db/cardinality :db.cardinality/one
      :db/doc "The artist's name"}
     })

  (def sample-tx
    [{:db/uuid #uuid "2688687b-895f-4d2f-b20b-d5ca494a7e62"
      :release/name "Take Care (Deluxe)"
      :release/artists #{#uuid "b548f534-c8ce-4322-90a0-2af47cb652da"}
      :release/year 2011}

     {:db/uuid #uuid "b9740778-0e7f-4c0d-905e-b4e6d28a9dac"
      :release/name "Scorpion"
      :release/artists #{#uuid "b548f534-c8ce-4322-90a0-2af47cb652da"}
      :release/year 2018}

     {:db/uuid #uuid "b548f534-c8ce-4322-90a0-2af47cb652da"
      :artist/name "Drake"}

     ])

  (def params*
    (assoc params
           :schema schema))

  (transact/transact! (assoc
                       params*
                       :data sample-tx))

  (query/q (assoc params*
                  :t 0
                  :query '[:find
                           ?e ?name
                           :where
                           [?e :release/year 2018]
                           [?e :release/name ?name]
                           ]))

  (query/q (assoc params*
                  :t 0
                  :query '[:find
                           ?e ?name
                           :where
                           [?e :release/name ?name]
                           ]))
  )
