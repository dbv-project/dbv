(ns dbv.create-table
  (:require [honey.sql.helpers :as h]
            [honey.sql :as sql]
            [dbv.db-type :as db-type]
            ))


(defn create-table
  [{:keys [table]}]
  (let [columns (sort
                 (map (juxt (comp name
                                  key)
                            (comp :column-type
                                  val))
                      db-type/db-types))
        tuples (mapcat
                (fn [n]
                  (map
                   (fn [[column-name column-type]]
                     [(str column-name n)
                      column-type])
                   columns))
                (range 0 8))]
    (-> table
        (h/create-table)
        (h/with-columns
          (concat
           [["e" "uuid"]
            ["a" "uuid"]]
           columns
           tuples
           [["t" "bigint"]
            ["r" "bigint"]
            ]))))
  )


(comment

  (-> (create-table {:table :dbv})
      (sql/format))

  (count (:with-columns (create-table {:table :dbv})))
  )
