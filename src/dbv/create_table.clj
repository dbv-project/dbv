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
                     [(keyword (str column-name "_" n))
                      (keyword column-type)])
                   columns))
                (range 0 8))]
    (-> (or table
            :dbv)
        (h/create-table)
        (h/with-columns
          (concat
           [[:e :uuid]
            [:a :uuid]
            [:t :bigint]
            [:r :bigint]
            [:p :uuid]]
           (map (fn [[k v]]
                  [(keyword k)
                   (keyword v)])
                columns)
           tuples))))
  )


(comment

  (-> (create-table {:table :dbv})
      (sql/format))

  (count (:with-columns (create-table {:table :dbv})))
  )
