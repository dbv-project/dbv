(ns dbv.query
  (:require [datalog.parser :as parser]
            [clojure.math.combinatorics :as combo]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [honey.sql :as sql]
            ))

(defn entid
  [db ident]
  (if (instance? java.util.UUID
                 ident)
    ident
    (get-in (:schema db)
            [ident
             :db/uuid])))

(defn prepare-sql
  [state]
  (-> state
      (select-keys [:select
                    :from])
      (assoc :where
             (into [:and]
                   (get-in state
                           [:where
                            :and])))))

(defn prepare-joins
  [state]
  (update-in
   state
   [:where
    :and]
   concat
   (map
    (fn [[k v]]
      [:=
       (keyword k)
       (keyword v)])
    (mapcat
     (fn [names]
       (combo/combinations names
                           2))
     (filter (fn [vals]
               (> (count vals)
                  1))
             (vals (:unify state)))))))

(defn prepare-select
  [state]
  (let [symbols (map :symbol
                     (get-in state
                             [:q
                              :qfind
                              :elements]))]
    (assoc state
           :select
           (map
            (fn [symbol]
              [(keyword (first (get-in state
                                       [:unify
                                        symbol])))
               (keyword (subs (str symbol)
                              1))])
            symbols))))

(defn value-column
  [db ident]
  (some-> db
          (get-in [:schema
                   ident
                   :db/valueType])
          name))

(defn prepare-clauses
  [db state clauses]
  (reduce
   (fn [state [n where-clause]]
     (cond
       (:fn where-clause)
       ;; {:fn {:symbol <}, :args [{:value 2015} {:symbol ?year}]}
       (update-in state
                  [:where
                   :and]
                  conj
                  (into [(get-in where-clause
                                 [:fn
                                  :symbol])]
                        (map
                         (fn [arg]
                           (cond
                             (:value arg)
                             (:value arg)

                             (:symbol arg)
                             (-> state
                                 (get-in [:unify
                                          (:symbol arg)])
                                 (first)
                                 (keyword))

                             :else
                             (ex-info "cannot handle fn"
                                      {:where-clause where-clause})
                             ))
                         (:args where-clause))))


       #_{:source {},
          :vars [{:symbol ?e}],
          :clauses
          [{:source {},
            :pattern
            [{:symbol ?e} {:value :release/year} {:value 2018}]}]}
       (:clauses where-clause)
       (update-in state
                  [:where
                   :and]
                  conj
                  [:not
                   (into [:in
                          (-> (get-in state
                                      [:unify
                                       (get-in where-clause
                                               [:vars
                                                0
                                                :symbol])])
                              (first)
                              (keyword))
                          (-> db
                              (prepare-clauses {}
                                               (:clauses where-clause))
                              (assoc :q
                                     {:qfind {:elements
                                              [{:symbol (get-in where-clause
                                                                [:vars
                                                                 0
                                                                 :symbol])}]}},)
                              (prepare-select)
                              (prepare-joins)
                              (prepare-sql)
                              )]
                         )])

       (:pattern where-clause)
       (let [[e a v] (:pattern where-clause)
             v-column (value-column db
                                    (:value a))
             state (cond
                     (:symbol e)
                     (update-in state
                                [:unify
                                 (:symbol e)]
                                (fn [s]
                                  (conj (or s
                                            #{})
                                        (keyword
                                         (str "c"
                                              n
                                              ".e")))))

                     :else
                     (throw (ex-info "cannot handle"
                                     {:where-clause where-clause})))
             state (cond
                     (:value a)
                     (update-in state
                                [:where
                                 :and]
                                conj
                                [:=
                                 (keyword
                                  (str "c"
                                       n
                                       ".a"))
                                 (entid db
                                        (:value a))])
                     :else
                     (throw (ex-info "cannot handle"
                                     {:where-clause where-clause})))
             state (cond
                     (:value v)
                     (update-in state
                                [:where
                                 :and]
                                conj
                                [:=
                                 (keyword
                                  (str "c"
                                       n
                                       "."
                                       v-column))
                                 (:value v)
                                 ])
                     (:symbol v)
                     (update-in state
                                [:unify
                                 (:symbol v)]
                                (fn [s]
                                  (conj (or s
                                            #{})
                                        (keyword
                                         (str "c"
                                              n
                                              "."
                                              v-column)))))
                     :else
                     (throw (ex-info "cannot handle"
                                     {:where-clause where-clause}))
                     )
             state (-> state
                       (update-in [:where
                                   :and]
                                  conj
                                  [:<=
                                   (keyword
                                    (str "c"
                                         n
                                         ".t"))
                                   (:t db)]
                                  [:or
                                   [:=
                                    (keyword
                                     (str "c"
                                          n
                                          ".r"))
                                    nil]
                                   [:>
                                    (keyword
                                     (str "c"
                                          n
                                          ".r"))
                                    (:t db)]
                                   ]))
             state (-> state
                       (update :from
                               conj
                               [(keyword (:table db))
                                (keyword (str "c"
                                              n))]))
             ]
         state
         )))
   state
   (map
    vector
    (range)
    clauses)))

(defn datalog->state
  [db datalog-query]
  (let [q (parser/parse datalog-query)]
    (-> (prepare-clauses db
                         {:q q}
                         (:qwhere q))
        (prepare-select)
        (prepare-joins)
        (prepare-sql))))

(defn q
  [{:keys [connectable query] :as params}]
  (jdbc/execute!
   connectable
   (-> (datalog->state params
                       query)
       (sql/format)
       )
   {:builder-fn rs/as-unqualified-maps}
   ))
