(ns com.quantfabrik.qcon.repl
  (:require [clojure.string :as cs]
            [clj-postgresql.core :as pg]
            [clojure.java.jdbc :as jdbc]))

(def db (pg/spec :dbname "qcon"))

(defn parse-int
  [item]
  (Integer/parseInt item))

(defn load-data
  [path]
  (->> path
       slurp
       cs/split-lines
       (map #(cs/split % #"\s"))
       flatten
       (map parse-int)))

(defn load-all-data
  []
  (let [data (into {}
                   (for [idx (range 1 65)]
                     [idx (load-data (format "resources/data%d.txt" idx))]))
        db (pg/pool :dbname "qcon" :schema "ml")]
    (doseq [kv data]
      (doseq [[idx value] (map-indexed (fn [idx value] [idx value]) (val kv))]
        (jdbc/insert! db :ml.data {:group_id (key kv) :idx (inc idx) :value value})))))

(defn load-t
  [path]
  (let [data (->> path
                  slurp
                  cs/split-lines
                  rest
                  (map #(->> (cs/split % #"\s*,\s*")
                             (map parse-int)))
                  (map #(vector (first %) (vec (rest %))))
                  (into {}))
        db (pg/pool :dbname "qcon" :schema "ml")]
    (doseq [kv data]
      (jdbc/insert! db :ml.t {:group_id (key kv) :idx 1 :value (first (val kv))})
      (jdbc/insert! db :ml.t {:group_id (key kv) :idx 2 :value (second (val kv))}))))

(defn init-network
  []
  (jdbc/execute! db ["delete from ml.results where id > 0;"])
  (jdbc/execute! db ["alter sequence ml.results_id_seq restart;"])
  (jdbc/execute! db ["delete from ml.node;"])
  (jdbc/execute! db ["alter sequence ml.results_id_seq restart;"])
  (jdbc/execute! db ["insert into ml.node(network_id, version, layer, idx, w, b) select 1, 0, layer, idx, w, b from ml.rand_network('{12, 3, 2}'::int[]);"]))

(defn train-once
  [eta]
  (jdbc/query db ["select * from ml.update_delta();"])
  (jdbc/query db ["select * from ml.update_partial_differential();"])
  (jdbc/query db ["select * from ml.train_once(?);" eta])
  (jdbc/execute! db ["delete from ml.results;"])
  (jdbc/execute! db ["alter sequence ml.results_id_seq restart;"])
  (jdbc/execute! db ["insert into ml.results(group_id, layer, idx, zeta, alpha) select group_id, layer, idx, zeta, alpha from ml.resolve();"]))

(defn train
  [eta cost]
  (jdbc/execute! db ["delete from ml.results where id > 0;"])
  (jdbc/execute! db ["alter sequence ml.results_id_seq restart;"])
  (jdbc/execute! db ["insert into ml.results(group_id, layer, idx, zeta, alpha) select group_id, layer, idx, zeta, alpha from ml.resolve();"])
  (loop [eta eta
         c (-> (jdbc/query db ["select ml.cost()"])
               first
               :cost)
         prev (-> (jdbc/query db ["select ml.cost()"])
                  first
                  :cost
                  (+ 5))]
    (cond
      (< c cost) (do (println "finish at " c) c)
      (>= c prev) (let [e (/ eta 2)]
                    (println (format "reduce eta to %f and try down from %f" e c))
                    (train-once e)
                    (recur e
                           (-> (jdbc/query db ["select ml.cost()"])
                               first
                               :cost)
                           c))
      :else (do
              (println "down to " c)
              (train-once eta)
              (recur eta
                     (-> (jdbc/query db ["select ml.cost()"])
                         first
                         :cost)
                     c)))))

(defn binary
  [x]
  (let [d 0.3]
    (cond
      (< 0 (- 1 x) d) 1
      (< 0 x d) 0
      :else -1)))

(defn extract
  [entity]
  [(key entity)
   (->> entity
        val
        (map #(dissoc % :group_id))
        (group-by :idx))])

(defn res []
  (let [data (->> (jdbc/query
                    db
                    ["select group_id, idx, alpha from ml.resolve() where layer = 3;"])
                  (map #(update % :alpha binary))
                  (group-by :group_id)
                  (into (sorted-map) (map extract)))
        t (->> (jdbc/query
                 db
                 ["select  group_id, idx, value as t from ml.t"])
               (map #(update % :t int))
               (group-by :group_id)
               (into (sorted-map) (map extract)))]
    (merge-with (fn
                  [x y]
                  (merge-with #(-> (merge (first %1) (first %2))
                                   (dissoc :idx)) x y))
                data t)))