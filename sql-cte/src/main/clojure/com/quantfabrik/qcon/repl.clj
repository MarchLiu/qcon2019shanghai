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

(defn train
  [eta cost]
  (jdbc/execute! db ["delete from ml.results where id > 0;"])
  (jdbc/execute! db ["alter sequence ml.results_id_seq restart;"])
  (jdbc/execute! db ["insert into ml.results(group_id, layer, idx, zeta, alpha) select group_id, layer, idx, zeta, alpha from ml.resolve();"])
  (loop [c (-> (jdbc/query db ["select ml.cost()"])
               first
               :cost)]
    (if (< c cost)
      (do
        (println "finish at " c)
        c)
      (do
        (println "down to " c)
        (jdbc/query db ["select * from ml.update_delta();"])
        (jdbc/query db ["select * from ml.update_partial_differential();"])
        (jdbc/query db ["select * from ml.train_once(?);" eta])
        (jdbc/execute! db ["delete from ml.results;"])
        (jdbc/execute! db ["alter sequence ml.results_id_seq restart;"])
        (jdbc/execute! db ["insert into ml.results(group_id, layer, idx, zeta, alpha) select group_id, layer, idx, zeta, alpha from ml.resolve();"])
        (recur (-> (jdbc/query db ["select ml.cost()"])
                   first
                   :cost))))))