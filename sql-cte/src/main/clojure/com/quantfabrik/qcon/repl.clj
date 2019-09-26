(ns com.quantfabrik.qcon.repl
  (:require [clojure.string :as cs]
            [clj-postgresql.core :as pg]
            [clojure.java.jdbc :as jdbc]))

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