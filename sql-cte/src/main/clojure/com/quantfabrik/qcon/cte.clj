(ns com.quantfabrik.qcon.cte
  (:require [clj-postgresql.core :as pg]
            [clojure.java.jdbc :as jdbc]))

(def db (pg/pool :dbname "qcon"))

(defn rand-pid [id]
  (rand-int id))

(defn create-continuous
  "生成带有不连续末端的连续序列，模拟连续数据的并发随机写入效果"
  [db size tail rate]
  (dotimes [idx size]
    (let [continuous (- size tail)
          threshold (* tail rate)]
      (if (< idx continuous)
        (jdbc/execute! db ["insert into data(id) values(?)" idx])
        (when (> (rand-int tail) threshold)
          (jdbc/execute! db ["insert into data(id) values(?)" idx])))
      (when (zero? (mod idx 1000))
        (println "generate " idx))))
  (println "completed"))

(defn create-tree
  "生成随机结构的树，原理是为每个节点随机指定一个id小于它的节点为上级节点。"
  [db size]
  (dotimes [idx size]
    (let [id (-> db
                 (jdbc/query
                   ["insert into tree(pid) values(?) returning id"
                    (rand-pid idx)])
                 first
                 :id)]
      (when (zero? (mod id 10000))
        (println "generate " id))))
  (println "completed"))

(defn mark-level
  "为 data 表逐级标记级别信息"
  [db]
  (loop [level 1
         rows (jdbc/query db [(str "update tree set level=? "
                                   "where pid = 0 "
                                   "returning id") level])]
    (if (empty? (doall rows))
      (println "completed at level " (dec level))
      (let [next-level (inc level)]
        (println "refresh level " level " with pid " (into [] (map :id rows)))
        (recur next-level
               (jdbc/query db [(str "update tree set level=? "
                                    "where pid = any(?) and id != pid "
                                    "returning id") next-level
                               (into [] (map :id rows))]))))))
