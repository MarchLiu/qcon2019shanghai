(defproject sql-cte "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [liu.mars/jaskell "0.2.7"]
                 [org.clojure/java.jdbc "0.7.9"]
                 [org.postgresql/postgresql "42.2.5"]
                 [clj-postgresql "0.7.0"]
                 [org.clojure/core.async "0.4.490"]
                 [cheshire "5.8.1"]
                 [clojure.java-time "0.3.2"]]
  :source-paths ["src/main/clojure"]
  :repl-options {:init-ns com.quantfabrik.qcon.repl})
