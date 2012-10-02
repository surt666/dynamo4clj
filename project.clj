(defproject dynamo4clj "1.0.12"
  :description "Amazon DynamoDB API"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [com.amazonaws/aws-java-sdk "1.3.14"]
                 [org.clojure/algo.generic "0.1.0"]]
  :dev-dependencies [[swank-clojure "1.4.0"]
                    [yij/lein-plugins "1.0.5"]]
  :warn-on-reflection true)
