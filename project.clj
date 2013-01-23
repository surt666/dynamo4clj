(defproject dynamo4clj "1.0.13"
  :description "Amazon DynamoDB API"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [com.amazonaws/aws-java-sdk "1.3.27"]
                 [org.clojure/algo.generic "0.1.0"]]

  :plugins [[yij/lein-plugins "1.0.12"]
            [swank-clojure "1.4.0"]]
  :warn-on-reflection true)
