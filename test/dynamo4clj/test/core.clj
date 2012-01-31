(ns dynamo4clj.test.core
  (:use [dynamo4clj.core]
        [clojure.test])
  (:import [java.util UUID]))

(defn- insert-event [author category title content]
  (let [seqno (Integer/parseInt (:value (update-item "seq" "event_count" {:value [1 "add"]})))
        id (str (UUID/randomUUID))]
    (insert-item "events" {:id id :seqno seqno :author author :category category :title title :content content})))

(comment (time (doall (pmap #(insert-event "steen" "events" (str "test" %) "{\"bla\" : 2}") (range 500 700)))))

(deftest replace-me ;; FIXME: write
  (is false "No tests have been written."))
