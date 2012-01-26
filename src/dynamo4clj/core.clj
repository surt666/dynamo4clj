(ns dynamo4clj.core
  (:import [com.amazonaws.auth AWSCredentials PropertiesCredentials]
           [com.amazonaws.services.dynamodb AmazonDynamoDBClient]
           [com.amazonaws.services.dynamodb.model AttributeValue PutItemRequest QueryRequest ]
           [com.amazonaws AmazonServiceException ClientConfiguration Protocol]
           [java.util HashMap]))

(defn get-client []
  (let [creds (PropertiesCredentials. (.getResourceAsStream (clojure.lang.RT/baseLoader) "aws.properties"))
        config (ClientConfiguration.)]
    (comment (. config (setProxyHost "tglarray06.tdk.dk"))
             (. config (setProxyPort 8080)))
    (. config (setProtocol Protocol/HTTP))
    (. config (setMaxErrorRetry 3))
    (AmazonDynamoDBClient. creds config)))

(def client (get-client))

(defn insert [data]
  (let [hmap (HashMap.)]
    (doall (map #(doto hmap (.put (name %) (doto (AttributeValue.) (.withS (get data %))))) (keys data)))
    (let [req (doto (PutItemRequest.) (.withTableName "abonnement") (.withItem hmap))]      
      (doto client (.putItem req)))))

(defn find-abon [id]
  (let [req (doto (QueryRequest.) (.withTableName "abonnement") (.withHashKeyValue (doto (AttributeValue.) (.withS id))))]
    (map #(prn %) (.getItems (. client (query req))))))