(ns dynamo4clj.core
  (:import [com.amazonaws.auth AWSCredentials PropertiesCredentials]
           [com.amazonaws.services.dynamodb AmazonDynamoDBClient]
           [com.amazonaws.services.dynamodb.model AttributeValue PutItemRequest]
           [com.amazonaws AmazonServiceException ClientConfiguration Protocol]
           [java.util HashMap]))

(defn get-client []
  (let [creds (PropertiesCredentials. (.getResourceAsStream (clojure.lang.RT/baseLoader) "aws.properties"))
        config (ClientConfiguration.)]
    (. config (setProxyHost "tglarray06.tdk.dk"))
    (. config (setProxyPort 8080))
    (. config (setProxyUsername "m00522"))
    (. config (setProxyPassword "skrid#nar20"))
    (. config (setProxyWorkstation "xpn55455"))
    (. config (setProtocol Protocol/HTTP))
    (. config (setMaxErrorRetry 3))
    (AmazonDynamoDBClient. creds config)))

(def client (get-client))

(defn insert [data]
  (let [hmap (HashMap.)
        attrv (AttributeValue.)]
    (doall (map #(. hmap (put (name %) (. attrv (withS (get data %))))) (keys data)))
    (let [req (PutItemRequest.)]
      (. req (withTableName "abonnement") (withItem hmap))
      (. client (putItem req)))))