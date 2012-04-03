(ns dynamo4clj.core
  (:use [clojure.algo.generic.functor :only (fmap)]
        [clojure.walk :only (stringify-keys keywordize-keys)])
  (:import [com.amazonaws.auth AWSCredentials BasicAWSCredentials PropertiesCredentials]
           [com.amazonaws.services.dynamodb AmazonDynamoDBClient]
           [com.amazonaws.services.dynamodb.model AttributeValue AttributeValueUpdate AttributeAction PutItemRequest QueryRequest Key GetItemRequest DeleteItemRequest ScanRequest UpdateItemRequest ReturnValue Condition ComparisonOperator KeysAndAttributes BatchGetItemRequest BatchGetItemResult BatchResponse]
           [com.amazonaws AmazonServiceException ClientConfiguration Protocol]
           [java.util HashMap Properties]))

(defn  get-client
  (^AmazonDynamoDBClient [] 
   "Configure client from aws.properties and config.properties"
   (let [credstream (.getResourceAsStream (clojure.lang.RT/baseLoader) "aws.properties")
         configstream (.getResourceAsStream (clojure.lang.RT/baseLoader) "config.properties") 
         creds (PropertiesCredentials. credstream)
         config (ClientConfiguration.)
         props (Properties.)]
     (. props (load configstream))
     (. config (setProtocol Protocol/HTTPS))
     (. config (setMaxErrorRetry 3))
     (when-not (nil? (. props (getProperty "proxy-host"))) (. config (setProxyHost (. props (getProperty "proxy-host")))))
     (when-not (nil? (. props (getProperty "proxy-port"))) (. config (setProxyPort (Integer/parseInt (. props (getProperty "proxy-port"))))))
     (doto (AmazonDynamoDBClient. creds config) (.setEndpoint (. props (getProperty "region"))))))
  (^AmazonDynamoDBClient [{:keys [access-key secret-key proxy-host proxy-port region] :as config}]  
   "Configures a client
   
   :access-key mandatory 
   :secret-key mandatory 
   :region mandatory (ex. for europe dynamodb.eu-west-1.amazonaws.com )  
   :proxy-host optional 
   :proxy-port integer  optional 
   "
   (let [creds (BasicAWSCredentials. access-key secret-key) 
         config (ClientConfiguration.)]
     (. config (setProtocol Protocol/HTTPS))  
     (. config (setMaxErrorRetry 3)) 
     (when proxy-host (. config (setProxyHost proxy-host )))
     (when (number? proxy-port) (. config (setProxyPort proxy-port)))
     (doto (AmazonDynamoDBClient. creds config) (.setEndpoint region )))))

;
;
; http://docs.amazonwebservices.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/STSSessionCredentialsProvider.html
;
;

(defn- to-attr-value [value]
  "Convert a value into an AttributeValue object."
  (cond
   (string? value) (doto (AttributeValue.) (.setS value))
   (number? value) (doto (AttributeValue.) (.setN (str value)))
   (and (set? value) (string? (first value))) (doto (AttributeValue.) (.setSS value))
   (and (set? value) (number? (first value))) (doto (AttributeValue.) (.setNS value))))

(defn- to-attr-value-update [value]
  "Convert a value into an AttributeValueUpdate object. Value is a tuple like [1 \"add\"]"
  (cond
   (= (get value 1) "add") (doto (AttributeValueUpdate.) (.withValue (to-attr-value (get value 0))) (.withAction AttributeAction/ADD))
   (= (get value 1) "delete") (doto (AttributeValueUpdate.) (.withValue (to-attr-value (get value 0))) (.withAction AttributeAction/DELETE))
   (= (get value 1) "put") (doto (AttributeValueUpdate.) (.withValue (to-attr-value (get value 0))) (.withAction AttributeAction/PUT))))

(defn- item-key [hash-key]
  "Create a Key object from a value."  
  (Key. (to-attr-value hash-key)))

(defn- item-key-range [hash-key range-key]
  "Create a Key object from a value."  
  (doto (Key.) (.withHashKeyElement (to-attr-value hash-key)) (.withRangeKeyElement (to-attr-value range-key))))

(defn- get-value [^AttributeValue attr-value]
  "Get the value of an AttributeValue object."
  (or (.getS attr-value)
      (.getN attr-value)
      (.getNS attr-value)
      (.getSS attr-value)))

(defn- to-map [item]
  "Turn a item in DynamoDB into a Clojure map."
  (if item
    (fmap get-value (into {} item))))

(defn get-item [^AmazonDynamoDBClient client table hash-key]
  "Retrieve an item from a table by its hash key."
  (keywordize-keys
   (to-map
    (.getItem
     (. client (getItem (doto (GetItemRequest.) (.withTableName table)
                              (.withKey (Key. (to-attr-value hash-key))))))))))

(defn- create-keys-and-attributes [keys] 
  (let [ka (KeysAndAttributes.)]
    (if (vector? (first keys))
      (. ka (withKeys (map #(item-key-range (% 0) (% 1)) keys)))
      (. ka (withKeys (map #(item-key %) keys))))))

(defn- get-request-items [requests]
  (loop [r requests res {}]
    (if (empty? r)
      res
      (recur (rest r) (assoc res ((first r) 0) (create-keys-and-attributes ((first r) 1)))))))

(defn get-batch-items [^AmazonDynamoDBClient client requests]
  "requests is a vector of vectors of the following form [[table1 [hash1 hash3]] [table2 [[hash1 range1] [hash4 range4]]]]"
  (let [ri (get-request-items requests)
        batchresult (. client (batchGetItem (doto (BatchGetItemRequest.) (.withRequestItems ri))))
        tables (map #(first %) requests)]
    (loop [t tables res {}]
      (if (empty? t)
        (keywordize-keys res)
        (recur (rest t) (assoc res (first t) (vec (map to-map (.getItems (. (. batchresult getResponses) (get (first t))))))))))))

(defn delete-item [^AmazonDynamoDBClient client table hash-key]
  "Delete an item from a table by its hash key."  
  (. client (deleteItem (DeleteItemRequest. table (item-key hash-key)))))


(defn insert-item [^AmazonDynamoDBClient client table item]
  "Insert item (map) in table"
  (let [req (doto (PutItemRequest.) (.withTableName table) (.withItem (fmap to-attr-value (stringify-keys item))))]      
    (. client (putItem req))))


(defn update-item [^AmazonDynamoDBClient client table key attr]
  "Update item (map) in table with optional attributes"
  (let [key (doto (Key.) (.withHashKeyElement (to-attr-value key)))
        attrupd (fmap to-attr-value-update (stringify-keys attr))
        req (doto (UpdateItemRequest.) (.withTableName table) (.withKey key) (.withReturnValues ReturnValue/ALL_NEW) (.withAttributeUpdates attrupd))]
    (keywordize-keys (to-map (.getAttributes (. client (updateItem req)))))))

(defn- create-condition [c]
  (let [[operator param1 param2] c]
    (cond
     (= operator "between") (doto (Condition.) (.withComparisonOperator ComparisonOperator/BETWEEN) (.withAttributeValueList ^java.util.List (vector (to-attr-value param1) (to-attr-value param2))))
     (= operator "begins-with") (doto (Condition.) (.withComparisonOperator ComparisonOperator/BEGINS_WITH) (.withAttributeValueList ^java.util.List (vector (to-attr-value param1))))
     (= operator "contains") (doto (Condition.) (.withComparisonOperator ComparisonOperator/CONTAINS) (.withAttributeValueList ^java.util.List (vector (to-attr-value param1))))
     (= operator "eq") (doto (Condition.) (.withComparisonOperator ComparisonOperator/EQ) (.withAttributeValueList ^java.util.List (vector (to-attr-value param1))))
     (= operator "ge") (doto (Condition.) (.withComparisonOperator ComparisonOperator/GE) (.withAttributeValueList ^java.util.List (vector (to-attr-value param1))))
     (= operator "gt") (doto (Condition.) (.withComparisonOperator ComparisonOperator/GT) (.withAttributeValueList ^java.util.List (vector (to-attr-value param1))))
     (= operator "le") (doto (Condition.) (.withComparisonOperator ComparisonOperator/LE) (.withAttributeValueList ^java.util.List (vector (to-attr-value param1))))
     (= operator "lt") (doto (Condition.) (.withComparisonOperator ComparisonOperator/LT) (.withAttributeValueList ^java.util.List (vector (to-attr-value param1))))
     (= operator "ne") (doto (Condition.) (.withComparisonOperator ComparisonOperator/NE) (.withAttributeValueList ^java.util.List (vector (to-attr-value param1))))
     (= operator "not-contains") (doto (Condition.) (.withComparisonOperator ComparisonOperator/NOT_CONTAINS) (.withAttributeValueList ^java.util.List (vector (to-attr-value param1))))
     (= operator "not-null") (doto (Condition.) (.withComparisonOperator ComparisonOperator/NOT_NULL) (.withAttributeValueList ^java.util.List (vector (to-attr-value param1))))
     (= operator "null") (doto (Condition.) (.withComparisonOperator ComparisonOperator/NULL) (.withAttributeValueList ^java.util.List (vector (to-attr-value param1))))
     (= operator "in") (doto (Condition.) (.withComparisonOperator ComparisonOperator/IN) (.withAttributeValueList ^java.util.List (vector (to-attr-value param1)))))))

(defn find-items [^AmazonDynamoDBClient client table key consistent & range]
  "Find items with key and optional range. Range has the form [operator param1 param2] or [operator param1]"
  (let [condition (create-condition (first range))       
        req (cond
             (empty? range) (doto (QueryRequest.) (.withTableName table) (.withHashKeyValue (to-attr-value key)) (.withConsistentRead consistent))
             (not (empty? range)) (doto (QueryRequest.) (.withTableName table) (.withHashKeyValue (to-attr-value key)) (.withRangeKeyCondition condition) (.withConsistentRead consistent)))]
    (keywordize-keys (map to-map (.getItems (. client (query req)))))))

(defn  scan [^AmazonDynamoDBClient client table & conditions]
  "Return the items in a DynamoDB table. Conditions is vector of tuples like [field operator param1 param2] or [field operator param1]"
  (let [conds (loop [c (first conditions) res {}]
                (if (empty? c)
                  res
                  (recur (rest c) (assoc res (first (first c)) (create-condition (vec (rest (first c))))))))]
    (let [req (cond
                (empty? conds) (doto (ScanRequest.) (.withTableName table))
                (not (empty? conds)) (doto (ScanRequest.) (.withTableName table) (.withScanFilter conds)))]
      (keywordize-keys (map to-map (.getItems (. client (scan req))))))))
