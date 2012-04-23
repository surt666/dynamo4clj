(ns dynamo4clj.core
  (:use [clojure.algo.generic.functor :only (fmap)]
        [clojure.walk :only (stringify-keys keywordize-keys)])
  (:import [com.amazonaws.auth STSSessionCredentialsProvider AWSCredentials BasicAWSCredentials PropertiesCredentials]
           [com.amazonaws.services.dynamodb AmazonDynamoDBClient]
           [com.amazonaws.services.dynamodb.model AttributeValue AttributeValueUpdate AttributeAction PutItemRequest QueryRequest Key GetItemRequest DeleteItemRequest ScanRequest UpdateItemRequest ReturnValue Condition ComparisonOperator KeysAndAttributes BatchGetItemRequest BatchGetItemResult BatchResponse]
           [com.amazonaws AmazonServiceException ClientConfiguration Protocol]
           [java.util HashMap Properties]))

(def refresh (* 1000 60 40)) ; 40 minutes

(defn get-client 
  ([]
   "Configures a client from aws.properties and config.properties"
   (let [credstream (.getResourceAsStream (clojure.lang.RT/baseLoader) "aws.properties")
         configstream (.getResourceAsStream (clojure.lang.RT/baseLoader) "config.properties") 
         config (ClientConfiguration.)
         provider-config (ClientConfiguration.)
         props (Properties.)]
     (. props (load configstream))
     (. config (setMaxErrorRetry 3))
     (. provider-config (setMaxErrorRetry 3))
     (. provider-config (setProtocol Protocol/HTTPS)) ; provider-config must use https
     (when-not (nil? (. props (getProperty "proxy-host"))) (. config (setProxyHost (. props (getProperty "proxy-host"))))(. provider-config (setProxyHost (. props (getProperty "proxy-host")))))
     (when-not (nil? (. props (getProperty "proxy-port"))) (. config (setProxyPort (Integer/parseInt (. props (getProperty "proxy-port")))))(. provider-config (setProxyPort (Integer/parseInt (. props (getProperty "proxy-port"))))) ) 
     (if (= (. props (getProperty "protocol")) "https") (. config (setProtocol Protocol/HTTPS)) (. config (setProtocol Protocol/HTTP)))     
     (let [provider (STSSessionCredentialsProvider.  (PropertiesCredentials. credstream) provider-config)
           client-map  {:session-provider provider :time (System/currentTimeMillis)  :client (doto (AmazonDynamoDBClient. provider config) (.setEndpoint (. props (getProperty "region"))))}]
       (atom client-map ))))

  ([{:keys [access-key secret-key proxy-host proxy-port protocol region] :as configuration}]  
   "Configures a client
   :access-key mandatory 
   :secret-key mandatory 
   :region mandatory (ex. for europe dynamodb.eu-west-1.amazonaws.com )  
   :proxy-host optional 
   :proxy-port integer  optional
   :protocol http|https optional
   "
   (let [config (ClientConfiguration.)
         provider-config (ClientConfiguration.)]
     (if (= protocol "https") (. config (setProtocol Protocol/HTTPS)) (. config (setProtocol Protocol/HTTP)))  
     (. config (setMaxErrorRetry 3)) 
     (. provider-config (setProtocol Protocol/HTTPS)) ; provider-config must use https
     (. provider-config (setMaxErrorRetry 3)) 
     (when proxy-host (. config (setProxyHost proxy-host ))(. provider-config (setProxyHost proxy-host )))
     (when (number? proxy-port) (. config (setProxyPort proxy-port))(. provider-config (setProxyPort proxy-port)))
     (let [provider (STSSessionCredentialsProvider. (BasicAWSCredentials. access-key secret-key) provider-config)
           client-map  {:session-provider provider
                        :time (System/currentTimeMillis)  
                        :client (doto (AmazonDynamoDBClient. provider config) (.setEndpoint region))}]
       (atom client-map )))))

(defn- ^AmazonDynamoDBClient refresh-client [client-atom]
  (let [{:keys [client time session-provider] :as client-map} @client-atom
        now (System/currentTimeMillis)]
    (when (> now (+ time refresh))
      (.refresh ^STSSessionCredentialsProvider session-provider) 
      (swap! client-atom #(assoc % :time now )))
    client))

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


(defn get-item [client table hash-key & [range-key]]
  "Retrieve an item from a table by its hash key."
  (let [key (if range (item-key-range hash-key range-key) (item-key hash-key))
        ires (. (refresh-client client) (getItem (doto (GetItemRequest.) (.withTableName table) (.withKey key))))
        item (keywordize-keys (to-map (.getItem ires)))]
    (when item  (with-meta item {:consumed-capacity-units (.getConsumedCapacityUnits ires)}))))

(defn- create-keys-and-attributes [keys] 
  (let [ka (KeysAndAttributes.)]
    (if (vector? (first keys))
      (. ka (withKeys ^java.util.Collection (map #(item-key-range (% 0) (% 1)) keys)))
      (. ka (withKeys ^java.util.Collection (map #(item-key %) keys))))))

(defn- get-request-items [requests]
  (loop [r requests res {}]
    (if (empty? r)
      res
      (recur (rest r) (assoc res ((first r) 0) (create-keys-and-attributes ((first r) 1)))))))

(defn get-batch-items [client requests]
  "requests is a vector of vectors of the following form [[table1 [hash1 hash3]] [table2 [[hash1 range1] [hash4 range4]]]]"
  (let [ri (get-request-items requests)
        batchresult (. (refresh-client client)(batchGetItem (doto (BatchGetItemRequest.) (.withRequestItems ri))))
        tables (map #(first %) requests)]
    (loop [t tables res {}]
      (if (empty? t)
        (keywordize-keys res)
        (recur (rest t) (let [^BatchResponse bres (. (. batchresult getResponses) (get (first t)))]
                          (assoc res (first t) (with-meta (vec (map to-map (.getItems bres)))
                                                 {:consumed-capacity-units (.getConsumedCapacityUnits bres)}))))))))

;;TODO specify returnvalues for all calls

(defn delete-item [client table hash-key & [range-key]]
  "Delete an item from a table by its hash key and optional range-key. Return old value"  
  (let [req (if range-key
              (doto (DeleteItemRequest.) (.withTableName table) (.withKey (item-key-range hash-key range-key)) (.withReturnValues ReturnValue/ALL_OLD))
              (doto (DeleteItemRequest.) (.withTableName table) (.withKey (item-key hash-key)) (.withReturnValues ReturnValue/ALL_OLD)))
        dres (. (refresh-client client)(deleteItem req))
        attr (.getAttributes dres)]
    (with-meta (keywordize-keys (to-map attr)) {:consumed-capacity-units (.getConsumedCapacityUnits dres)})))


(defn insert-item [client table item]
  "Insert item (map) in table. Returns empty map if new key, else returns the old value"
  (let [req (doto (PutItemRequest.) (.withTableName table) (.withItem (fmap to-attr-value (stringify-keys item))) (.withReturnValues ReturnValue/ALL_OLD))      
        pres (. (refresh-client client) (putItem req))
        attr (.getAttributes pres)]    
    (with-meta (if attr (keywordize-keys (to-map attr)) {}) {:consumed-capacity-units (.getConsumedCapacityUnits pres)})))


(defn update-item [client table hash-key attr & [range-key]]
  "Update item (map) in table with optional attributes"
  (let [key (if range-key (item-key-range hash-key range-key) (item-key hash-key))
        attrupd (fmap to-attr-value-update (stringify-keys attr))
        req (doto (UpdateItemRequest.) (.withTableName table) (.withKey key) (.withReturnValues ReturnValue/ALL_NEW) (.withAttributeUpdates attrupd))
        ures (.  (refresh-client client) (updateItem req))]
    (with-meta (keywordize-keys (to-map (.getAttributes ures))) {:consumed-capacity-units (.getConsumedCapacityUnits ures)})))

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

(defn find-items
  "Find items with key and optional range. Range has the form [operator param1 param2] or [operator param1], and return-attributes is a vector of attributes to return as in [attr1 attr2]"
  ([client table key consistent range-condition return-attributes]                    
     (let [condition (create-condition range-condition)       
           reqq (cond
                 (empty? range-condition) (doto (QueryRequest.) (.withTableName table) (.withHashKeyValue (to-attr-value key)) (.withConsistentRead consistent))
                 (not (empty? range-condition)) (doto (QueryRequest.) (.withTableName table) (.withHashKeyValue (to-attr-value key)) (.withRangeKeyCondition condition) (.withConsistentRead consistent)))
           req (if (empty? return-attributes) reqq (doto reqq (.withAttributesToGet ^java.util.Collection (map #(name %) return-attributes))))]
       (let [qres (.  (refresh-client client) (query req))]
         (with-meta (keywordize-keys (map to-map (.getItems qres)))
           {:consumed-capacity-units (.getConsumedCapacityUnits qres) :count (.getCount qres) :last-key (.getLastEvaluatedKey qres)}))))
  ([client table key consistent range-condition]     
     (find-items client table key consistent range-condition nil))
  ([client table key consistent]     
     (find-items client table key consistent nil nil)))

(defn scan
  "Return the items in a DynamoDB table. Conditions is vector of tuples like [field operator param1 param2] or [field operator param1]. Return-attributes is a vector of attributes to return as in [attr1 attr2]"
  ([client table conditions return-attributes]  
     (let [conds (loop [c (first conditions) res {}]
                   (if (empty? c)
                     res
                     (recur (rest c) (assoc res (first (first c)) (create-condition (vec (rest (first c))))))))]
       (let [reqq (cond
                   (empty? conds) (doto (ScanRequest.) (.withTableName table))
                   (not (empty? conds)) (doto (ScanRequest.) (.withTableName table) (.withScanFilter conds)))
             req (if (empty? return-attributes) reqq (doto reqq (.withAttributesToGet ^java.util.Collection (map #(name %) return-attributes))))]
         (let [sres (.  (refresh-client client) (scan req))]
           (with-meta (keywordize-keys (map to-map (.getItems sres)))
             {:consumed-capacity-units (.getConsumedCapacityUnits sres) :count (.getCount sres) :last-key (.getLastEvaluatedKey sres)})))))
  ([client table conditions]
     (scan client table conditions nil))
  ([client table]
     (scan client table nil nil)))
