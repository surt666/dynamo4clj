# dynamo4clj

Clojure API for Amazon DynamoDB

## Usage

### with config files
Put a file called `aws.properties` in src (or somewhere else where it ends up in the root of the classpath) with your aws keys in the format:

accessKey:blabla    
secretKey:fooooo    

Optionally put a `config.properties` file in src with the following optional parameters

proxy-host=sltarray   
proxy-port=8080  
region=dynamodb.eu-west-1.amazonaws.com  
protocol=https

    (def client (get-client))


### with config map 

    (def client (get-client {:access-key "foo"
                             :secret-key "bar"
                             :proxy-host "example.com" 
                             :proxy-port 8080 
                             :region "dynamodb.eu-west-1.amazonaws.com"
                             :protocol "https"}))

### api-calls 

    (insert-item client "table" {:hash "hash-key" :range "range-key" :foo "bar"})

    (get-item client "table" "hash-key")
    
    (get-item client "table" "hash-key" "range-key")

    (get-batch-items client [[table1 [hash1 hash3]] [table2 [[hash1 range1] [hash4 range4]]]])

    (delete-item client "table" "hash-key")
    
    (delete-item client "table" "hash-key" "range-key")

    (update-item client "table" "hash-key" {:value [1 "add"]})
     
    (update-item client "table" "hash-key" {:value [1 "add"]} "range-key")

    (scan client "table")

    (scan client  "table" [["author" "eq" "steen"]] [:attr])

    (find-items client "table" "NORMAL" true)   

    (find-items client "table" "NORMAL" true ["between" 715 815])

    (find-items client "table" "NORMAL" true ["between" 715 815] [:attr1 :attr2])
    
    (find-items client "table" "NORMAL" true ["between" 715 815] [:attr1 :attr2] false)
    
    (find-items client "table" "NORMAL" true ["between" 715 815] [:attr1 :attr2] false 1)
    
    (find-items client "table" "NORMAL" true ["between" 715 815] [:attr1 :attr2] false 1 "hash")
    
    (find-items client "table" "NORMAL" true ["between" 715 815] [:attr1 :attr2] false 1 ["hash" "range"])
    
    (batch-write client {:table1 [{:id \"foo1\" :key \"bar1\"} {:id \"foo2\" :key \"bar2\"}] :table2 [{:id2 \"foo1\" :key2 \"bar1\"} {:id2 \"foo2\" :key2 \"bar2\"}]})
    
    (batch-delete client {:table1 [\"hash1\" \"hash2\"] :table2 [[\"hash1\" \"range1\"] [\"hash2\" \"range2\"]]})

Return values have meta data containing consumed units, count and lastkey where applicable.

Range and conditions support

between,contains,eq,ge,gt,le,lt,ne,not-contains,not-null,null,begins-with

Check the Amazon docs for info on which conditions works for query. All should work for scan.

## License

Copyright (C) 2012 

Distributed under the Eclipse Public License, the same as Clojure.
