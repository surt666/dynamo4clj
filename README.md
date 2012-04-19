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

    (insert-item client "table" {:hash "hash" :range "range" :foo "bar"})

    (get-item client "table" "hash")
    
    (get-item client "table" "hash" "range")

    (get-batch-items client [["products" [1101001 1101101]] ["prices" [["ys" 1101001] ["kab" 1101201]]]])

    (delete-item client "seq" "events_seq")

    (update-item client "seq" "login_count" {:value [1 "add"]})

    (scan client "events")

    (scan client  "events" [["author" "eq" "steen"]] [:attr])

    (find-items client "events" "NORMAL" true)   

    (find-items client "events" "NORMAL" true ["between" 715 815])

    (find-items client "events" "NORMAL" true ["between" 715 815] [:attr1 :attr2])

Return values have meta data containing consumed units, count and lastkey where applicable.

Range and conditions support

between,contains,eq,ge,gt,le,lt,ne,not-contains,not-null,null,begins-with

Check the Amazon docs for info on which conditions works for query. All should work for scan.

## License

Copyright (C) 2012 

Distributed under the Eclipse Public License, the same as Clojure.
