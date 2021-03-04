// Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/io;
import ballerina/jballerina.java;
import ballerina/log;
import ballerina/regex;
import ballerinax/azure_cosmosdb as cosmosdb;

configurable string baseURL = ?;
configurable string masterToken = ?;

cosmosdb:Configuration config = {
    baseUrl: baseURL,
    masterOrResourceToken: masterToken
};
cosmosdb:DataPlaneClient azureCosmosClient = new(config);

public function main() {

    string csvFilePath1 = "./files/summary.csv";
    string databaseId = "my_database";
    string containerId = "my_container";

    map<json>[] jsonObjectArray = convertToJsonObjectArray(csvFilePath1);

    foreach var item in jsonObjectArray {
        var uuid = createRandomUUIDWithoutHyphens();
        string documentId = string `document_${uuid.toString()}`;
        json documentBody = {
            id: documentId
        };
        json merged = checkpanic documentBody.mergeJson(item);
        record {|string id; json ...;|} finalRec = checkpanic merged.cloneWithType(RecordType);
        string partitionKeyValue = string `"${documentId}"`;

        cosmosdb:Document|error result = azureCosmosClient->createDocument(databaseId, containerId, finalRec, 
            partitionKeyValue); 

        if (result is cosmosdb:Document) {
            log:print(result.toString());
            log:print("Success!");
        } else {
            log:printError(result.message());
        }
    }
}

# Create a random UUID removing the unnecessary hyphens which will interrupt querying opearations.
# 
# + return - A string UUID without hyphens
function createRandomUUIDWithoutHyphens() returns string {
    string? stringUUID = java:toString(createRandomUUID());
    if (stringUUID is string) {
        stringUUID = 'string:substring(regex:replaceAll(stringUUID, "-", ""), 1, 4);
        return stringUUID;
    } else {
        return "";
    }
}

function createRandomUUID() returns handle = @java:Method {
    name: "randomUUID",
    'class: "java.util.UUID"
} external;

function convertToJsonObjectArray(string filePath) returns map<json>[] {

    // Create a readable CSV channel from the provided path, panic if an error occurs.
    io:ReadableCSVChannel readableCsvChannel = checkpanic io:openReadableCsvFile(filePath);
    
    // Each record (i.e., a single line/entry) in the CSV file will be read as a `string[]`.
    // Define an array of `string[]` to hold all the records.
    string[][] records = [];

    // Read all the records from the provided file via the channel.
    while (readableCsvChannel.hasNext()) {
        // Attempting reading a single record, panic if an error occurs.
        string[] currentRecord = <string[]> checkpanic readableCsvChannel.getNext();

        // Add the read record to array of records.
        records[records.length()] = currentRecord;
    }

    // The first element of the array represents the column names.
    // We use the `.shift()` langlib method to REMOVE and retrieve it from the
    // array. After this operation, the array will not contain the column names, 
    // i.e., it would only contain the values.
    string[] columns = records.shift();
    int columnCount = columns.length();

    // Create an array of JSON objects from the array of `string[]`. The `.map()` iterable 
    // operation is used here. 
    // See https://ballerina.io/learn/by-example/functional-iteration.html for examples.
    map<json>[] objectArray = records.map(function(string[] currentRecord) returns map<json> {

        // This function takes a `string[]` and converts it to a JSON object (`map<json>`)
        // using the column names identified above.
        //
        // Where the `columns` array is ["id", "firstName", "lastName", "age"]
        // and the `currentRecord` array is ["1", "John", "Doe", "25"]
        // this function creates the following JSON object.
        //
        // {
        //     id: "1",
        //     firstName: "John",
        //     lastName: "Doe",
        //     age: "25"
        // }

        // For each record, do the following:
        // 1. Create an empty `map<json>` value that will be the object representation for the record.
        map<json> obj = {};

        // 2. Set the individual fields, using the relevant value in the `columns` array as the key
        // and the corresponding value in the `currentRecord` array as the value.
        foreach int index in 0 ..< columnCount {
            obj[columns[index]] = currentRecord[index];
        }

        return obj;
    });
    // Close the channel.
    checkpanic readableCsvChannel.close();

    return objectArray;
}

type RecordType record {|string id; json ...;|};
