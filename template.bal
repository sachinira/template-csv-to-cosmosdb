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

    string[][] records = [];
    while (readableCsvChannel.hasNext()) {
        string[] currentRecord = <string[]> checkpanic readableCsvChannel.getNext();
        records[records.length()] = currentRecord;
    }

    string[] columns = records.shift();
    int columnCount = columns.length();

    map<json>[] objectArray = records.map(function(string[] currentRecord) returns map<json> {
        map<json> obj = {};
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
