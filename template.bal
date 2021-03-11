import ballerina/http;
import ballerina/io;
import ballerina/jballerina.java;
import ballerina/log;
import ballerina/mime;
import ballerina/regex;
import ballerinax/azure_cosmosdb as cosmosdb;

configurable string baseURL = ?;
configurable string masterToken = ?;
configurable string databaseId = ?;
configurable string containerId = ?;

cosmosdb:Configuration config = {
    baseUrl: baseURL,
    masterOrResourceToken: masterToken
};
cosmosdb:DataPlaneClient azureCosmosClient = new(config);

service / on new http:Listener(8090) {
    resource function post uploader(http:Caller caller, http:Request request) returns error? {
        var bodyParts = request.getBodyParts();
        if (bodyParts is mime:Entity[]) {
            foreach var part in bodyParts {
                map<json>[] jsonObjectArray = check convertToJsonObjectArray(part);
                foreach var item in jsonObjectArray {
                    var uuid = createRandomUUIDWithoutHyphens();
                    string documentId = string `document_${uuid.toString()}`;
                    json documentBody = {
                        id: documentId
                    };
                    json merged = checkpanic documentBody.mergeJson(item);
                    record {|string id; json ...;|} finalRec = checkpanic merged.cloneWithType(RecordType);
                    string partitionKeyValue = string `"${documentId}"`;

                    cosmosdb:Document|error result = azureCosmosClient->createDocument(databaseId, containerId, 
                        finalRec, partitionKeyValue); 
                    if (result is cosmosdb:Document) {
                        log:print("Success!");
                    } else {
                         _ = check caller->respond(http:FAILED);
                    }
                }
            }
        } else {
            log:printError(bodyParts.message());
            io:println("Error in decoding multiparts!");
        }        
    }
}

function convertToJsonObjectArray(mime:Entity bodyPart) returns map<json>[]|error {
    mime:MediaType mediaType = check mime:getMediaType(bodyPart.getContentType());
    string baseType = mediaType.getBaseType();
    string[][] initial = [];
    if (baseType == "text/csv") {
        string array = checkpanic bodyPart.getText();

        string[] ss = regex:split(array, "\n");
        foreach var item in ss {
            initial.push(regex:split(item, ","));
        }
        string[] columns = initial.shift();
        int columnCount = columns.length();
        map<json>[] objectArray = initial.map(function(string[] currentRecord) returns map<json> {
            map<json> obj = {};
            foreach int index in 0 ..< columnCount {
                obj[columns[index]] = currentRecord[index];
            }
            return obj;
        });
        return objectArray;
    } else {
        return error("error");
    }
}

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

type RecordType record {|string id; json ...;|};
