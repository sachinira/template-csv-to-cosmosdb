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

const TYPE_CSV = "text/csv";
const NEW_LINE = "\n";
const COMMA = ",";

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
                    record {|string id; json ...;|} finalRecord = checkpanic merged.cloneWithType(RecordType);
                    string partitionKeyValue = string `"${documentId}"`;

                    cosmosdb:Document|error result = azureCosmosClient->createDocument(databaseId, containerId, 
                        finalRecord, partitionKeyValue); 
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
    string[][] arrayFromCsv = [];
    if (baseType == TYPE_CSV) {
        string textFromCsv = checkpanic bodyPart.getText();

        string[] rowArray = regex:split(textFromCsv, NEW_LINE);
        foreach var row in rowArray {
            arrayFromCsv.push(regex:split(row, COMMA));
        }
        string[] columns = arrayFromCsv.shift();
        int columnCount = columns.length();
        map<json>[] objectArray = arrayFromCsv.map(function(string[] currentRecord) returns map<json> {
            map<json> jsonObject = {};
            foreach int index in 0 ..< columnCount {
                jsonObject[columns[index]] = currentRecord[index];
            }
            return jsonObject;
        });
        return objectArray;
    } else {
        return error("Files are not CSV");
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
