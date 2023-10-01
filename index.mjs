import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  PutCommand,
  UpdateCommand,
  DeleteCommand,
  QueryCommand,
} from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);
const tableName = "nocode";

export const handler = async (event, context) => {
  console.log(event);

  let body, response;
  let path = event.rawPath.substring(1).replaceAll("/", "#");
  console.log(path);

  let statusCode = 200;
  const headers = {
    "Content-Type": "application/json",
  };

  const getPartition = function(path) {
    return path.split("/").slice(0, 3).join("#");
  }

  const add = function() {

  };

  const get = function(tableName, path) {
    return dynamo.send(
      new QueryCommand({
        TableName: tableName,
        KeyConditionExpression:
          "PK = :pk AND begins_with (SK, :sk)",
        ExpressionAttributeValues: {
          ":pk": getPartition(path),
          ":sk": path
        },
      })
    );
  };

  const put = function(tableName, path, body) {
    return dynamo.send(
      new PutCommand({
        TableName: tableName,
        Item: {
          PK: getPartition(path),
          SK: path,
          ...body
        }
      })
    );
  };
  
  const del = function(tableName, path) {
    return dynamo.send(
      new DeleteCommand({
        TableName: tableName,
        Key: {
          PK: getPartition(path),
          SK: path
        },
      })
    );
  };

  try {
    switch (event.routeKey) {
      case "DELETE /v1/{proxy+}":
        await del(tableName, path); 
        body = `Deleted item: ${path}`;
        break;
      case "GET /v1/{proxy+}":
        body = await get(tableName, path);
        body = body.Items;
        break;
      case "POST /v1/{proxy+}":
        console.log(getPartition(path));
        console.log(path + "#" + "counter");

        let id;
        try {
          const response = await dynamo.send(
            new UpdateCommand({
              TableName: tableName,
              Key: {
                PK: getPartition(path),
                SK: path + "#" + "counter"
              },
              UpdateExpression: "SET #Increment = #Increment + :incr",
              ExpressionAttributeNames: {
                "#Increment": "Increment"
              },
              ExpressionAttributeValues: {
                ":incr": 1
              },
              ReturnValues: "UPDATED_NEW",
            })    
          );
          id = response.Attributes && response.Attributes.Increment ? response.Attributes.Increment : 0;
        } catch (err) {
          if (err.__type === "com.amazon.coral.validate#ValidationException") {
            id = 10004321;
            response = await put(tableName, path, {
              SK: path + "#" + "counter",
              Increment: id
            });
          }
        }

        path = (event.rawPath.substring(1) + "/" + id);

        body = {
          id: id,
          path: path,
          value: JSON.parse(event.body)
        };

        const tableValue = {
          id: body.id,
          PK: getPartition(body.path),
          SK: body.path.replaceAll("/", "#"),
          value: body.value
        };

        if (body.value.unique) {
          
        }

        response = await put(tableName, path, tableValue);

        console.log(response);
        break;
      case "PUT /v1/{proxy+}":
        body = event.body;
        response = await put(tableName, path, body);
        console.log(response);
        break;
      default:
        throw new Error(`Unsupported route: "${event.routeKey}"`);
    }
  } catch (err) {
    statusCode = 400;
    console.log(JSON.stringify(err));
    body = err.message;
  } finally {
    body = JSON.stringify(body);
    console.log(body);
  }

  return {
    statusCode,
    body,
    headers,
  };
};
