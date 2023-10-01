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

  const add = function() {

  };

  const put = function(body) {
    return dynamo.send(
      new PutCommand({
        TableName: tableName,
        Item: {
          PK: path.split("#").slice(0, 3).join("#"),
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
          PK: path.split("#").slice(0, 3).join("#"),
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
        body = await dynamo.send(
          new QueryCommand({
            TableName: tableName,
            KeyConditionExpression:
              "PK = :pk AND begins_with (SK, :sk)",
            ExpressionAttributeValues: {
              ":pk": path.split("#").slice(0, 3).join("#"),
              ":sk": path
            },
          })
        );
        console.log(JSON.stringify(body, null, 2));
        body = body.Items;
        break;
      case "POST /v1/{proxy+}":
        console.log(path.split("#").slice(0, 3).join("#"));
        console.log(path + "#" + "counter");

        let id;
        try {
          const response = await dynamo.send(
            new UpdateCommand({
              TableName: tableName,
              Key: {
                PK: path.split("#").slice(0, 3).join("#"),
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
            response = await put({
              SK: path + "#" + "counter",
              Increment: id
            });
          }
        }

        path = (event.rawPath.substring(1) + "#" + id).replaceAll("/", "#");

        body = {
          id: id,
          partition: path.split("#").slice(0, 3).join("/"),
          path: path,
          value: JSON.parse(event.body)
        };

        response = await put({
          id: body.id,
          PK: body.partition,
          SK: body.path,
          value: body.value
        });

        console.log(response);
        break;
      case "PUT /v1/{proxy+}":
        response = await put(JSON.parse(event.body));
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
  }

  return {
    statusCode,
    body,
    headers,
  };
};
