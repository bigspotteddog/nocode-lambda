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

const getPartitionKey = function (path) {
  return path.substring(1).split("/").slice(0, 3).join("#");
}

const getSortKey = function (path) {
  return path.substring(1).replaceAll("/", "#");
}

export const get = function (tableName, path) {
  return dynamo.send(
    new QueryCommand({
      TableName: tableName,
      KeyConditionExpression:
        "PK = :pk AND begins_with (SK, :sk)",
      ExpressionAttributeValues: {
        ":pk": getPartitionKey(path),
        ":sk": getSortKey(path)
      },
    })
  );
};

export const checkUnique = function (tableName, path, unique) {
  const params = {
    TableName: tableName,
    IndexName: "PK-SK2-index",
    KeyConditionExpression:
      "PK = :pk AND SK2 = :sk2",
    ExpressionAttributeValues: {
      ":pk": getPartitionKey(path),
      ":sk2": unique
    }
  };
  console.log(params);

  return dynamo.send(
    new QueryCommand(params)
  );
};

export const post = function(tableName, path, body) {
  return dynamo.send(
    new PutCommand({
      TableName: tableName,
      Item: {
        PK: getPartitionKey(path),
        SK: getSortKey(path),
        ...body
      }
    })
  );
};

export const put = function (tableName, path, body) {
  var params = {
    TableName: tableName,
    Key: {
      PK: getPartitionKey(path),
      SK: getSortKey(path)
    },
    ExpressionAttributeValues: {},
    ExpressionAttributeNames: {},
    UpdateExpression: "",
    ReturnValues: "ALL_NEW"
  };

  let prefix = "set ";
  let attributes = Object.keys(body);
  for (let i = 0; i < attributes.length; i++) {
    let attribute = attributes[i];
    if (attribute === "PK" || attribute === "SK") {
      continue;
    }
    params["UpdateExpression"] += prefix + "#" + attribute + " = :" + attribute;
    params["ExpressionAttributeValues"][":" + attribute] = body[attribute];
    params["ExpressionAttributeNames"]["#" + attribute] = attribute;
    prefix = ", ";
  }

  console.log(params);

  const response = dynamo.send(
    new UpdateCommand(params)
  );

  console.log(response);
  return response;
};

export const del = function (tableName, path) {
  return dynamo.send(
    new DeleteCommand({
      TableName: tableName,
      Key: {
        PK: getPartitionKey(path),
        SK: getSortKey(path)
      },
    })
  );
};

export const nextId = async function (tableName, path) {
  let id = 0;
  try {
    const response = await dynamo.send(
      new UpdateCommand({
        TableName: tableName,
        Key: {
          PK: getPartitionKey(path),
          SK: getSortKey(path) + "#" + "counter"
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
    return response.Attributes && response.Attributes.Increment ? response.Attributes.Increment : 0;
  } catch (err) {
    console.log(err);
    if (err.__type === "com.amazon.coral.validate#ValidationException") {
      id = 10004321;
      const response = await post(tableName, path, {
        SK: getSortKey(path) + "#" + "counter",
        Increment: id
      });
      console.log(response);
    }
  }
  return id;
}
