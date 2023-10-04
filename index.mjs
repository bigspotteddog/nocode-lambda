import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  PutCommand,
  UpdateCommand,
  DeleteCommand,
  QueryCommand,
} from "@aws-sdk/lib-dynamodb";

export const handler = async (event, context) => {
  try {
    switch (event.routeKey) {
      case "DELETE /v1/{proxy+}":
        return doDelete(event, context);
      case "GET /v1/{proxy+}":
        return doGet(event, context);
      case "POST /v1/{proxy+}":
        return doPost(event, context);
      case "PUT /v1/{proxy+}":
        return doPut(event, context);
      default:
        throw new Error(`Unsupported route: "${event.routeKey}"`);
    }
  } catch (err) {
    console.log(err);
    return getResponse(err.message, 400)
  }
};

const doGet = async function(event, context) {
  const response = await get(TABLE_NAME, event.rawPath);
  console.log(response);
  const items = [];
  for (let i = 0; i < response.Items.length; i++) {
    let item = response.Items[i];
    if (item.SK.endsWith("#counter")) {
      continue;
    }
    delete item.PK;
    delete item.SK;
    delete item.SK2;
    items.push(item);
  }
  return getResponse(items);
}

const doPost = async function(event, context) {
  const eventBody = JSON.parse(event.body);
  if (eventBody.unique) {
    const response = await checkUnique(TABLE_NAME, event.rawPath, eventBody.unique);
    if (response.Count > 0) {
      return getResponse(`Unique constraint violation: ${eventBody.unique}`, 400);
    }
  }

  const id = await nextId(TABLE_NAME, event.rawPath);
  const path = event.rawPath + "/" + id;
  const body = {
    id: id,
    ...eventBody
  };

  const response = await put(TABLE_NAME, path, {
    PK: getPartitionKey(path),
    SK: getSortKey(path),
    ...body,
    SK2: eventBody.unique
  });
  console.log(response);
  return getResponse(body);
}

const doPut = async function(event, context) {
  const body = JSON.parse(event.body);
  const response = await put(TABLE_NAME, event.rawPath, body);
  console.log(response);
  return getResponse(body);
}

const doDelete = async function(event, context) {
  const response = await del(TABLE_NAME, event.rawPath);
  console.log(response);
  return getResponse(`Deleted item: ${event.rawPath}`);
}

// This should go in another file that is imported into this.

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);

const TABLE_NAME = "nocode";
const HEADERS = {
  "Content-Type": "application/json",
};

const getResponse = function(body, statusCode = 200, headers = HEADERS) {
  body = JSON.stringify(body);
  return {body, statusCode, headers};
}

const getPartitionKey = function(path) {
  return path.substring(1).split("/").slice(0, 3).join("#");
}

const getSortKey = function(path) {
  return path.substring(1).replaceAll("/", "#");
}

const get = function(tableName, path) {
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

const checkUnique = function(tableName, path, unique) {
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

const put = function(tableName, path, body) {
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

const del = function(tableName, path) {
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

const nextId = async function(tableName, path) {
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
      const response = await put(tableName, path, {
        SK: getSortKey(path) + "#" + "counter",
        Increment: id
      });
      console.log(response);
    }
  }
  return id;
}
