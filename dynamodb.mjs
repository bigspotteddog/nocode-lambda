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

export const doGet = async function (tableName, path) {
  const response = await get(tableName, path);
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
  return items;
}

export const doPost = async function (tableName, path, body) {
  const eventBody = body;
  if (eventBody.unique) {
    const response = await checkUnique(tableName, path, eventBody.unique);
    if (response.Count > 0) {
      return getResponse(`Unique constraint violation: ${eventBody.unique}`, 400);
    }
  }

  const id = await nextId(tableName, path);
  console.log("nextId: " + id);
  const path = path + "/" + id;
  body = {
    id: id,
    ...eventBody
  };

  console.log("post:");
  console.log(body);

  body = {
    PK: getPartitionKey(path),
    SK: getSortKey(path),
    ...body,
    created: new Date().toISOString()
  }

  if (eventBody.unique) {
    body = {...body, SK2: eventBody.unique}
  }

  try {
    const response = await post(tableName, path, body);
    console.log(response);
  } catch(err) {
    console.log(err);
  }
  return body;
}

export const doPut = async function (tableName, path, body) {
  const eventBody = body;
  if (eventBody.unique) {
    const response = await checkUnique(tableName, path, eventBody.unique);
    if (response.Count > 0) {
      return getResponse(`Unique constraint violation: ${eventBody.unique}`, 400);
    }
  }

  body = { ...body, updated: new Date().toISOString() };

  if (eventBody.unique) {
    body = {...body, SK2: eventBody.unique}
  }
  const response = await put(tableName, path, body);
  console.log(response);
  return body;
}

export const doDelete = async function (tableName, path) {
  const response = await del(tableName, path);
  console.log(response);
  return true;
}

const getPartitionKey = function (path) {
  return path.substring(1).split("/").slice(0, 3).join("#");
}

const getSortKey = function (path) {
  return path.substring(1).replaceAll("/", "#");
}

const get = function (tableName, path) {
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

const checkUnique = function (tableName, path, unique) {
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

const post = function(tableName, path, body) {
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

const put = function (tableName, path, body) {
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

const del = function (tableName, path) {
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

const nextId = async function (tableName, path) {
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
