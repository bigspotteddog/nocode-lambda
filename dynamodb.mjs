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

export const doGet = async function (tableName, eventPath) {
  const response = await get(tableName, eventPath);
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

const getUniqueKey = function(eventBody) {
  let uniqueKey = "";
  const keySplit = eventBody.unique.split(",");
  for (let i = 0; i < keySplit.length; i++) {
    let field = keySplit[i].trim();
    let value = eventBody[field];
    if (uniqueKey.length > 0) uniqueKey += "#";
    uniqueKey += field + "#" + value;
  }
  return uniqueKey;
}

export const doPost = async function (tableName, eventPath, eventBody) {
  let search = "";
  const uniqueKey = getUniqueKey(eventBody);

  if (eventBody.unique) {
    search = "unique#" + uniqueKey + "#" + eventPath.substring(1).replaceAll("/", "#");

    const response = await checkUnique(tableName, eventPath, search);
    console.log("doPost check unique");
    console.log(response);
    if (response.Count > 0) {
      throw new Error(`Unique constraint violation: ${eventBody.unique}`);
    }
  }

  const id = await nextId(tableName, eventPath);
  const path = eventPath + "/" + id;
  let body = {
    id: id,
    ...eventBody
  };

  body = {
    ...body,
    created: new Date().toISOString()
  }

  let postBody = {
    ...body,
    PK: getPartitionKey(path),
    SK: getSortKey(path)
  };

  if (eventBody.unique) {
    const sk = search + "#" + id;
    const response = post(tableName, eventPath, {
      SK: sk
    });
  }

  try {
    const response = await post(tableName, path, postBody);
  } catch(err) {
    throw new Error("Unable to persist item.");
  }
  return body;
}

export const doPut = async function (tableName, eventPath, eventBody) {
  if (eventBody.unique) {
    const search = "unique#" + eventBody.unique + "#" + eventPath.substring(1).replaceAll("/", "#");
    const response = await checkUnique(tableName, eventPath, search);
    if (response.Count > 0) {
      throw new Error(`Unique constraint violation: ${eventBody.unique}`);
    }
  }

  let body = { ...eventBody, updated: new Date().toISOString() };

  let putBody = { ...body };

  if (eventBody.unique) {
    const sk = "unique#" + eventBody.unique + "#" + eventPath.substring(1).replaceAll("/", "#") + "#" + id;
    const response = post(tableName, eventPath, {
      SK: sk
    });
  }

  const response = await put(tableName, eventPath, putBody);
  return body;
}

export const doDelete = async function (tableName, eventPath) {
  const response = await del(tableName, eventPath);
  return true;
}

const getPartitionKey = function (path) {
  return path.substring(1).split("/").slice(0, 3).join("#");
}

const getSortKey = function (path) {
  return path.substring(1).replaceAll("/", "#");
}

const get = function (tableName, path) {
  return getByKeys(tableName, getPartitionKey(path), getSortKey(path));
};

const getByKeys = function (tableName, pk, sk) {
  const params = {
    TableName: tableName,
    KeyConditionExpression:
      "PK = :pk AND begins_with (SK, :sk)",
    ExpressionAttributeValues: {
      ":pk": pk,
      ":sk": sk
    },
  };
  console.log(params);

  return dynamo.send(
    new QueryCommand(params)
  );
};

const checkUnique = async function (tableName, path, unique) {
  const response = await getByKeys(
    tableName, getPartitionKey(path), unique.split("#").slice(0, 3).join("#"));
  return response;
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

  const response = dynamo.send(
    new UpdateCommand(params)
  );

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
    if (err.__type === "com.amazon.coral.validate#ValidationException") {
      id = 10004321;
      const response = await post(tableName, path, {
        SK: getSortKey(path) + "#" + "counter",
        Increment: id
      });
    }
  }
  return id;
}
