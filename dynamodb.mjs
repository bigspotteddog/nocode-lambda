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
  const response  = await get(tableName, eventPath);
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
  if (eventBody.unique) {
    const keySplit = eventBody.unique.split(",");
    for (let i = 0; i < keySplit.length; i++) {
      let field = keySplit[i].trim();
      let value = eventBody[field];
      if (uniqueKey.length > 0) uniqueKey += "#";
      uniqueKey += field + "#" + value;
    }
  }
  return uniqueKey;
}

export const doPost = async function (tableName, eventPath, eventBody) {
  let search = "";
  const uniqueKey = getUniqueKey(eventBody);

  if (eventBody.unique) {
    search = "unique#" + uniqueKey;
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
    const sk = search + "#" + eventPath.substring(1).replaceAll("/", "#") + "#" + id;
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
  let search = "";
  const uniqueKey = getUniqueKey(eventBody);

  if (eventBody.unique) {
    search = "unique#" + uniqueKey;

    const uniqueResponse = await checkUnique(tableName, eventPath, search);
    if (uniqueResponse.Count > 0) {
      if (!uniqueResponse.Items[0].SK.endsWith(eventPath.substring(1).replaceAll("/", "#"))) {
        throw new Error(`Unique constraint violation: ${eventBody.unique}`);
      }
    }
  }

  let body = { ...eventBody, updated: new Date().toISOString() };

  let putBody = { ...body };

  if (eventBody.unique) {
    let path = eventPath.split("/");
    path = path.slice(0, path.length - 1).join("/");
    const sk = search + "#" + eventPath.substring(1).replaceAll("/", "#");
    const postResponse = post(tableName, path, {
      SK: sk
    });
  }

  const putResponse = await put(tableName, eventPath, putBody);
  console.log(putResponse);
  if (putResponse.Attributes.unique && putResponse.Attributes.unique !== body.unique) {
    const deleteSearch = "unique#" + getUniqueKey(putResponse.Attributes.unique);
    let path = eventPath.split("/");
    path = path.slice(0, path.length - 1).join("/");
    const sk = deleteSearch + "#" + eventPath.substring(1).replaceAll("/", "#");
    const delResponse = await delByKeys(tableName, getPartitionKey(path), sk);
    console.log("delete old unique");
    console.log(delResponse);
  }
  body = {...putResponse.Attributes, ...body};
  delete body.PK;
  delete body.SK;
  delete body.SK2;
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

const post = async function(tableName, path, body) {
  return await dynamo.send(
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

const put = async function (tableName, path, body) {
  var params = {
    TableName: tableName,
    Key: {
      PK: getPartitionKey(path),
      SK: getSortKey(path)
    },
    ExpressionAttributeValues: {},
    ExpressionAttributeNames: {},
    UpdateExpression: "",
    ReturnValues: "ALL_OLD"
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

  const response = await dynamo.send(
    new UpdateCommand(params)
  );
  return response;
};

const delByKeys = function (tableName, pk, sk) {
  console.log("delByKeys");
  console.log(pk);
  console.log(sk);
  return dynamo.send(
    new DeleteCommand({
      TableName: tableName,
      Key: {
        PK: pk,
        SK: sk
      },
    })
  );
}

const del = function (tableName, path) {
  const pk = getPartitionKey(path);
  const sk = getSortKey(path);
  return delByKeys(tableName, pk, sk);
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
