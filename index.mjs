import {get, post, put, del} from "./dynamodb.mjs"

const allowedHostnames = {
  "localhost": true
};

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "X-Requested-With"
}

export const handler = async (event, context) => {
  console.log(event);
  console.log(context);
  const url = new URL(event.headers.origin);
  console.log("origin: " + url);
  console.log("domain: " + url.hostname);
  const hostname = url.hostname;
  if (!allowedHostnames[hostname]) {
    return getResponse("Unauthorized", 401);
  }

  try {
    switch (event.routeKey) {
      case "OPTIONS /v1/{proxy+}":
        console.log("options called");
        return getResponse("", 204, CORS_HEADERS);
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

const doGet = async function (event, context) {
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

const doPost = async function (event, context) {
  const eventBody = JSON.parse(event.body);
  if (eventBody.unique) {
    const response = await checkUnique(TABLE_NAME, event.rawPath, eventBody.unique);
    if (response.Count > 0) {
      return getResponse(`Unique constraint violation: ${eventBody.unique}`, 400);
    }
  }

  const id = await nextId(TABLE_NAME, event.rawPath);
  console.log("nextId: " + id);
  const path = event.rawPath + "/" + id;
  let body = {
    id: id,
    ...eventBody
  };

  console.log("post:");
  console.log(body);

  body = {
    PK: getPartitionKey(path),
    SK: getSortKey(path),
    ...body
  }

  if (eventBody.unique) {
    body = {...body, SK2: eventBody.unique}
  }

  try {
    const response = await post(TABLE_NAME, path, body);
    console.log(response);
  } catch(err) {
    console.log(err);
  }
  return getResponse(body);
}

const doPut = async function (event, context) {
  const eventBody = JSON.parse(event.body);
  if (eventBody.unique) {
    const response = await checkUnique(TABLE_NAME, event.rawPath, eventBody.unique);
    if (response.Count > 0) {
      return getResponse(`Unique constraint violation: ${eventBody.unique}`, 400);
    }
  }

  let body = JSON.parse(event.body);
  if (eventBody.unique) {
    body = {...body, SK2: eventBody.unique}
  }
  const response = await put(TABLE_NAME, event.rawPath, body);
  console.log(response);
  return getResponse(body);
}

const doDelete = async function (event, context) {
  const response = await del(TABLE_NAME, event.rawPath);
  console.log(response);
  return getResponse(`Deleted item: ${event.rawPath}`);
}
