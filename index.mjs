import * as dynamodb from "./dynamodb.mjs"

const allowedHostnames = {
  "localhost": true
};

const TABLE_NAME = "nocode";

const HEADERS = {
  "Content-Type": "application/json",
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
  const hostname = url.hostname;
  if (!allowedHostnames[hostname]) {
    return getResponse("Unauthorized", 401);
  }

  try {
    switch (event.routeKey) {
      case "OPTIONS /v1/{proxy+}":
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

const getResponse = function (body, statusCode = 200, headers = HEADERS) {
  body = JSON.stringify(body);
  return { body, statusCode, headers };
}

const doGet = function (event, context) {
  const items = dynamodb.doGet(TABLE_NAME, event.rawPath);
  return getResponse(items);
}

const doPost = function (event, context) {
  const body = dynamodb.doPost(TABLE_NAME, event.rawPath, event.body);
  return getResponse(body);
}

const doPut = function (event, context) {
  const body = dynamodb.doPut(TABLE_NAME, event.rawPath, event.body);
  return getResponse(body);
}

const doDelete = function (event, context) {
  const response = dynamodb.del(TABLE_NAME, event.rawPath);
  console.log(response);
  return getResponse({ "deleted": event.rawPath });
}
