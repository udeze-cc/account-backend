'use strict';
const { v4: uuidv4 } = require('uuid');
const DynamodbService = require('./src/dynamodb.service');
const tableName = process.env.DB_TABLE;
const DB = new DynamodbService(tableName);

const headers = {
  // Required for CORS support to work
  "Access-Control-Allow-Origin": "*",
  // Required for cookies, authorization headers with HTTPS
  "Access-Control-Allow-Credentials": true,
  "Access-Control-Allow-Methods": "OPTIONS, POST, GET, PUT"
}

function sucessResponse(response, code) {
  return {
      statusCode: code || 200,
      headers: headers,
      body: typeof response == "string" ? response : JSON.stringify(response, null, 2)
  };
}

function failureResponse(message, code) {
  return {
      statusCode: code || 500,
      headers: headers,
      body: JSON.stringify({ message }, null, 2)
  };
}

module.exports.read = async (event) => {
  const email = event.pathParameters.email;
  try {
    const response = await DB.findById({email});
    return sucessResponse(response, 200);
  } catch (error) {
    return failureResponse(error.message, 500)
  }
};

module.exports.readAll = async (event) => {
  try {
    const response = await DB.findAll();
    return sucessResponse(response, 200);
  } catch (error) {
    return failureResponse(error.message, 500)
  }
};


module.exports.create = async (event) => {
  const account = JSON.parse(event.body);
  try {
    const response = await DB.save(account, 'email');
    return sucessResponse(response, 201);
  } catch (error) {
    return failureResponse(error.message, 500)
  }
};

module.exports.updateAccount = async (event) => {
  const admin = event.pathParameters.admin;
  const {email, ...rest} = JSON.parse(event.body);
  try {
    const response = await DB.update({email}, {...rest, authorized_by: admin, authorized_on: new Date().toLocaleString()}, 'email');
    return sucessResponse(response, 200);
  } catch (error) {
    return failureResponse(error.message, 500)
  }
}
