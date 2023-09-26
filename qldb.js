"use strict";

const { v4: uuidv4 } = require('uuid');
const { QldbDriver } = require('amazon-qldb-driver-nodejs');

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

module.exports.createDocument = async (event) => {
  const driver = new QldbDriver(process.env.LEDGER_NAME);
  const tableName = event.pathParameters.tableName;
  const {partyName, voteCount, pollingCentre, officerId, electionDate, electionId} = JSON.parse(event.body);

  try {
    const response = await driver.executeLambda(async (txn) => {
      const document = {key: uuidv4(), partyName, voteCount, pollingCentre, officerId, electionDate, electionId, ward, lga, state, federal};
      return await insertDocument(txn, tableName, document)
      .then(res => {
        const key = uuidv4()
        const wkey = `${document.key}--${key}`;
        // After saving result into results table
        insertDocument(txn, 'Ward', {...document, wkey})
        .then(res => {
          const key = uuidv4();
          const skey = `${wkey}--${key}`;
          insertDocument(txn, 'State', {...document, skey, key})
          .then(res => {
            const key = uuidv4();
            const fkey = `${skey}--${key}`;
            insertDocument(txn, 'Federal', {...document, fkey, key})
            .then(res => {
              return document;
            })
            return document;
          })
          return document;
        })
        return document;
      })

      
      // return Promise.all([
      //   insertDocument(txn, tableName, document)
      //   .then(res => {
      //     return document;
      //   }),
      //   insertDocument(txn, 'Ward', document)
      //   .then(res => {
      //     return document;
      //   }),
      //   insertDocument(txn, 'LGA', document)
      //   .then(res => {
      //     return document;
      //   }),
      //   insertDocument(txn, 'State', document)
      //   .then(res => {
      //     return document;
      //   }),
      //   insertDocument(txn, 'Federal', document)
      //   .then(res => {
      //     return document;
      //   })
      // ])
    });

    driver.close();
    console.log('response: ', response[0]);
    return sucessResponse(response, 200)
  } catch (error) {
    console.error(error);
    return failureResponse(error.message, 500)
  }
}

module.exports.createTable = async (event) => {
  const driver = new QldbDriver(process.env.LEDGER_NAME);
  const admin = event.pathParameters.admin;
  const {table} = JSON.parse(event.body);

  try {
    const response = await driver.executeLambda(async (txn) => {
      return await createTable(txn, table);
    });
    driver.close();
    return sucessResponse(response, 200)
  } catch (error) {
    console.error(error);
    return failureResponse(error.message, 500)
  }
}

module.exports.createIndex = async (event) => {
  const driver = new QldbDriver(process.env.LEDGER_NAME);
  const admin = event.pathParameters.admin;
  const {index, table} = JSON.parse(event.body);

  try {
    const response = await driver.executeLambda(async (txn) => {
      return await createIndex(txn, table, index);
    });
    driver.close();
    return sucessResponse(response, 200)
  } catch (error) {
    console.error(error);
    return failureResponse(error.message, 500)
  }
}

module.exports.fetchElectionData = async (event) => {
  const driver = new QldbDriver(process.env.LEDGER_NAME);
  // "SELECT firstName, age, lastName FROM People WHERE firstName = ?", "John"
  const table = event.pathParameters.table;
  // const {table} = JSON.parse(event.body);
  const query = `SELECT * FROM ${table}`;
  try {
    const response = await driver.executeLambda(async (txn) => {
      return await fetchDocuments(txn, query);
    });
    driver.close();
    return sucessResponse(response, 200)
  } catch (error) {
    console.error(error);
    return failureResponse(error.message, 500)
  }
}

// async function addGuid(txn, id) {
//     const statement = 'UPDATE Results SET resultId = ?';
//     return txn.execute(statement, id);
// }

async function createTable(txn, tableName = "ElectionResults") {
  await txn.execute(`CREATE TABLE ${tableName}`);
}

async function createIndex(txn, tableName = "ElectionResults", indexName = "pollingLocation") {
  await txn.execute(`CREATE INDEX ON ${tableName} (${indexName})`);
}

async function insertDocument(txn, tableName = "ElectionResults", document) {
  await txn.execute(`INSERT INTO ${tableName} ?`, document);
}

async function fetchDocuments(txn, query) {
  return await txn.execute(query);
}

const electionResults = {
  partyName: "PDP",
  numberOfVotes: "27000",
  pollingLocation: 42,
  officialId: 'eoritottitototo'
};