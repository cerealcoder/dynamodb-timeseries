'use strict'
const tape = require('tape')
const _test = require('tape-promise').default // <---- notice 'default'
const test = _test(tape) // decorate tape
const Random = require('random-js').Random;
const random = new Random();
const _ = require('underscore');

const AWS = require('aws-sdk');

const Ddbts = require('../dynamodb-time-series.js');


function delay (time) {
  return new Promise(function (resolve, reject) {
    setTimeout(function () {
      resolve()
    }, time)
  })
}
 

const TableName = random.string(16);

async function DynamoDbAwaitTableState(ddb, desiredState) {
  let i = 0;
  let currState;
  let described;

  // busy poll table state
  do {
    await delay(1000);
    described = await ddb.describeTable({TableName: TableName}).promise();
    //console.log(described);
    currState = described.Table.TableStatus;
    i = i + 1;
  } while (currState != desiredState && i < 20);

  return described;

};

//
// create one test table and use it for all tests since creating the table takes about 10 seconds
// In productin you would use a Cloud Formation Template to create the table and not create one
// all the time
//
test('create dynamoDB table for all tests.  Use random keys to make tests independent', async function (t) {
  const ddb = new AWS.DynamoDB();

	const ddbParams = {
		TableName: TableName,
    BillingMode: 'PAY_PER_REQUEST',
		AttributeDefinitions: [
			{
				AttributeName: 'UserIdType', 
				AttributeType: 'S'
			}, 
			{
				AttributeName: 'EpochTime', 
				AttributeType: 'N'
			}
		], 
		KeySchema: [
			{
				AttributeName: 'UserIdType', 
				KeyType: 'HASH'
			}, 
			{
				AttributeName: 'EpochTime', 
				KeyType: 'RANGE'
			}
		], 
	};

  let result = await ddb.createTable(ddbParams).promise();
  //console.log(result);

  let tableDescription = await DynamoDbAwaitTableState(ddb, 'ACTIVE');
  t.equal(tableDescription.Table.TableName, TableName, 'table name is the random string created before the tests start');
  t.equal(tableDescription.Table.TableStatus, 'ACTIVE', 'table is active');

});

test('put and get a time series event', async function(t) {
  const ddbts = Object.create(Ddbts).setOptions({tableName: TableName});
  const userId = random.string(16);
  const eventType = 'testEvent';
  const startTime = 100;
  const endTime = 1000;
  const event = { eventFoo: 'bar' };


  const putResult = await ddbts.putEvent(userId, 'testEvent', startTime, event);
  //console.log(putResult);
  t.ok(_.isEqual(putResult, {}), 'put result is an empty object?... okay aws');

  const getResult = await ddbts.getEvents(userId, 'testEvent', startTime, endTime);
  //console.log(getResult);
  t.equal(getResult.Items.length, 1, 'one item put, one item queried');
  t.equal(getResult.Items[0].UserIdType, userId + eventType , 'user ID and type match what was queried');
  t.ok(_.isEqual(getResult.Items[0].Event, event), 'event contents queried is same as was put');

});

test('create an time series DB instance with an invalid credential option and make sure correct error is thrown', async function(t) {
  const ddbts = Object.create(Ddbts).setOptions({
    tableName: TableName, 
    awsOptions: { 
      credentials: 'invalid credentials'
    }
  });
  const userId = random.string(16);
  const eventType = 'testEvent';
  const startTime = 100;
  const endTime = 1000;
  const event = { eventFoo: 'bar' };


  let putResult;
  try {
    putResult = await ddbts.putEvent(userId, 'testEvent', startTime, event);
  } catch (e) {
    t.equal(e.code, 'CredentialsError', 'Credentials error was thrown when using bogus credentials as options to time series db');
    return;
  }

  t.fail('no error was thrown with invalid credentials');
  
});

test('create an a raw dynamo DB instance with invalid options', async function(t) {
  const userId = random.string(16);
  const eventType = 'testEvent';
  const startTime = 100;
  const endTime = 1000;
  const event = { eventFoo: 'bar' };

 
  let ddb;
  try {
    ddb = new AWS.DynamoDB.DocumentClient({ apiVersion: 'fuck you', credentials: 'more fuckery' });
  } catch (e) {
    //console.log('error when creating the client', e);
    return;
  }

  try {
    const ddbParams = {
      TableName: TableName,
      KeyConditionExpression: 'UserIdType = :UserIdType AND EpochTime BETWEEN :Begin AND :End',
      ExpressionAttributeValues: {
        ':UserIdType': userId + eventType,
        ':Begin':  startTime,
        ':End': endTime,
      },
    };
    const result = await ddb.query(ddbParams).promise();
  } catch (e) {
    //console.log('error when actually doing the query', e);
    t.equal(e.code, 'CredentialsError', 'Credentials error was thrown when using bogus credentials with direct creation');
    return;
  }

  t.fail('no error was thrown with invalid credentials');

});

test('delete test table', async function(t) {
  const ddb = new AWS.DynamoDB();

  let result = await ddb.deleteTable({TableName: TableName}).promise();
  //console.log(result);
  try {
    await DynamoDbAwaitTableState(ddb, '');
  } catch (e) {
    //console.log(JSON.stringify(e));
    t.equal(e.code, 'ResourceNotFoundException', 'Table was deleted because we got correct exception trying to wait for it to delete');
  }
});

