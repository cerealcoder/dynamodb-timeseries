'use strict'
const assert = require('assert');
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
    process.stdout.write(`${i}.`);
  } while (currState != desiredState && i < 45);
  console.log('');
  assert(i < 45, 'timed out waiting for dynamodb to change state');

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
  t.equal(getResult.length, 1, 'one item put, one item queried');
  t.equal(getResult[0].mfgrId, eventType , 'user ID and type match what was queried');
  t.equal(getResult[0].epochTimeMilliSec, startTime, 'event time was the same as was put');
  t.ok(_.isEqual(getResult[0].event, event), 'event contents queried is same as was put');

});

test('put and get a time series event Array', async function(t) {
  const ddbts = Object.create(Ddbts).setOptions({tableName: TableName});
  const userId = random.string(16);
  const eventType = 'arrayEvent';
  const startTime = 100;
  const endTime = 1000;
  const event = [
    { eventFoo: 'bar1' },
    { eventFoo: 'bar2' },
    { eventFoo: 'bar3' },
  ];

  const foo = [].concat(event);

  const putResult = await ddbts.putEvent(userId, eventType, startTime, event);
  //console.log(putResult);
  t.ok(_.isEqual(putResult, {}), 'put result is an empty object?... okay aws');

  const getResult = await ddbts.getEvents(userId, eventType, startTime, endTime);
  t.equal(getResult.length, event.length, 'one array item put, array returned');
  t.equal(getResult[0].mfgrId, eventType , 'user ID and type match what should have been marshaled');
  t.equal(getResult[0].epochTimeMilliSec, startTime, 'event time was the same as what should have been marshalled');
  t.equal(getResult.length, event.length, 'event array length was the same as was put');

});

test('Verify we can chunk data up', async function (t) {

  const ddbts = Object.create(Ddbts).setOptions({tableName: TableName});

  // Create 10 days worth of data spaced out by a day
  const startTime = 1447858800001; // 2015-11-18T15:00:00:001
	const events = [
		{ eventFoo: 'bar1' },
		{ eventFoo: 'bar2' },
		{ eventFoo: 'bar3' },
		{ eventFoo: 'bar4' },
		{ eventFoo: 'bar5' },
		{ eventFoo: 'bar6' },
		{ eventFoo: 'bar7' },
		{ eventFoo: 'bar8' },
		{ eventFoo: 'bar9' },
		{ eventFoo: 'bar10' },
	];
  const marshalled = events.map((el, i) => {
    return {
      // NOTE: must match the graphql schema
      epochTimeMilliSec: new Date(startTime + i * 86400 * 1000).getTime(),
      mfgrId: 'foo',
      event: el,
    };
  });

  const chunked = ddbts.chunkDataByDay(marshalled);
  //console.log(chunked);
  t.equal(chunked.length, 10, 'chunked into 10 days of data');
  chunked.forEach(el => {
    //console.log(el);
    t.equal(el.event.length, 1, 'each chunk is one long');
  });

});

test('batch put and get time series events', async function(t) {
  const ddbts = Object.create(Ddbts).setOptions({tableName: TableName});
  const userId = random.string(16);
  const eventType = 'batchPut';
  const events = [
    {
      epochTimeMilliSec: 100,
      event: JSON.stringify( { eventFoo: 'super' }),
    },
    {
      epochTimeMilliSec: 200,
      event: JSON.stringify( { eventFoo: 'cali' }),
    },
  ];

  const putResult = await ddbts.putEvents(userId, eventType, events);
  //console.log(putResult);
  t.equal(putResult, 2, 'the number of items put is what was requested');

  const getResult = await ddbts.getEvents(userId, eventType, 100, 200);
  //console.log(getResult);
  t.equal(getResult.length, 2, 'two  items put, two items queried');
  t.equal(getResult[0].mfgrId, eventType , 'user ID and type match what was queried');
  t.equal(getResult[0].epochTimeMilliSec, 100, 'event time was the same as was put');
  //t.ok(_.isEqual(getResult[0].event, event), 'event contents queried is same as was put');

});

test('Verify we can mass insert data that gets chunked for us', async function (t) {

  const ddbts = Object.create(Ddbts).setOptions({tableName: TableName});
  const userId = random.string(16);
  const eventType = 'chunkedPut';

  // Create 10 days worth of data spaced out by a day
  const startTime = 1447858800001; // 2015-11-18T15:00:00:001
	const events = [
		{ eventFoo: 'bar1' },
		{ eventFoo: 'bar2' },
		{ eventFoo: 'bar3' },
		{ eventFoo: 'bar4' },
		{ eventFoo: 'bar5' },
		{ eventFoo: 'bar6' },
		{ eventFoo: 'bar7' },
		{ eventFoo: 'bar8' },
		{ eventFoo: 'bar9' },
		{ eventFoo: 'bar10' },
	];
  const marshalled = events.map((el, i) => {
    return {
      // NOTE: must match the graphql schema
      // XXX Which means a yucky forward dependency
      epochTimeMilliSec: new Date(startTime + i * 86400 * 1000).getTime(),
      mfgrId: eventType,
      event: el,
    };
  });

  const putResult = await ddbts.putEventsInChunks(userId, eventType, marshalled);

  //console.log(putResult);

  const getResult = await ddbts.getEvents(userId, eventType, startTime, marshalled[marshalled.length-1].epochTimeMilliSec);
  //console.log(getResult);
  t.equal(getResult.length, events.length, 'length of result is what we got');
  t.equal(getResult[0].mfgrId, eventType , 'user ID and type match what was queried');
 

});

test('Verify we find the last latest event from chunked data', async function (t) {

  const ddbts = Object.create(Ddbts).setOptions({tableName: TableName});
  const userId = random.string(16);
  const eventType = 'chunkedPut';

  // Create 10 days worth of data spaced out by a day
  const startTime = 1447858800001; // 2015-11-18T15:00:00:001
	const events = [
		{ eventFoo: 'bar1' },
		{ eventFoo: 'bar2' },
		{ eventFoo: 'bar3' },
		{ eventFoo: 'bar4' },
		{ eventFoo: 'bar5' },
		{ eventFoo: 'bar6' },
		{ eventFoo: 'bar7' },
		{ eventFoo: 'bar8' },
		{ eventFoo: 'bar9' },
		{ eventFoo: 'bar10' },
	];
  const marshalled = events.map((el, i) => {
    return {
      // NOTE: must match the graphql schema
      // XXX Which means a yucky forward dependency
      epochTimeMilliSec: new Date(startTime + i * 86400 * 1000).getTime(),
      mfgrId: eventType,
      event: el,
    };
  });

  const putResult = await ddbts.putEventsInChunks(userId, eventType, marshalled);

  //console.log(putResult);

  const getResult = await ddbts.getLatest(userId, eventType);
  //console.log(getResult);
  t.equal(getResult.epochTimeMilliSec, marshalled[marshalled.length - 1].epochTimeMilliSec, 'got timestamp of last item');
 
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
    //console.log(e);
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

