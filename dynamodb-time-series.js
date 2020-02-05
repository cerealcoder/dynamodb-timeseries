'use strict';

const assert = require('assert');
const AWS = require('aws-sdk');
const packageJson = require('./package.json');


const DynamoTimeSeries = Object.create({});
exports = module.exports = DynamoTimeSeries;


// 
// set options
//
DynamoTimeSeries.options = {
  tableName: null,
};

DynamoTimeSeries.setOptions = function(options) {
  this.options = this.verifyOptions(options);
  this.dynamoDbInstance = new AWS.DynamoDB( options.awsOptions );
  return this;
};

DynamoTimeSeries.verifyOptions = function(options) {
  // mandatory options
  assert(options.tableName, 'table name must be defined');

  // optional options
  //
  //  options.awsOptions;
  //    use for e.g. accessKeyID and the like when constructing AWS objects
  //    @see https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#constructor-property
  //
  //    note:  use the `credentials` options not the individual accessKey & etc options when using
  //    credentials obtained from sts
  options.awsOptions  = options.awsOptions  ? options.awsOptions : {};

  return options;
};

/**
 *
 */
DynamoTimeSeries.putEvent = async function(userId, eventType, epochTime, evt) {
  this.verifyOptions(this.options);
  assert(userId, 'userId required');
  assert(eventType, 'eventType required');
  assert(epochTime, 'epochTime required');
  assert(evt, 'evt required');

  console.log(`DynamoTimeSeries.putEvent() version ${packageJson.version}`);
  console.log(this.options);

  let ddb;
  try {
    ddb = new AWS.DynamoDB.DocumentClient({
      service:     this.dynamoDbInstance,
      credentials: this.options.awsOptions.credentials,
    });
  } catch(exception) {
    console.log(`Function AWS.DynamoDB.DocumentClient() failed: ${exception}`);
  }

  const ddbParams = {
    TableName: this.options.tableName,
    Item: {
      UserIdType: userId + eventType,
      EpochTime: epochTime,
      Event: evt,
    }
  };

  let result = null;
  try {
    result = await ddb.put(ddbParams).promise();
  } catch(exception) {
      console.log(`Function ddb.put() failed: ${exception}, stack = ${exception.stack}`);
      throw exception;
  }
  return result;
};

/**
 *
 */
DynamoTimeSeries.getEvents = async function(userId, eventType, startTime, endTime) {
  this.verifyOptions(this.options);
  assert(userId, 'userId required');
  assert(eventType, 'eventType required');
  assert(startTime, 'startTime required');
  assert(endTime, 'endTime required');

  const ddb = new AWS.DynamoDB.DocumentClient({ service: this.dynamoDbInstance });

  const ddbParams = {
    TableName: this.options.tableName,
    KeyConditionExpression: 'UserIdType = :UserIdType AND EpochTime BETWEEN :Begin AND :End',
    ExpressionAttributeValues: {
      ':UserIdType': userId + eventType,
      ':Begin':  startTime,
      ':End': endTime,
    },
  };

  const result = await ddb.query(ddbParams).promise();

  return result;
};
