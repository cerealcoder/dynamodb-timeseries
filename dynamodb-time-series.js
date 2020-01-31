'use strict';

const assert = require('assert');
const AWS = require('aws-sdk');


const DynamoTimeSeries = Object.create({});
exports = module.exports = DynamoTimeSeries;


// 
// set options
//
DynamoTimeSeries.options = {
  tableName: null,
};
DynamoTimeSeries.setOptions = function(options) {
  this.verifyOptions(options);
  this.options = options;
  return this;
}
DynamoTimeSeries.verifyOptions = function(options) {
  assert(options.tableName, 'table name must be defined');
}

/**
 *
 */
DynamoTimeSeries.putEvent = async function(userId, eventType, epochTime, evt) {
  this.verifyOptions(this.options);
  assert(userId, 'userId required');
  assert(eventType, 'eventType required');
  assert(epochTime, 'epochTime required');
  assert(evt, 'evt required');

  const ddb = new AWS.DynamoDB.DocumentClient();

  const ddbParams = {
    TableName: this.options.tableName,
    Item: {
      UserIdType: userId + eventType,
      EpochTime: epochTime,
      Event: evt,
    }
  };

  const result = await ddb.put(ddbParams).promise();

  return result;
}

/**
 *
 */
DynamoTimeSeries.getEvents = async function(userId, eventType, startTime, endTime) {
  this.verifyOptions(this.options);
  assert(userId, 'userId required');
  assert(eventType, 'eventType required');
  assert(startTime, 'startTime required');
  assert(endTime, 'endTime required');

  const ddb = new AWS.DynamoDB.DocumentClient();


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
}
