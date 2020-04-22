'use strict';

const assert = require('assert');
const AWS = require('aws-sdk');
const packageJson = require('./package.json');
const DynamoDbBatchIterator = require('@aws/dynamodb-batch-iterator');


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

  const ddb = new AWS.DynamoDB.DocumentClient({service: this.dynamoDbInstance});

  const ddbParams = {
    TableName: this.options.tableName,
    Item: {
      UserIdType: userId + eventType,
      EpochTime: epochTime,
      Event: {
        epochTimeMilliSec: epochTime,
        mfgrId: eventType,
        event: evt,
      },
    }
  };

  const result = await ddb.put(ddbParams).promise();
  return result;
};


/**
 * evts input format: {
 *   EpochTimeMilliSec: <number>
 *   event: <JSON string of event>
 * }
 */
DynamoTimeSeries.putEvents = async function(userId, eventType, evts) {
  const self = this;
  this.verifyOptions(this.options);
  assert(userId, 'userId required');
  assert(eventType, 'eventType required');
  assert(evts, 'evt required');

  const ddb = new AWS.DynamoDB({service: this.dynamoDbInstance});

  // @see https://github.com/awslabs/dynamodb-data-mapper-js/tree/master/packages/dynamodb-batch-iterator
  const dynamoedEvts = evts.map(el => {
    assert('epochTimeMilliSec' in el, 'putEvents: each element must have epochTimeMilliSec key');
    return( 
    [
      self.options.tableName, 
      { 
        PutRequest: {
          Item: {
            UserIdType: { S: userId + eventType },
            EpochTime:  { N: el.epochTimeMilliSec.toString() },
            Event: { M: { 
              epochTimeMilliSec: { N: el.epochTimeMilliSec.toString() },
              mfgrId:            { S: eventType            },
              event:             { S: el.event             },
            }}
          }
        }
      }
    ]
    );
  });

  //console.log(JSON.stringify(evts,null,2));
  //console.log(JSON.stringify(dynamoedEvts,null,2));
  //const result = await DynamoDbBatchIterator.BatchWrite(ddb, dynamoedEvts);
  //console.log(JSON.stringify(result,null,2));
  
  // wish I could figure out how to do a simple await for the entire thing to be done
  let count = 0;
  for await (const item of new DynamoDbBatchIterator.BatchWrite(ddb, dynamoedEvts)) {
    //console.log(item);
    count++;
  }
  return count;
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
  return result.Items.map(el => {
    return el.Event;
  });
};
