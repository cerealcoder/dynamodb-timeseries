'use strict';

const assert = require('assert');
const AWS = require('aws-sdk');
const packageJson = require('./package.json');
const DynamoDbBatchIterator = require('@aws/dynamodb-batch-iterator');
const _ = require('underscore');
const {gzip, ungzip} = require('node-gzip');
const Promise = require('bluebird');


const DynamoTimeSeries = Object.create({});
exports = module.exports = DynamoTimeSeries;

// outside of everything so we have connection reuse
var ddb;

// 
// set options
//
DynamoTimeSeries.options = {
  tableName: null,
};

DynamoTimeSeries.setOptions = function(options) {
  this.options = this.verifyOptions(options);
  this.dynamoDbInstance = new AWS.DynamoDB( options.awsOptions );
  ddb = new AWS.DynamoDB.DocumentClient({ service: this.dynamoDbInstance });
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

  let marshalledEvent = evt;
  let mfgrId;

  if (_.isArray(evt)) {
    // each element of the array must look like
    // a standardized event so that unmarshalling
    // of arrays in getEvents works properly
    marshalledEvent = evt.map(el => {
      if (el.mfgrId === undefined) {
        el.mfgrId = eventType;
      }
      if (el.epochTimeMilliSec === undefined) {
        el.epochTimeMilliSec = epochTime;
      }
      if (!mfgrId) {
        mfgrId =  el.mfgrId;
      }
      return el;
    });
  } else if (_.isObject(evt)){
    if (evt.mfgrId) {
      mfgrId = evt.mfgrId;
    } else {
      mfgrId = eventType;
    }
  } else {
    mfgrId = eventType;
  }

  const evtCompressed = await gzip(JSON.stringify(marshalledEvent));


  const ddbParams = {
    TableName: this.options.tableName,
    Item: {
      UserIdType: userId + eventType,
      EpochTime: epochTime,
      Gzip: true,
      Event: {
        epochTimeMilliSec: epochTime,
        mfgrId: mfgrId,
        event: evtCompressed,
      },
    }
  };

  const result = await ddb.put(ddbParams).promise();
  return result;
};


/**
 *
 */
DynamoTimeSeries.delEvent = async function(userId, eventType, epochTime) {
  this.verifyOptions(this.options);
  assert(userId, 'userId required');
  assert(eventType, 'eventType required');
  assert(epochTime, 'epochTime required');

  const ddbParams = {
    TableName: this.options.tableName,
    Key: {
      UserIdType: userId + eventType,
      EpochTime: epochTime,
    }
  };

  const result = await ddb.delete(ddbParams).promise();
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
  assert(Array.isArray(evts), 'evts must be an array');

  const ddbLocal = new AWS.DynamoDB({ service: this.dynamoDbInstance });

  // @see https://github.com/awslabs/dynamodb-data-mapper-js/tree/master/packages/dynamodb-batch-iterator
  const marshalledEvents = evts.map(el => {
    return(
    [
      self.options.tableName, 
      { 
        PutRequest: {
          Item: AWS.DynamoDB.Converter.marshall({
            UserIdType: userId + eventType,
            EpochTime: el.epochTimeMilliSec,
            Event: { 
              epochTimeMilliSec: el.epochTimeMilliSec,
              mfgrId:            eventType,
              event:             el.event,
            }
          })
        }
      }
    ]
    )
  });
  
  //console.log(JSON.stringify(evts,null,2));
  //console.log(JSON.stringify(dynamoedEvts,null,2));
  //const result = await DynamoDbBatchIterator.BatchWrite(ddb, dynamoedEvts);
  //console.log(JSON.stringify(result,null,2));
  
  // wish I could figure out how to do a simple await for the entire thing to be done
  let count = 0;
  for await (const item of new DynamoDbBatchIterator.BatchWrite(ddbLocal, marshalledEvents)) {
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
  const uncompressedEvents = await Promise.map(result.Items, async (el) => {
    if (el.Gzip) {
      const eventUnzipped = await ungzip(el.Event.event);
      el.Event.event = JSON.parse(eventUnzipped);
    }
    return el.Event;
  });
  return uncompressedEvents.reduce((acc, el) => {
    // flatten multiple array results down to one big array
    if (Array.isArray(el.event)) {
      Array.prototype.push.apply(acc, el.event)
    } else {
      acc.push(el);
    }
    return acc;
  },[]);
};

/**
 * @brief return latest item in the time series efficiently, we hope
 *
 */
DynamoTimeSeries.getLatest = async function(userId, eventType) {
  this.verifyOptions(this.options);
  assert(userId, 'userId required');
  assert(eventType, 'eventType required');

  const ddbParams = {
    TableName: this.options.tableName,
    KeyConditionExpression: 'UserIdType = :UserIdType',
    ScanIndexForward: false,
    Limit: 1,
    ExpressionAttributeValues: {
      ':UserIdType': userId + eventType,
    },
    ReturnConsumedCapacity: 'TOTAL',
  };

  const result = await ddb.query(ddbParams).promise();
  //console.log(JSON.stringify(result.ConsumedCapacity,null,2));
  return result.Items.map(el => {
    return el.Event;
  }).reduce((acc, el) => {
    // flatten multiple array results down to one big array
    if (Array.isArray(el.event)) {
      Array.prototype.push.apply(acc, el.event)
    } else {
      acc.push(el);
    }
    return acc;
  },[]).pop();
};


/**
 * @brief chunks time series data up into 1 day chunks
 *
 * @param data
 * Data is an object that includes the element `epochTimeMilliSec`.  This data
 * is assumed to have already been sorted
 *
 * @param async callback
 * A function that will be called for every chunk (e.g. you can write the data to a database)
 *
 * @returns an array of chunks, each chunk having its
 * epochTime being the value of the first entry
 *
 */
DynamoTimeSeries.chunkDataByDay = function(events) {

  let currentDate = new Date(events[0].epochTimeMilliSec);
  let currentDay = currentDate.getDay();
  let currentTime = currentDate.getTime();

  const chunked = _.groupBy(events, el => {
    const thisDate = new Date(el.epochTimeMilliSec);
    if (thisDate.getDay() != currentDay) {
      // make a new chunk
      currentDate = thisDate;
      currentDay = currentDate.getDay();
      currentTime = currentDate.getTime();
    }
    // put into chunk that is labeled by the time
    return currentTime;
  });
	return Object.keys(chunked).map(el => {
		const a = chunked[el];
		//console.log(`a is ${a}`);
		if (Array.isArray(a) && a.length > 0) {
			const normalizedArray = {
				epochTimeMilliSec: a[0].epochTimeMilliSec,
				mfgrId: a[0].mfgrId,
				event: a,
			};
			return normalizedArray;
		} else {
			return undefined;
		}
	}).filter(el => {
		return (el != undefined);
	});
};



/**
 * @brief chunks time series data up into 1 day chunks and writes it to the time series database
 *
 * @param userId
 * Unique identifer of a user, IOT device, or something else
 *
 * @param eventType
 * Additional identifer appended to the userId
 *
 * @param evts
 * an array of events that will be chunked up and each chunk written to the time series database
 *
 *
 */

DynamoTimeSeries.putEventsInChunks = async function(userId, eventType, evts) {
  const chunked = this.chunkDataByDay(evts);
  return this.putEvents(userId, eventType, chunked);
};
