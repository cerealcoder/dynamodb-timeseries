# DynamoDB Time Series
This module allows one to use a dynamodb table as a time-series database, allowing queries for
time ranges, and allows keeping data from different sources in the same table.

## Usage

1. Add this package and if necessary, a peer dependency of the AWS SDK to your package.json file.  
Note that AWS Lambda does not require the inclusion of the AWS SDK, it does that for you.

        "dependencies": {
            ...
            "dynamodb-timeseries": "^0.1.00",
            "aws-sdk: "^2.x",
            ...
        }

1. Add a require statement to your code:

        const DynamoDbTimeSeries = require('dynamodb-timeseries');

1. Create an instance of the API that attaches to a DynamoDB table:

        const tableName           = 'Your existing DynamoDB table name';
        const dbTimeSeriesOptions = {
            tableName: tableName,
            awsOptions: {
                credentials: {
                    accessKeyId:     'An access key ID, typically created by STS',
                    secretAccessKey: 'A secret access key, typically created by STS',
                    sessionToken:    'A session token, typically created by STS',
                    expiration:      'The credentials expiration, typically created by STS'
                }
            }
        };
        const dbApiInstance = Object.create(DynamoDbTimeSeries).setOptions(dbTimeSeriesOptions);

1. Create an event that will be written to the DynamoDB table:

        const event = {
            foo: 'bar'
        };

1. Write to the DynamoDB table:

        const userId        = 'Your user ID';
        const eventType     = 'Your event type';
        const epochTime     = new Date().getTime();
        await dbApiInstance.putEvent(userId, eventType, epochTime, event);


See files `test/*.js` for specific examples.

### output format

    [
      {
       // outer attributes are common no matter the manufacturer
       epochTimeMilliSec: ... per Javascript standard.  56 bit int or a 64 bit float ... ,
       mfgrId: '<ID of the manufacturer>'
       event: { ... event specific to the manufacturer, see their API ... },
      }
    ]

### Prerequisites
* You must have access to AWS, including any credentials, IAM permissions, and region as required by the AWS SDK


## Unit Tests

### Prerequisites
* Set environment variable `AWS_DEFAULT_REGION`
* set environment variable `export NPM_TOKEN=$(cat ~/.npm/token)`
* Make your AWS credentials available in file `~/.aws`


### Interactive Invocation
1. Run Docker Compose:

        docker-compose run app  /bin/bash
        
1. Navigate to the directory where the source code is located:

        cd /home/code       

1. Run NPM install

        npm install
        
1. Run the unit tests

        npm test test/*.js


### Automated Invocation

1. Set environment variable AWS_DEFAULT_REGION to your desired region:

        export AWS_DEFAULT_REGION=us-east-1
        
1. Run script test.sh

        ./test.sh
        
You should see a transcipt that looks similar to the following:

```
$ ./test.sh
audited 19 packages in 0.644s
found 0 vulnerabilities

up to date in 10.89s

> dynamodb-timeseries-test@0.0.1 test /home/code/test
> node_modules/tape/bin/tape "dynamodb-time-series.js"

TAP version 13
# create dynamoDB table for all tests. Use random keys to make tests independent
ok 1 table name is the random string created before the tests start
ok 2 table is active
# put and get a time series event
ok 3 put result is an empty object?... okay aws
ok 4 one item put, one item queried
ok 5 user ID and type match what was queried
ok 6 event contents queried is same as was put
# delete test table
ok 7 Table was deleted because we got correct exception trying to wait for it to delete

1..7
# tests 7
# pass  7

# ok
```        
