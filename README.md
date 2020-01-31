# DynamoDB Time Series

## Usage


### Prerequisites
* You must have access to AWS, including any credentials, IAM permissions, and region as required by the AWS SDK


## Unit Tests

### Prerequisites
* Set environment variable `AWS_DEFAULT_REGION`
* Make your AWS credentials available in file `~/.aws`


### Interactive Invocation
1. Run Docker Compose:

        docker-compose run app  /bin/bash
        
1. Navigate to the directory where the source code is located:

        cd /home/code       

1. Run NPM install

        npm install
        
1. Navigate to the test directory:

        cd test
        
1. Run NPM install again (for the test directory)

        npm install
        
1. Run the unit tests

        npm test *.js


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