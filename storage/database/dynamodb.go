package database

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"

	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/klaytn/klaytn/log"
)

var dataNotFoundErr = errors.New("data is not found with the given key")
var nilDynamoConfigErr = errors.New("attempt to create DynamoDB with nil configuration")

const dynamoWriteSizeLimit = 400 * 1024 // The maximum item size is 400 KB including a key size
const dynamoBatchSize = 25
const dynamoMaxRetry = 5

// dynamo table provisioned setting
const dynamoReadCapacityUnits = 10000
const dynamoWriteCapacityUnits = 10000

// batch write
const WorkerNum = 10
const itemChanSize = WorkerNum * 2

var overSizedDataPrefix = []byte("oversizeditem")

type DynamoDBConfig struct {
	Region             string
	Endpoint           string
	TableName          string
	ReadCapacityUnits  int64
	WriteCapacityUnits int64
}

type batchWriteWorkerInput struct {
	items []*dynamodb.WriteRequest
	wg    *sync.WaitGroup
}

type dynamoDB struct {
	config *DynamoDBConfig
	db     *dynamodb.DynamoDB
	fdb    fileDB
	logger log.Logger // Contextual logger tracking the database path

	// worker pool
	quitCh  chan struct{}
	writeCh chan *batchWriteWorkerInput

	batchWriteTimeMeter       metrics.Meter
	batchWriteCountMeter      metrics.Meter
	batchWriteSizeMeter       metrics.Meter
	batchWriteSecPerItemMeter metrics.Meter
	batchWriteSecPerByteMeter metrics.Meter
}

type DynamoData struct {
	Key []byte `json:"Key" dynamodbav:"Key"`
	Val []byte `json:"Val" dynamodbav:"Val"`
}

// TODO-Klaytn: remove the test config when flag setting is completed
/*
 * Please Run DynamoDB local with docker
 * $ docker pull amazon/dynamodb-local
 * $ docker run -d -p 8000:8000 amazon/dynamodb-local
 */
func createTestDynamoDBConfig() *DynamoDBConfig {
	return &DynamoDBConfig{
		Region:             "ap-northeast-2",
		Endpoint:           "https://dynamodb.ap-northeast-2.amazonaws.com",
		TableName:          "dynamo-test",
		ReadCapacityUnits:  dynamoReadCapacityUnits,
		WriteCapacityUnits: dynamoWriteCapacityUnits,
	}
}

func NewDynamoDB(config *DynamoDBConfig) (*dynamoDB, error) {
	if config == nil {
		return nil, nilDynamoConfigErr
	}

	logger.Info("creating s3FileDB ", "bucket", config.TableName+"-bucket")
	s3FileDB, err := newS3FileDB(config.Region, "https://s3.ap-northeast-2.amazonaws.com", config.TableName+"-bucket")
	if err != nil {
		logger.Error("Unable to create/get S3FileDB", "DB", config.TableName+"-bucket")
		return nil, err
	}

	dynamoDB := &dynamoDB{
		config:  config,
		logger:  logger.NewWith("region", config.Region, "tableName", config.TableName),
		quitCh:  make(chan struct{}),
		writeCh: make(chan *batchWriteWorkerInput, itemChanSize),
		fdb:     s3FileDB,
		db: dynamodb.New(session.Must(session.NewSessionWithOptions(session.Options{
			Config: aws.Config{
				Endpoint: aws.String(config.Endpoint),
				Region:   aws.String(config.Region),
			},
		}))),
	}

	// Check if the table is ready to serve
	for {
		tableStatus, err := dynamoDB.tableStatus()
		if err != nil {
			if !strings.Contains(err.Error(), "ResourceNotFoundException") {
				dynamoDB.logger.Error("unable to get DynamoDB table status", "err", err.Error())
				return nil, err
			}

			dynamoDB.logger.Info("creating a DynamoDB table", "endPoint", config.Endpoint)
			if err := dynamoDB.createTable(); err != nil {
				dynamoDB.logger.Error("unable to create a DynamoDB table", "err", err.Error())
				return nil, err
			}
		}

		switch tableStatus {
		case dynamodb.TableStatusActive:
			dynamoDB.logger.Info("DynamoDB configurations", "endPoint", config.Endpoint)
			for i := 0; i < WorkerNum; i++ {
				go dynamoDB.createBatchWriteWorker()
			}
			dynamoDB.logger.Info("made dynamo batch write workers", "workerNum", WorkerNum)
			return dynamoDB, nil
		case dynamodb.TableStatusDeleting, dynamodb.TableStatusArchiving, dynamodb.TableStatusArchived:
			return nil, errors.New("failed to get DynamoDB table, table status : " + tableStatus)
		default:
			dynamoDB.logger.Info("waiting for the table to be ready", "table status", tableStatus)
			time.Sleep(1 * time.Second)
		}
	}
}

func (dynamo *dynamoDB) createTable() error {
	input := &dynamodb.CreateTableInput{
		// TODO-Klaytn: enable Provisioned mode
		BillingMode: aws.String("PAY_PER_REQUEST"),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("Key"),
				AttributeType: aws.String("B"), // B - the attribute is of type Binary
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("Key"),
				KeyType:       aws.String("HASH"), // HASH - partition key, RANGE - sort key
			},
		},
		// ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
		// 	dynamoReadCapacityUnits:  aws.Int64(dynamo.config.dynamoReadCapacityUnits),
		// 	dynamoWriteCapacityUnits: aws.Int64(dynamo.config.dynamoWriteCapacityUnits),
		// },
		TableName: aws.String(dynamo.config.TableName),
	}

	_, err := dynamo.db.CreateTable(input)
	if err != nil {
		dynamo.logger.Error("Error while creating the DynamoDB table", "err", err, "tableName", dynamo.config.TableName)
		return err
	}
	dynamo.logger.Info("Successfully created the Dynamo table", "tableName", dynamo.config.TableName)
	return nil
}

func (dynamo *dynamoDB) deleteTable() error {
	if _, err := dynamo.db.DeleteTable(&dynamodb.DeleteTableInput{TableName: &dynamo.config.TableName}); err != nil {
		dynamo.logger.Error("Error while deleting the DynamoDB table", "tableName", dynamo.config.TableName)
		return err
	}
	dynamo.logger.Info("Successfully deleted the DynamoDB table", "tableName", dynamo.config.TableName)
	return nil
}

func (dynamo *dynamoDB) tableStatus() (string, error) {
	desc, err := dynamo.tableDescription()
	if err != nil {
		return "", err
	}

	return *desc.TableStatus, nil
}

func (dynamo *dynamoDB) tableDescription() (*dynamodb.TableDescription, error) {
	describe, err := dynamo.db.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(dynamo.config.TableName)})
	if describe == nil {
		return nil, err
	}

	return describe.Table, err
}

func (dynamo *dynamoDB) Type() DBType {
	return DynamoDB
}

// Path returns the path to the database directory.
func (dynamo *dynamoDB) Path() string {
	return fmt.Sprintf("%s-%s", dynamo.config.Region, dynamo.config.Endpoint)
}

// Put inserts the given key and value pair to the database.
func (dynamo *dynamoDB) Put(key []byte, val []byte) error {
	if len(key) == 0 {
		return nil
	}

	if len(key)+len(val) > dynamoWriteSizeLimit {
		_, err := dynamo.fdb.write(item{key: key, val: val})
		if err != nil {
			return err
		}
		return dynamo.Put(key, overSizedDataPrefix)
	}

	data := DynamoData{Key: key, Val: val}
	marshaledData, err := dynamodbattribute.MarshalMap(data)
	if err != nil {
		return err
	}

	params := &dynamodb.PutItemInput{
		TableName: aws.String(dynamo.config.TableName),
		Item:      marshaledData,
	}

	_, err = dynamo.db.PutItem(params)
	if err != nil {
		fmt.Printf("Put ERROR: %v\n", err.Error())
		return err
	}

	return nil
}

// Has returns true if the corresponding value to the given key exists.
func (dynamo *dynamoDB) Has(key []byte) (bool, error) {
	if _, err := dynamo.Get(key); err != nil {
		return false, err
	}
	return true, nil
}

// Get returns the corresponding value to the given key if exists.
func (dynamo *dynamoDB) Get(key []byte) ([]byte, error) {
	params := &dynamodb.GetItemInput{
		TableName: aws.String(dynamo.config.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Key": {
				B: key,
			},
		},
		ConsistentRead: aws.Bool(true),
	}

	result, err := dynamo.db.GetItem(params)
	if err != nil {
		fmt.Printf("Get ERROR: %v\n", err.Error())
		return nil, err
	}

	if result.Item == nil {
		return nil, dataNotFoundErr
	}

	var data DynamoData
	if err := dynamodbattribute.UnmarshalMap(result.Item, &data); err != nil {
		return nil, err
	}

	if data.Val == nil {
		return []byte{}, nil
	}

	if bytes.Equal(data.Val, overSizedDataPrefix) {
		return dynamo.fdb.read(key)
	}

	return data.Val, nil
}

// Delete deletes the key from the queue and database
func (dynamo *dynamoDB) Delete(key []byte) error {
	params := &dynamodb.DeleteItemInput{
		TableName: aws.String(dynamo.config.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Key": {
				B: key,
			},
		},
	}

	_, err := dynamo.db.DeleteItem(params)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err.Error())
		return err
	}
	return nil
}

func (dynamo *dynamoDB) Close() {
	close(dynamo.quitCh)
}

func (dynamo *dynamoDB) Meter(prefix string) {
	// TODO-Klaytn: add metrics
	dynamo.batchWriteTimeMeter = metrics.NewRegisteredMeter(prefix+"batchwrite/time", nil)
	dynamo.batchWriteCountMeter = metrics.NewRegisteredMeter(prefix+"batchwrite/count", nil)
	dynamo.batchWriteSizeMeter = metrics.NewRegisteredMeter(prefix+"batchwrite/size", nil)
	dynamo.batchWriteSecPerItemMeter = metrics.NewRegisteredMeter(prefix+"batchwrite/secperitem", nil)
	dynamo.batchWriteSecPerByteMeter = metrics.NewRegisteredMeter(prefix+"batchwrite/secperbyte", nil)
}

func (dynamo *dynamoDB) NewIterator() Iterator {
	return nil
}

func (dynamo *dynamoDB) NewIteratorWithStart(start []byte) Iterator {
	return nil
}

func (dynamo *dynamoDB) NewIteratorWithPrefix(prefix []byte) Iterator {
	return nil
}

func (dynamo *dynamoDB) createBatchWriteWorker() {
	failCount := 0
	tableName := dynamo.config.TableName
	batchWriteInput := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{},
	}
	dynamo.logger.Info("generate a dynamoDB batchWrite worker", "time", time.Now())

	for {
		select {
		case <-dynamo.quitCh:
			dynamo.logger.Info("close a dynamoDB batchWrite worker", "time", time.Now())
			return
		case batchInput := <-dynamo.writeCh:
			batchWriteInput.RequestItems[tableName] = batchInput.items

			BatchWriteItemOutput, err := dynamo.db.BatchWriteItem(batchWriteInput)
			numUnprocessed := len(BatchWriteItemOutput.UnprocessedItems)
			for err != nil || numUnprocessed != 0 {
				if err != nil {
					failCount++
					dynamo.logger.Warn("dynamoDB failed to write batch items", "err", err, "failCnt", failCount)
					if failCount > dynamoMaxRetry {
						dynamo.logger.Error("dynamoDB failed many times. sleep a second and retry",
							"failCnt", failCount)
						time.Sleep(time.Second)
					}
				}

				if numUnprocessed != 0 {
					dynamo.logger.Debug("dynamoDB batchWrite remains unprocessedItem",
						"numUnprocessedItem", numUnprocessed)
					batchWriteInput.RequestItems[tableName] = BatchWriteItemOutput.UnprocessedItems[tableName]
				}

				BatchWriteItemOutput, err = dynamo.db.BatchWriteItem(batchWriteInput)
				numUnprocessed = len(BatchWriteItemOutput.UnprocessedItems)
			}

			failCount = 0
			batchInput.wg.Done()
		}
	}
}

func (dynamo *dynamoDB) NewBatch() Batch {
	return &dynamoBatch{db: dynamo, tableName: dynamo.config.TableName, wg: &sync.WaitGroup{}}
}

type dynamoBatch struct {
	db         *dynamoDB
	tableName  string
	batchItems []*dynamodb.WriteRequest
	size       int
	wg         *sync.WaitGroup
}

func (batch *dynamoBatch) Put(key, val []byte) error {
	data := DynamoData{Key: key, Val: val}
	dataSize := len(val)
	if dataSize == 0 {
		return nil
	}

	// If the size of the item is larger than the limit, it should be handled in different way
	if len(key)+dataSize > dynamoWriteSizeLimit {
		batch.wg.Add(1)
		go func() {
			failCnt := 0
			batch.db.logger.Debug("write large size data into fileDB")

			_, err := batch.db.fdb.write(item{key: key, val: val})
			for err != nil {
				failCnt++
				batch.db.logger.Error("cannot write an item into fileDB. check the status of s3", "err", err,
					"numRetry", failCnt)
				time.Sleep(time.Second)

				batch.db.logger.Warn("retrying write an item into fileDB")
				_, err = batch.db.fdb.write(item{key: key, val: val})
			}
			batch.wg.Done()
		}()
		data.Val = overSizedDataPrefix
		dataSize = len(data.Val)
	}

	marshaledData, err := dynamodbattribute.MarshalMap(data)
	if err != nil {
		batch.db.logger.Error("err while batch put", "err", err, "len(val)", len(val))
		return err
	}

	batch.batchItems = append(batch.batchItems, &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{Item: marshaledData},
	})
	batch.size += dataSize

	if len(batch.batchItems) == dynamoBatchSize {
		batch.wg.Add(1)
		batch.db.writeCh <- &batchWriteWorkerInput{batch.batchItems, batch.wg}
		batch.Reset()
	}
	return nil
}

func (batch *dynamoBatch) Write() error {
	if batch.size == 0 {
		return nil
	}

	var writeRequest []*dynamodb.WriteRequest
	numRemainedItems := len(batch.batchItems)

	for numRemainedItems > 0 {
		if numRemainedItems > dynamoBatchSize {
			writeRequest = batch.batchItems[:dynamoBatchSize]
			batch.batchItems = batch.batchItems[dynamoBatchSize:]
		} else {
			writeRequest = batch.batchItems
		}
		batch.wg.Add(1)
		batch.db.writeCh <- &batchWriteWorkerInput{batch.batchItems, batch.wg}
		numRemainedItems -= len(writeRequest)
	}

	batch.wg.Wait()
	return nil
}

func (batch *dynamoBatch) ValueSize() int {
	return batch.size
}

func (batch *dynamoBatch) Reset() {
	batch.batchItems = batch.batchItems[:0]
	batch.size = 0
}
