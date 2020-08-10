package database

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/klaytn/klaytn/common/hexutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/klaytn/klaytn/log"
)

// s3FileDB is an implementation of fileDB based on AWS S3.
// It stores the data to the designated AWS S3 bucket.
type s3FileDB struct {
	region   string
	endpoint string
	bucket   string
	s3       *s3.S3
	logger   log.Logger
}

// newS3FileDB returns a new s3FileDB with the given region, endpoint and bucketName.
// If the given bucket does not exist, it creates one.
func newS3FileDB(region, endpoint, bucketName string) (*s3FileDB, error) {
	localLogger := logger.NewWith("endpoint", endpoint, "bucketName", bucketName)
	sessionConf, err := session.NewSession(&aws.Config{
		Region:           aws.String(region),
		Endpoint:         aws.String(endpoint),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		localLogger.Error("failed to create session", "region", region)
		return nil, err
	}

	s3DB := &s3FileDB{
		region:   region,
		endpoint: endpoint,
		bucket:   bucketName,
		s3:       s3.New(sessionConf),
		logger:   localLogger,
	}

	exist, err := s3DB.hasBucket(bucketName)
	if err != nil {
		localLogger.Error("failed to retrieve a bucket list", "err", err)
		return nil, err
	}
	if !exist {
		localLogger.Warn("cannot find the bucket. creating the bucket")
		_, err = s3DB.s3.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			localLogger.Error("failed to create a bucket", "err", err)
			return nil, err
		}
	}
	return s3DB, nil
}

// hasBucket returns if the bucket exists in the endpoint of s3FileDB.
func (s3DB *s3FileDB) hasBucket(bucketName string) (bool, error) {
	output, err := s3DB.s3.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		return false, err
	}

	bucketExist := false
	for _, bucket := range output.Buckets {
		if bucketName == *bucket.Name {
			bucketExist = true
			break
		}
	}
	return bucketExist, nil
}

// write puts list of items to its bucket and returns the list of URIs.
func (s3DB *s3FileDB) write(item item) (string, error) {
	o := &s3.PutObjectInput{
		Bucket:      aws.String(s3DB.bucket),
		Key:         aws.String(hexutil.Encode(item.key)),
		Body:        bytes.NewReader(item.val),
		ContentType: aws.String("application/octet-stream"),
	}

	if _, err := s3DB.s3.PutObject(o); err != nil {
		return "", fmt.Errorf("failed to write item to S3. key: %v, err: %w", string(item.key), err)
	}

	return hexutil.Encode(item.key), nil
}

// read gets the data from the bucket with the given key.
func (s3DB *s3FileDB) read(key []byte) ([]byte, error) {
	var returnVal []byte

	output, err := s3DB.s3.GetObject(&s3.GetObjectInput{
		Bucket:              aws.String(s3DB.bucket),
		Key:                 aws.String(hexutil.Encode(key)),
		ResponseContentType: aws.String("application/octet-stream"),
	})
	if err != nil {
		return nil, err
	}

	bodySize := int(*output.ContentLength)
	totalReadSize, currReadSize := 0, 0

	// Below loop is to load all the data with the given key,
	// as `Read` method does not read all the data when the data is large.
	// The loop continues until there's nothing remaining.
	buf := make([]byte, bodySize)
	for totalReadSize < bodySize {
		currReadSize, err = output.Body.Read(buf)
		if err != nil && !strings.Contains(err.Error(), "EOF") {
			return nil, err
		}

		returnVal = append(returnVal, buf[:currReadSize]...)
		totalReadSize += currReadSize
		buf = buf[:0]
	}

	return returnVal, nil
}

// delete removes the data with the given key from the bucket.
// No error is returned if the data with the given key does not exist.
func (s3DB *s3FileDB) delete(key []byte) error {
	_, err := s3DB.s3.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s3DB.bucket),
		Key:    aws.String(hexutil.Encode(key)),
	})
	return err
}

// deleteBucket removes the bucket
func (s3DB *s3FileDB) deleteBucket() {
	if _, err := s3DB.s3.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(s3DB.bucket)}); err != nil {
		s3DB.logger.Error("failed to delete the test bucket", "err", err, "bucketName", s3DB.bucket)
	}
}
