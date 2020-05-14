run:
	TOPICS=test2,test.64,kafka_client_test \
	GROUP_NAME=test_diego \
	KAFKA_AGGREGATE_BROKERS=xxx:9093 \
	KAFKA_CLOUD_AGGREGATE_CLIENT_CERT=path-to-cert \
	KAFKA_CLOUD_AGGREGATE_CLIENT_KEY=path-to-key \
	go run main.go
