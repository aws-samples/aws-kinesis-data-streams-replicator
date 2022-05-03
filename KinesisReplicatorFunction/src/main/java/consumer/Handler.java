package consumer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static software.amazon.awssdk.services.dynamodb.model.ComparisonOperator.EQ;

public class Handler implements RequestHandler<KinesisEvent, StreamsEventResponse> {

    private static final Logger logger = LoggerFactory.getLogger(Handler.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final CharsetDecoder utf8Decoder = StandardCharsets.UTF_8.newDecoder();

    private KinesisClient kinesisForwarder = null;
    private DynamoDbClient ddbClient = null;
    private CloudWatchClient cw = null;

    public Handler() {

        kinesisForwarder = KinesisClient.builder()
                .region(Region.of(System.getenv("TARGET_STREAM_REPLICATION_REGION")))
                .httpClient(ApacheHttpClient.create())
                .build();
        ddbClient = DynamoDbClient.create();
        cw = CloudWatchClient.builder()
                .region(Region.of(System.getenv("TARGET_STREAM_REPLICATION_REGION")))
                .httpClient(ApacheHttpClient.create())
                .build();
    }

    @Override
    public StreamsEventResponse handleRequest(KinesisEvent kinesisEvent, Context context) {
        Instant start = Instant.now();
        List<StreamsEventResponse.BatchItemFailure> failures = new ArrayList<>();
        String streamName = getStreamName(kinesisEvent.getRecords().get(0));
        String currentRegion = System.getenv("AWS_REGION");
        if (isStreamActiveInCurrentRegion(streamName, currentRegion)) {
            int batchSize = kinesisEvent.getRecords().size();
            AtomicInteger successful = new AtomicInteger();
            AtomicReference<String> currentSequenceNumber = new AtomicReference<>("");
            try {
                for (KinesisEvent.KinesisEventRecord record : kinesisEvent.getRecords()) {
                    currentSequenceNumber.getAndSet(record.getKinesis().getSequenceNumber());
                    String actualDataString = utf8Decoder.decode(record.getKinesis().getData()).toString();
                    logger.info("Actual Record SequenceNumber: {}, Data: {}", currentSequenceNumber, actualDataString);

                    // Forward to other region

                    PutRecordRequest putRecordRequest = PutRecordRequest.builder()
                            .streamName(streamName)
                            .partitionKey(record.getKinesis().getPartitionKey())
                            .data(SdkBytes.fromString(actualDataString, StandardCharsets.UTF_8))
                            .build();
                    kinesisForwarder.putRecord(putRecordRequest);
                    ddbClient.putItem(buildItem(streamName, actualDataString));

                    successful.getAndIncrement();
                }
            } catch (Exception e) {
                failures.add(StreamsEventResponse.BatchItemFailure.builder().withItemIdentifier(currentSequenceNumber.get()).build());
                logger.error("Error while processing batch", e);
            }
            Instant end = Instant.now();
            int successfullyProcessed = successful.get();
            long replicationLatency = Duration.between(start, end).toSeconds();
            logger.info("TotalBatchSize {} Successful {} TimeTakenSeconds {}", batchSize, successfullyProcessed, replicationLatency);
            try {
                MetricDatum throughputMetricData = MetricDatum.builder()
                        .metricName("ThroughPut")
                        .dimensions(Dimension.builder()
                                .name("StreamName")
                                .value(streamName)
                                .build())
                        .value(Double.valueOf(successfullyProcessed))
                        .build();
                MetricDatum replicationLatencyMetricData = MetricDatum.builder()
                        .metricName("ReplicationLatency")
                        .dimensions(Dimension.builder()
                                .name("StreamName")
                                .value(streamName)
                                .build())
                        .value(Double.valueOf(replicationLatency))
                        .build();
                cw.putMetricData(PutMetricDataRequest.builder()
                                .namespace("KinesisCrossRegionReplication")
                                .metricData(asList(throughputMetricData, replicationLatencyMetricData))
                        .build());
            } catch (Throwable e) {
                logger.error("Error while publishing metric to cloudwatch");
            }
        } else {
            logger.info("Stream {} is not active in the current region", streamName);
        }
        return StreamsEventResponse.builder().withBatchItemFailures(failures).build();
    }

    private PutItemRequest buildItem(String streamName, String actualDataString) throws JsonProcessingException {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("streamName", AttributeValue.builder().s(streamName).build());
        item.put("lastReplicatedCommitTimestamp", AttributeValue.builder().s(objectMapper.readTree(actualDataString).at("/commitTimestamp").asText()).build());
        return PutItemRequest.builder()
                .tableName(System.getenv("DDB_CHECKPOINT_TABLE_NAME"))
                .item(item)
                .build();
    }

    private boolean isStreamActiveInCurrentRegion(String streamName, String currentRegion) {
        try {

            Map<String, Condition> keyConditions = Collections.singletonMap("streamName", Condition.builder()
                            .comparisonOperator(EQ)
                            .attributeValueList(AttributeValue.builder().s(streamName).build())
                    .build());
            QueryResponse queryResponse = ddbClient.query(QueryRequest.builder()
                    .tableName(System.getenv("DDB_ACTIVE_REGION_CONFIG_TABLE_NAME"))
                    .keyConditions(keyConditions)
                    .attributesToGet("activeRegion")
                    .build());

            if(!queryResponse.hasItems()) {
                logger.warn("Stream is not configured for cross region replication");
                return false;
            } else {
                if (queryResponse.count() > 1) {
                    logger.error("A stream cannot be active in more than one region");
                    return false;
                }
                AttributeValue activeRegionAttributeValue = queryResponse.items().get(0).get("activeRegion");
                return currentRegion.equalsIgnoreCase(activeRegionAttributeValue.s());
            }
        } catch (Exception e) {
            logger.error("Error while attempting to fetch current stream's active region");
        }
        return false;
    }

    private String getStreamName(KinesisEvent.KinesisEventRecord record) {
        return record.getEventSourceARN().split(":")[5].split("/")[1];
    }
}
