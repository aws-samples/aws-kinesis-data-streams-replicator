package consumer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Handler implements RequestHandler<KinesisEvent, StreamsEventResponse> {

    private static final Logger logger = LoggerFactory.getLogger(Handler.class);

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    private static final CharsetDecoder utf8Decoder = Charset.forName("UTF-8").newDecoder();

    private AmazonKinesis kinesisForwarder = null;

    private DynamoDB docClient = null;

    public Handler() {
        AWSCredentialsProvider provider = new DefaultAWSCredentialsProviderChain();
        //AWSCredentialsProvider provider = new STSAssumeRoleSessionCredentialsProvider(new DefaultAWSCredentialsProviderChain(), "<RoleToAssumeARN>", "KinesisForwarder");

        //Set max conns to 1 since we use this client serially
        ClientConfiguration kinesisConfig = new ClientConfiguration();
        kinesisConfig.setMaxConnections(1);
        kinesisConfig.setProtocol(Protocol.HTTPS);
        kinesisConfig.setConnectionTimeout(30000);
        kinesisConfig.setSocketTimeout(30000);

        this.kinesisForwarder = AmazonKinesisClientBuilder.standard()
                .withClientConfiguration(kinesisConfig)
                .withRegion(System.getenv("TARGET_STREAM_REPLICATION_REGION"))
                .build();

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        docClient = new DynamoDB(client);

    }

    @Override
    public StreamsEventResponse handleRequest(KinesisEvent kinesisEvent, Context context) {
        List<StreamsEventResponse.BatchItemFailure> failures = new ArrayList<>();
        String streamName = getStreamName(kinesisEvent.getRecords().get(0));
        String currentRegion = System.getenv("AWS_REGION");
        if (isStreamActiveInCurrentRegion(streamName, currentRegion, docClient)) {
            Table checkpointTable = docClient.getTable(System.getenv("DDB_CHECKPOINT_TABLE_NAME"));

            int batchSize = kinesisEvent.getRecords().size();
            AtomicInteger successful = new AtomicInteger();
            AtomicReference<String> currentSequenceNumber = new AtomicReference<>("");
            try {
                for (KinesisEvent.KinesisEventRecord record : kinesisEvent.getRecords()) {
                    currentSequenceNumber.getAndSet(record.getKinesis().getSequenceNumber());
                    String actualDataString = utf8Decoder.decode(record.getKinesis().getData()).toString();
                    logger.info("Actual Record SequenceNumber: {}, Data: {}", currentSequenceNumber, actualDataString);

                    // Forward to other region

                    PutRecordRequest putRecordRequest = new PutRecordRequest();
                    putRecordRequest.setStreamName(streamName);
                    putRecordRequest.setPartitionKey(record.getKinesis().getPartitionKey());
                    putRecordRequest.setData(ByteBuffer.wrap(actualDataString.getBytes("UTF-8")));
                    this.kinesisForwarder.putRecord(putRecordRequest);

                    // Checkpoint
                    Item item = transform(streamName, actualDataString);

                    checkpointTable.putItem(item);
                    successful.getAndIncrement();
                }
            } catch (Exception e) {
                failures.add(StreamsEventResponse.BatchItemFailure.builder().withItemIdentifier(currentSequenceNumber.get()).build());
                logger.error("Error while processing batch", e);
            }
            logger.info("Total Batch Size: {}, Successfully Processed: {}", batchSize, successful.get());
        } else {
            logger.info("Stream {} is not active in the current region", streamName);
        }
        return StreamsEventResponse.builder().withBatchItemFailures(failures).build();
    }

    private boolean isStreamActiveInCurrentRegion(String streamName, String currentRegion, DynamoDB docClient) {
        try {
            Table activeRegionConfigTable = docClient.getTable(System.getenv("DDB_ACTIVE_REGION_CONFIG_TABLE_NAME"));

            GetItemSpec spec = new GetItemSpec().withPrimaryKey("streamName", streamName);

            Item item = activeRegionConfigTable.getItem(spec);

            String activeRegion = item.get("activeRegion").toString();

            return currentRegion.equalsIgnoreCase(activeRegion);
        } catch (Exception e) {
            logger.error("Error while attempting to fetch current stream's active region");
        }
        return true;
    }

    private Item transform(String streamName, String actualDataString) {
        String commitTimestamp = gson.fromJson(actualDataString, JsonObject.class).get("commitTimestamp").getAsString();

        return new Item()
                .withPrimaryKey("streamName", streamName)
                .withString("lastReplicatedCommitTimestamp", commitTimestamp);
    }

    private String getStreamName(KinesisEvent.KinesisEventRecord record) {
        return record.getEventSourceARN().split(":")[5].split("/")[1];
    }
}
