package consumer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class HandlerTest {

    public void successTest() {
        Handler subject = new Handler();
        System.getenv().put("DDB_CHECKPOINT_TABLE_NAME", "testTable");
        KinesisEvent inputEvent = new KinesisEvent();
        KinesisEventRecord inputRecord = new KinesisEventRecord();
        inputRecord.setAwsRegion("us-east-1");
        Record record = new Record();
        record.setPartitionKey("1");
        record.setData(Base64.getEncoder().encode(Charset.forName("UTF-8").encode("Hello, World")));
        inputRecord.setKinesis(record);
        inputEvent.setRecords(Collections.singletonList(inputRecord));

        Context context = new TestContext();
        subject.handleRequest(inputEvent, context);
    }

    @Test
    public void testJsonParsing() throws JsonProcessingException {
        String jsonString = "{ \"key\": 2049761200, \"commitTimestamp\": \"2021-10-12T19:16:14Z\"}";
        assertThat(new ObjectMapper().readTree(jsonString).at("/commitTimestamp").asText()).isEqualTo("2021-10-12T19:16:14Z");
    }

    @Test
    public void testExtractStreamName() {
        String eventSourceArn = "arn:aws:kinesis:us-east-1:1000000000:stream/kds-stream-1/consumer/kds-replicator:843564834";
        String streamName = eventSourceArn.split(":")[5].split("/")[1];
        assertThat(streamName).isEqualTo("kds-stream-1");
    }

}
