package consumer;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.Record;
import com.amazonaws.util.StringUtils;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Collections;

import static java.util.Arrays.asList;
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
    public void testJsonParsing() {
        String jsonString = "{ \"key\": 2049761200, \"commitTimestamp\": \"2021-10-12T19:16:14Z\"}";
        JsonObject jsonObject = new Gson().fromJson(jsonString, JsonObject.class);
        assertThat(jsonObject.get("commitTimestamp").getAsString()).isEqualTo("2021-10-12T19:16:14Z");
    }

    @Test
    public void testExtractStreamName() {
        String eventSourceArn = "arn:aws:kinesis:us-east-1:1000000000:stream/kds-stream-1/consumer/kds-replicator:843564834";
        String streamName = eventSourceArn.split(":")[5].split("/")[1];
        assertThat(streamName).isEqualTo("kds-stream-1");
    }

}
