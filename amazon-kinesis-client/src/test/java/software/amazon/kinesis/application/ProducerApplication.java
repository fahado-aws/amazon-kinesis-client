package software.amazon.kinesis.application;

import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.ScalingType;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountRequest;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountResponse;
import software.amazon.kinesis.config.KCLAppConfig;
import software.amazon.kinesis.utils.ReshardOptions;
import software.amazon.kinesis.utils.StreamExistenceManager;

@Slf4j
public class ProducerApplication {

    private KCLAppConfig producerConfig;
    public final KinesisAsyncClient kinesisClientForStreamOwner;
    private StreamExistenceManager streamExistenceManager;
    private ScheduledExecutorService producerExecutor;
    private ScheduledFuture<?> producerFuture;
    private final ObjectMapper mapper = new ObjectMapper();
    public int successfulPutRecords = 0;
    public BigInteger payloadCounter = new BigInteger("0");

    public ProducerApplication (KCLAppConfig producerConfig) throws IOException, URISyntaxException {
        this.producerConfig = producerConfig;
        this.kinesisClientForStreamOwner = this.producerConfig.buildAsyncKinesisClientForStreamOwner();
    }

    public void run() throws URISyntaxException, IOException {
        // Skip cross account tests if no cross account credentials are provided
        if (this.producerConfig.isCrossAccount()) {
            assumeTrue(this.producerConfig.getCrossAccountCredentialsProvider() != null);
        }
        createResources();
        startProducer();
    }

    public void stop() throws Exception {
        stop(false);
    }

    public void stop(Boolean cleanupResources) throws Exception {
        log.info("Cancelling producer and shutting down executor.");
        if (producerFuture != null) {
            producerFuture.cancel(false);
        }
        if (producerExecutor != null) {
            producerExecutor.shutdown();
        }
        if (cleanupResources) {
            cleanupResources();
        }
    }

    private void createResources() throws URISyntaxException, IOException {
        this.streamExistenceManager = new StreamExistenceManager(this.producerConfig);
        this.streamExistenceManager.checkStreamsAndCreateIfNecessary();
    }

    private void cleanupResources() throws Exception {
        log.info("-------------Start deleting streams.---------");
        for (String streamName : this.producerConfig.getStreamNames()) {
            log.info("Deleting stream {}", streamName);
            try {
                this.streamExistenceManager.deleteResource(streamName);
            } catch (Exception e) {
                log.warn("Failed to delete stream: {}, {}", streamName, e.getMessage());
            }
        }
        log.info("---------Finished deleting resources.---------");
    }

    private void startProducer() {
        this.producerExecutor = Executors.newSingleThreadScheduledExecutor();
        this.producerFuture = this.producerExecutor.scheduleAtFixedRate(this::publishRecord, 10, 1, TimeUnit.SECONDS);

        // Reshard logic if required for the test
        if (this.producerConfig.getReshardFactorList() != null) {
            log.info("----Reshard Config found: {}", this.producerConfig.getReshardFactorList());

            for (String streamName : this.producerConfig.getStreamNames()) {
                final StreamScaler streamScaler = new StreamScaler(kinesisClientForStreamOwner, streamName,
                        this.producerConfig.getReshardFactorList(), this.producerConfig);

                // Schedule the stream scales 4 minutes apart with 2 minute starting delay
                for (int i = 0; i < this.producerConfig.getReshardFactorList()
                                                  .size(); i++) {
                    this.producerExecutor.schedule(streamScaler, (4 * i) + 2, TimeUnit.MINUTES);
                }
            }
        }
    }

    public void publishRecord() {
        for (String streamName : producerConfig.getStreamNames()) {
            try {
                final PutRecordRequest request = PutRecordRequest.builder()
                                          .partitionKey(RandomStringUtils.randomAlphabetic(5, 20))
                                          .streamName(streamName)
                                          .data(SdkBytes.fromByteBuffer(wrapWithCounter(5, payloadCounter))) // 1024
                                          // is 1 KB
                                          .build();
                kinesisClientForStreamOwner.putRecord(request)
                                           .get();

                // Increment the payload counter if the putRecord call was successful
                payloadCounter = payloadCounter.add(new BigInteger("1"));
                successfulPutRecords += 1;
                log.info("---------Record published for stream {}, successfulPutRecords is now: {}",
                        streamName, successfulPutRecords);
            } catch (InterruptedException e) {
                log.info("Interrupted, assuming shutdown. ", e);
            } catch (ExecutionException | RuntimeException e) {
                log.error("Error during publish records", e);
            }
        }
    }

    private ByteBuffer wrapWithCounter(int payloadSize, BigInteger payloadCounter) throws RuntimeException {
        final byte[] returnData;
        log.info("---------Putting record with data: {}", payloadCounter);
        try {
            returnData = mapper.writeValueAsBytes(payloadCounter);
        } catch (Exception e) {
            throw new RuntimeException("Error converting object to bytes: ", e);
        }
        return ByteBuffer.wrap(returnData);
    }


    @Data
    private static class StreamScaler implements Runnable {
        private final KinesisAsyncClient client;
        private final String streamName;
        private final List<ReshardOptions> scalingFactors;
        private final KCLAppConfig consumerConfig;
        private int scalingFactorIdx = 0;
        private DescribeStreamSummaryRequest describeStreamSummaryRequest;

        private synchronized void scaleStream() throws InterruptedException, ExecutionException {
            final DescribeStreamSummaryResponse response = client.describeStreamSummary(describeStreamSummaryRequest).get();

            final int openShardCount = response.streamDescriptionSummary().openShardCount();
            final int targetShardCount = scalingFactors.get(scalingFactorIdx).calculateShardCount(openShardCount);

            log.info("Scaling stream {} from {} shards to {} shards w/ scaling factor {}",
                    streamName, openShardCount, targetShardCount, scalingFactors.get(scalingFactorIdx));

            final UpdateShardCountRequest updateShardCountRequest = UpdateShardCountRequest.builder()
                    .streamName(streamName).targetShardCount(targetShardCount).scalingType(ScalingType.UNIFORM_SCALING).build();
            final UpdateShardCountResponse shardCountResponse = client.updateShardCount(updateShardCountRequest).get();
            log.info("Executed shard scaling request. Response Details : {}", shardCountResponse.toString());

            scalingFactorIdx++;
        }

        @Override
        public void run() {
            if (scalingFactors.size() == 0 || scalingFactorIdx >= scalingFactors.size()) {
                log.info("No scaling factor found in list");
                return;
            }
            log.info("Starting stream scaling with params : {}", this);

            if (describeStreamSummaryRequest == null) {
                describeStreamSummaryRequest = DescribeStreamSummaryRequest.builder().streamName(streamName).build();
            }
            try {
                scaleStream();
            } catch (InterruptedException | ExecutionException e) {
                log.error("Caught error while scaling shards for stream", e);
            } finally {
                log.info("Reshard List State : {}", scalingFactors);
            }
        }
    }
}