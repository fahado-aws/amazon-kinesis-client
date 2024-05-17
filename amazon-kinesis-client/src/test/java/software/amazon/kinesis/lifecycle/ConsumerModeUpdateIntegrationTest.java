package software.amazon.kinesis.lifecycle;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import lombok.AllArgsConstructor;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.kinesis.application.ConsumerApplication;
import software.amazon.kinesis.application.ProducerApplication;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.config.KCLAppConfig;
import software.amazon.kinesis.config.ReleaseCanaryPollingH2TestConfig;
import software.amazon.kinesis.config.ReleaseCanarySingleStreamCompatiblePollingH2TestConfig;
import software.amazon.kinesis.config.ReleaseCanarySingleStreamUpgradePollingH2TestConfig;
import software.amazon.kinesis.config.multistream.ReleaseCanaryMultiStreamPollingH2TestConfig;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy;
import software.amazon.kinesis.processor.MultiStreamTracker;
import software.amazon.kinesis.processor.StreamTracker;
import software.amazon.kinesis.utils.RecordValidatorQueue;

@RunWith(Enclosed.class)
public class ConsumerModeUpdateIntegrationTest {

    enum KCLMode {
        SingleStream,
        SingleStreamCompatible,
        SingleStreamUpgrade,
        MultiStream
    }

    @RunWith(Parameterized.class)
    @AllArgsConstructor
    public static class HappyPathIntegrationTest {

        @Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {
                    { KCLMode.SingleStream, KCLMode.SingleStreamCompatible },
                    { KCLMode.SingleStreamCompatible, KCLMode.SingleStreamUpgrade },
                    { KCLMode.SingleStreamUpgrade, KCLMode.MultiStream },
            });
        }

        private KCLMode prevMode;
        private KCLMode nextMode;

        @Test
        public void consumerModeUpdateTest() throws Exception {

            final UUID uniqueId = UUID.randomUUID();
            final String testName = "ModeUpdateFrom" + prevMode + "to" + nextMode + "Test_" + uniqueId;
            final String streamName = testName;

            RecordValidatorQueue recordValidator = new RecordValidatorQueue();
            KCLAppConfig prevKCLAppConfig = getKCLAppConfig(prevMode, testName, streamName, recordValidator);
            KCLAppConfig nextKCLAppConfig = getKCLAppConfig(nextMode, testName, streamName, recordValidator);

            ProducerApplication producer = new ProducerApplication(prevKCLAppConfig);
            ConsumerApplication consumerA = new ConsumerApplication(prevKCLAppConfig, "A");
            ConsumerApplication consumerBPrev = new ConsumerApplication(prevKCLAppConfig, "BPrev");
            ConsumerApplication consumerBNext = new ConsumerApplication(nextKCLAppConfig, "BNext");
            

            // Sleep to allow the producer/consumer to run and then end the test case.
            // If non-reshard sleep 1 minutes, else sleep 4 minutes per scale.
            final int sleepMinutes = (prevKCLAppConfig.getReshardFactorList() == null) ? 3
                    : (4 * prevKCLAppConfig.getReshardFactorList().size());

            try {
                // Phase 1 : Run
                producer.run();
                consumerA.run();
                consumerBPrev.run();
                Thread.sleep(TimeUnit.MINUTES.toMillis(sleepMinutes));
                producer.stop();
                // Wait a few seconds for the last few records to be processed
                Thread.sleep(TimeUnit.SECONDS.toMillis(10));
                consumerBPrev.stop();

                // Phase1 : Verification
                assertEquals(true, recordValidator.validateRecordsDelivery(producer.successfulPutRecords));

                // Phase 2 : Run
                producer.run();
                consumerBNext.run();
                Thread.sleep(TimeUnit.MINUTES.toMillis(sleepMinutes));
                producer.stop();
                // Wait a few seconds for the last few records to be processed
                Thread.sleep(TimeUnit.SECONDS.toMillis(10));
                consumerA.stop();
                consumerBNext.stop();

                // Phase 2 : Verification
                assertEquals(true, recordValidator.validateRecordsDelivery(producer.successfulPutRecords));
                verifyLeasesType(prevKCLAppConfig.buildDynamoDbClient(), KCLAppConfig.INTEGRATION_TEST_RESOURCE_PREFIX + testName);

            } finally {
                // Clean up
                producer.stop(true);
                consumerA.stop(true);
                consumerBPrev.stop(true);
                consumerBNext.stop(true);
            }
        }

        private void verifyLeasesType(DynamoDbClient ddbClient, String tableName) throws Exception {
            ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).build();
            ScanResponse scanResponse = ddbClient.scan(scanRequest);
            for (Map<String, AttributeValue> item : scanResponse.items()){
                String leaseOwner = item.get("leaseOwner").s();
                if (leaseOwner.endsWith("A")) {
                    assertEquals(true, verifyLeaseType(item, prevMode));
                } else if (leaseOwner.endsWith("BNext")) {
                    assertEquals(true, verifyLeaseType(item, nextMode));
                } else {
                    throw new Exception("A stale lease owned by " + leaseOwner + " found after running the scenario.");
                }
            }
        }

        private Boolean verifyLeaseType(Map<String, AttributeValue> lease, KCLMode mode) throws Exception {
            AttributeValue streamName = lease.get("streamName");
            AttributeValue shardId = lease.get("shardId");
            switch (mode) {
                case SingleStream:
                    return streamName == null && shardId == null;
                case SingleStreamCompatible:
                    return true;
                case SingleStreamUpgrade:
                case MultiStream:
                    return streamName != null && shardId != null;
                default:
                    throw new Exception("Unable to verify lease type with the given mode: " + mode);
            }
        }
    }

    private static KCLAppConfig getKCLAppConfig(KCLMode mode, String testName, String streamName,
            RecordValidatorQueue recordValidator)
            throws Exception {
        switch (mode) {
            case SingleStream:
                return getSingleStreamConfig(testName, streamName, recordValidator);
            case SingleStreamCompatible:
                return getSingleStreamCompatibleConfig(testName, streamName, recordValidator);
            case SingleStreamUpgrade:
                return getSingleStreamUpgradeConfig(testName, streamName, recordValidator);
            case MultiStream:
                return getMultiStreamConfig(testName, streamName, recordValidator);
            default:
                throw new Exception("Unable to get KCLAppConfig with the given mode: " + mode);
        }
    }

    private static KCLAppConfig getSingleStreamConfig(String testName, String streamName,
            RecordValidatorQueue recordValidator) {
        return new ReleaseCanaryPollingH2TestConfig() {
            @Override
            public String getTestName() {
                return testName;
            }

            @Override
            public List<Arn> getStreamArns() {
                return Collections.singletonList(buildStreamArn(streamName));
            }

            @Override
            public RecordValidatorQueue getRecordValidator() {
                return recordValidator;
            }
        };
    }

    private static KCLAppConfig getSingleStreamCompatibleConfig(String testName, String streamName,
            RecordValidatorQueue recordValidator) {
        return new ReleaseCanarySingleStreamCompatiblePollingH2TestConfig() {
            @Override
            public String getTestName() {
                return testName;
            }

            @Override
            public List<Arn> getStreamArns() {
                return Collections.singletonList(buildStreamArn(streamName));
            }

            @Override
            public RecordValidatorQueue getRecordValidator() {
                return recordValidator;
            }
        };
    }

    private static KCLAppConfig getSingleStreamUpgradeConfig(String testName, String streamName,
            RecordValidatorQueue recordValidator) {
        return new ReleaseCanarySingleStreamUpgradePollingH2TestConfig() {
            @Override
            public String getTestName() {
                return testName;
            }

            @Override
            public List<Arn> getStreamArns() {
                return Collections.singletonList(buildStreamArn(streamName));
            }

            @Override
            public RecordValidatorQueue getRecordValidator() {
                return recordValidator;
            }
        };
    }

    private static KCLAppConfig getMultiStreamConfig(String testName, String streamName,
            RecordValidatorQueue recordValidator) {
        return new ReleaseCanaryMultiStreamPollingH2TestConfig() {
            @Override
            public String getTestName() {
                return testName;
            }

            @Override
            public List<Arn> getStreamArns() {
                return Collections.singletonList(buildStreamArn(streamName));
            }

            @Override
            public RecordValidatorQueue getRecordValidator() {
                return recordValidator;
            }

            @Override
            public StreamTracker getStreamTracker(Map<Arn, Arn> streamToConsumerArnsMap) {
                final MultiStreamTracker multiStreamTracker = new MultiStreamTracker() {
                    @Override
                    public List<StreamConfig> streamConfigList() {
                        return getStreamArns().stream().map(streamArn -> {
                            final StreamIdentifier streamIdentifier;
                            streamIdentifier = StreamIdentifier.multiStreamInstance(streamArn,
                                    getCreationEpoch(streamArn));

                            if (streamToConsumerArnsMap != null) {
                                final StreamConfig streamConfig = new StreamConfig(streamIdentifier,
                                        InitialPositionInStreamExtended.newInitialPosition(getInitialPosition()));
                                return streamConfig.consumerArn(streamToConsumerArnsMap.get(streamArn).toString());
                            } else {
                                return new StreamConfig(streamIdentifier,
                                        InitialPositionInStreamExtended.newInitialPosition(getInitialPosition()));
                            }
                        }).collect(Collectors.toList());
                    }

                    @Override
                    public FormerStreamsLeasesDeletionStrategy formerStreamsLeasesDeletionStrategy() {
                        return new FormerStreamsLeasesDeletionStrategy.NoLeaseDeletionStrategy();
                    }
                };
                return multiStreamTracker;
            }
        };
    }
}
