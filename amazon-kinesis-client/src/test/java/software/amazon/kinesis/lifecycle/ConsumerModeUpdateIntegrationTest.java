package software.amazon.kinesis.lifecycle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

import com.google.common.collect.Lists;

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
    public static class HappyPathParameterizedIntegrationTest {

        @Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {
                    // Upgrade
                    { KCLMode.SingleStream, KCLMode.SingleStreamCompatible },
                    { KCLMode.SingleStreamCompatible, KCLMode.SingleStreamUpgrade },
                    { KCLMode.SingleStreamUpgrade, KCLMode.MultiStream },
                    // Rollback
                    { KCLMode.SingleStreamCompatible, KCLMode.SingleStream },
                    { KCLMode.SingleStreamUpgrade, KCLMode.SingleStreamCompatible },
                    { KCLMode.MultiStream, KCLMode.SingleStreamUpgrade },
                    { KCLMode.MultiStream, KCLMode.SingleStreamCompatible },
            });
        }

        private KCLMode prevMode;
        private KCLMode nextMode;

        @Test
        public void consumerModeUpdateTest() throws Exception {
            ConsumerModeUpdateIntegrationTestHelper helper = new ConsumerModeUpdateIntegrationTestHelper(prevMode,
                    nextMode);
            try {
                helper.runProducerAndConsumersThenWait(Lists.newArrayList(helper.consumerA, helper.consumerBPrev));
                helper.stopProducerAndConsumers(Lists.newArrayList(helper.consumerBPrev));
                assertTrue(helper.validateRecordsDelivery());

                
                helper.runProducerAndConsumersThenWait(Lists.newArrayList(helper.consumerBNext));
                helper.stopProducerAndConsumers(Lists.newArrayList(helper.consumerBNext, helper.consumerA));
                assertTrue(helper.validateRecordsDelivery());
                verifyLeasesType(helper.prevKCLAppConfig.buildDynamoDbClient(),
                        KCLAppConfig.INTEGRATION_TEST_RESOURCE_PREFIX + helper.testName, prevMode, nextMode);

            } finally {
                // Clean up
                helper.cleanup();
            }
        }
    }

    @RunWith(Parameterized.class)
    @AllArgsConstructor
    public static class FailurePathParameterizedIntegrationTest {

        @Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {
                    // Upgrade
                    { KCLMode.SingleStream, KCLMode.MultiStream },
                    { KCLMode.SingleStreamCompatible, KCLMode.MultiStream },
                    // Rollback
                    { KCLMode.SingleStreamUpgrade, KCLMode.SingleStream },
                    { KCLMode.MultiStream, KCLMode.SingleStream },
            });
        }

        private KCLMode prevMode;
        private KCLMode nextMode;

        @Test
        public void consumerModeUpdateTest() throws Exception {
            ConsumerModeUpdateIntegrationTestHelper helper = new ConsumerModeUpdateIntegrationTestHelper(prevMode,
                    nextMode);
            try {
                helper.runProducerAndConsumersThenWait(Lists.newArrayList(helper.consumerA, helper.consumerBPrev));
                helper.stopProducerAndConsumers(Lists.newArrayList(helper.consumerBPrev));
                assertTrue(helper.validateRecordsDelivery());

                helper.runProducerAndConsumersThenWait(Lists.newArrayList(helper.consumerBNext));
                assertTrue(helper.consumerBNext.isTerminated());
                assertTrue(helper.validateRecordsDelivery());
                assertEquals(0, getLeaseCountOwnedBy(helper.prevKCLAppConfig.buildDynamoDbClient(),
                        KCLAppConfig.INTEGRATION_TEST_RESOURCE_PREFIX + helper.testName, "B"));

            } finally {
                // Clean up
                helper.cleanup();
            }
        }
    }

    public class FailurePathNonParameterizedIntegrationTest {
        @Test
        public void consumerModeUpdateFromSingleStreamToSingleStreamUpgradeTest() throws Exception {
            KCLMode prevMode = KCLMode.SingleStream;
            KCLMode nextMode = KCLMode.SingleStreamUpgrade;
            ConsumerModeUpdateIntegrationTestHelper helper = new ConsumerModeUpdateIntegrationTestHelper(prevMode,
                    nextMode);
            try {
                helper.runProducerAndConsumersThenWait(Lists.newArrayList(helper.consumerA, helper.consumerBPrev));
                helper.stopProducerAndConsumers(Lists.newArrayList(helper.consumerBPrev));
                assertTrue(helper.validateRecordsDelivery());
                
                helper.runProducerAndConsumersThenWait(Lists.newArrayList(helper.consumerA, helper.consumerBNext));
                helper.stopProducerAndConsumers(Lists.newArrayList(helper.consumerBNext, helper.consumerA));
                assertTrue(helper.validateRecordsDelivery());
                verifyLeasesType(helper.prevKCLAppConfig.buildDynamoDbClient(),
                        KCLAppConfig.INTEGRATION_TEST_RESOURCE_PREFIX + helper.testName,
                        prevMode, nextMode);

                helper.runProducerAndConsumersThenWait(Lists.newArrayList(helper.consumerA));
                helper.stopProducerAndConsumers(Lists.newArrayList(helper.consumerA));
                assertFalse(helper.validateRecordsDelivery());
                assertTrue(getLeaseCountOwnedBy(helper.prevKCLAppConfig.buildDynamoDbClient(),
                        KCLAppConfig.INTEGRATION_TEST_RESOURCE_PREFIX + helper.testName, "B") > 0);
            } finally {
                // Clean up
                helper.cleanup();
            }
        }
    }

    private static void verifyLeasesType(DynamoDbClient ddbClient, String tableName, KCLMode prevMode, KCLMode nextMode)
            throws Exception {
        ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).build();
        ScanResponse scanResponse = ddbClient.scan(scanRequest);
        for (Map<String, AttributeValue> item : scanResponse.items()) {
            String leaseOwner = item.get("leaseOwner").s();
            if (leaseOwner.endsWith("A")) {
                assertTrue(verifyLeaseType(item, prevMode));
            } else if (leaseOwner.endsWith("B")) {
                assertTrue(verifyLeaseType(item, nextMode));
            } else {
                throw new Exception("A stale lease owned by " + leaseOwner + " found after running the scenario.");
            }
        }
    }

    private static Boolean verifyLeaseType(Map<String, AttributeValue> lease, KCLMode mode) throws Exception {
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

    private static int getLeaseCountOwnedBy(DynamoDbClient ddbClient, String tableName, String workerIdSuffix) {
        ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).build();
        ScanResponse scanResponse = ddbClient.scan(scanRequest);
        int cnt = 0;
        for (Map<String, AttributeValue> item : scanResponse.items()) {
            String leaseOwner = item.get("leaseOwner").s();
            if (leaseOwner.endsWith(workerIdSuffix)) {
                ++ cnt;
            }
        }
        return cnt;
    }

    private static class ConsumerModeUpdateIntegrationTestHelper {

        KCLMode prevMode;
        KCLMode nextMode;

        String testName;
        String streamName;

        RecordValidatorQueue recordValidator;

        KCLAppConfig prevKCLAppConfig;
        KCLAppConfig nextKCLAppConfig;

        ProducerApplication producer;
        ConsumerApplication consumerA;
        ConsumerApplication consumerBPrev;
        ConsumerApplication consumerBNext;

        int sleepMinutes;

        public ConsumerModeUpdateIntegrationTestHelper(KCLMode prevMode, KCLMode nextMode)
                throws Exception {
            this.prevMode = prevMode;
            this.nextMode = nextMode;
            initialize();
        }

        public void runProducerAndConsumersThenWait(List<ConsumerApplication> consumers) throws Exception {
            this.producer.run();
            for (ConsumerApplication consumer : consumers) {
                consumer.run();
            }
            Thread.sleep(TimeUnit.MINUTES.toMillis(sleepMinutes));
        }

        public void stopProducerAndConsumers(List<ConsumerApplication> consumers) throws Exception {
            this.producer.stop();
            // Wait a few seconds for the last few records to be processed
            Thread.sleep(TimeUnit.SECONDS.toMillis(10));
            for (ConsumerApplication consumer : consumers) {
                consumer.stop();
            }
        }

        public void cleanup() throws Exception {
            this.producer.stop(true);
            this.consumerA.stop(true);
            this.consumerBPrev.stop(true);
            this.consumerBNext.stop(true);
        }

        public Boolean validateRecordsDelivery() {
            return this.recordValidator.validateRecordsDelivery(this.producer.successfulPutRecords);
        }

        private void initialize() throws Exception {
            final UUID uniqueId = UUID.randomUUID();
            this.testName = "ModeUpdateFrom" + this.prevMode + "to" + this.nextMode + "Test_" + uniqueId;
            this.streamName = testName;

            this.recordValidator = new RecordValidatorQueue();
            this.prevKCLAppConfig = getKCLAppConfig(this.prevMode, this.testName, this.streamName,
                    this.recordValidator);
            this.nextKCLAppConfig = getKCLAppConfig(this.nextMode, this.testName, this.streamName,
                    this.recordValidator);

            this.producer = new ProducerApplication(prevKCLAppConfig);
            this.consumerA = new ConsumerApplication(prevKCLAppConfig, "A");
            this.consumerBPrev = new ConsumerApplication(prevKCLAppConfig, "B");
            this.consumerBNext = new ConsumerApplication(nextKCLAppConfig, "B");

            // Sleep to allow the producer/consumer to run and then end the test case.
            // If non-reshard sleep 1 minutes, else sleep 4 minutes per scale.
            this.sleepMinutes = (prevKCLAppConfig.getReshardFactorList() == null) ? 3
                    : (4 * prevKCLAppConfig.getReshardFactorList().size());
        }

        private KCLAppConfig getKCLAppConfig(KCLMode mode, String testName, String streamName,
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

        private KCLAppConfig getSingleStreamConfig(String testName, String streamName,
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

        private KCLAppConfig getSingleStreamCompatibleConfig(String testName, String streamName,
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

        private KCLAppConfig getSingleStreamUpgradeConfig(String testName, String streamName,
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

        private KCLAppConfig getMultiStreamConfig(String testName, String streamName,
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
}
