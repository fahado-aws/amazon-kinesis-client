package software.amazon.kinesis.lifecycle;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;

import lombok.AllArgsConstructor;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
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

/*
 * Integration test verifying the end-to-end behaviour during KCL mode upgrade.
 * Each test runs/stops multiple consumers in different mode to simulate upgrade or rollback, 
 * then verify the process of records and the change of lease type.
 */
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
                // Run consumer A and B in the prev mode.
                helper.runProducerAndConsumersThenWait(Lists.newArrayList(helper.consumerA, helper.consumerBPrev));
                helper.stopProducerAndConsumers(Lists.newArrayList(helper.consumerBPrev));
                assertTrue(helper.validateRecordsDelivery());

                // Update consumer B to the next mode. All records should be processed and each
                // lease should be in correct format.
                helper.runProducerAndConsumersThenWait(Lists.newArrayList(helper.consumerBNext));
                helper.stopProducerAndConsumers(Lists.newArrayList(helper.consumerBNext, helper.consumerA));
                assertTrue(helper.validateRecordsDelivery());
                verifyLeasesType(helper.prevKCLAppConfig.buildAsyncDynamoDbClient(),
                        KCLAppConfig.INTEGRATION_TEST_RESOURCE_PREFIX + helper.testName,
                        helper.consumerA.getWorkerIdentifier(), prevMode, helper.consumerBNext.getWorkerIdentifier(),
                        nextMode);

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
                // Run consumer A and B in the prev mode.
                helper.runProducerAndConsumersThenWait(Lists.newArrayList(helper.consumerA, helper.consumerBPrev));
                helper.stopProducerAndConsumers(Lists.newArrayList(helper.consumerBPrev));
                assertTrue(helper.validateRecordsDelivery());

                // Update consumer B to the next mode. Consumer B should be failed to start
                // because it is unable to be initialized due to the incompatible
                // leases. However, all records would be consumed by consumer A and no lease
                // would be acquired by consumer B.
                helper.runProducerAndConsumersThenWait(Lists.newArrayList(helper.consumerBNext));
                assertTrue(helper.consumerBNext.isTerminated());
                helper.stopProducerAndConsumers(Lists.newArrayList(helper.consumerBNext, helper.consumerA));
                assertTrue(helper.validateRecordsDelivery());
                assertEquals(0, getLeaseCountOwnedBy(helper.prevKCLAppConfig.buildAsyncDynamoDbClient(),
                        KCLAppConfig.INTEGRATION_TEST_RESOURCE_PREFIX + helper.testName,
                        helper.consumerBNext.getWorkerIdentifier()));

            } finally {
                // Clean up
                helper.cleanup();
            }
        }
    }

    public static class FailurePathNonParameterizedIntegrationTest {

        @Test
        public void consumerModeUpdateFromSingleStreamToSingleStreamUpgradeTest() throws Exception {
            KCLMode prevMode = KCLMode.SingleStream;
            KCLMode nextMode = KCLMode.SingleStreamUpgrade;
            ConsumerModeUpdateIntegrationTestHelper helper = new ConsumerModeUpdateIntegrationTestHelper(prevMode,
                    nextMode);
            try {
                // Run consumer A and B in SingleStream mode.
                helper.runProducerAndConsumersThenWait(Lists.newArrayList(helper.consumerA, helper.consumerBPrev));
                helper.stopProducerAndConsumers(Lists.newArrayList(helper.consumerBPrev));
                assertTrue(helper.validateRecordsDelivery());

                // Update consumer B to the SingleStreamUpgrade mode. All records should be
                // processed and leases owned by consumer B will be upgraded to MultiStream
                // format.
                helper.runProducerAndConsumersThenWait(Lists.newArrayList(helper.consumerBNext));
                helper.stopProducerAndConsumers(Lists.newArrayList(helper.consumerBNext));
                assertTrue(helper.validateRecordsDelivery());
                verifyLeasesType(helper.prevKCLAppConfig.buildAsyncDynamoDbClient(),
                        KCLAppConfig.INTEGRATION_TEST_RESOURCE_PREFIX + helper.testName,
                        helper.consumerA.getWorkerIdentifier(), prevMode, helper.consumerBNext.getWorkerIdentifier(),
                        nextMode);

                // Run consumer A only in SingleStream mode. Consumer A consume the leases
                // already own but wouldn't acquire the MultiStream format lease previously
                // onwed by consumer B. Therefore, some events would be lost.
                helper.runProducerAndConsumersThenWait(Collections.emptyList());
                assertFalse(helper.consumerA.isTerminated());
                helper.stopProducerAndConsumers(Lists.newArrayList(helper.consumerA));
                assertFalse(helper.validateRecordsDelivery());
                assertTrue(getLeaseCountOwnedBy(helper.prevKCLAppConfig.buildAsyncDynamoDbClient(),
                        KCLAppConfig.INTEGRATION_TEST_RESOURCE_PREFIX + helper.testName,
                        helper.consumerBNext.getWorkerIdentifier()) > 0);
            } finally {
                // Clean up
                helper.cleanup();
            }
        }
    }

    private static void verifyLeasesType(DynamoDbAsyncClient ddbClient, String tableName, String consumerAIdentifier,
            KCLMode consumerAMode, String consumerBIdentifier, KCLMode consumerBMode)
            throws Exception {
        ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).build();
        ScanResponse scanResponse = ddbClient.scan(scanRequest).get(30, TimeUnit.SECONDS);
        for (Map<String, AttributeValue> item : scanResponse.items()) {
            String leaseOwner = item.get("leaseOwner").s();
            assertThat(leaseOwner, anyOf(is(consumerAIdentifier), is(consumerBIdentifier)));
            if (leaseOwner.equals(consumerAIdentifier)) {
                assertTrue(verifyLeaseType(item, consumerAMode));
            } else if (leaseOwner.equals(consumerBIdentifier)) {
                assertTrue(verifyLeaseType(item, consumerBMode));
            }
        }
    }

    private static Boolean verifyLeaseType(Map<String, AttributeValue> lease, KCLMode mode) throws Exception {
        AttributeValue streamName = lease.get("streamName");
        AttributeValue shardId = lease.get("shardId");
        AttributeValue leaseKey = lease.get("leaseKey");
        Boolean isLeaseKeySingleStreamFormat = leaseKey.s().split(":").length == 1;
        Boolean isLeaseKeyMultiStreamFormat = leaseKey.s().split(":").length == 4;
        switch (mode) {
            case SingleStream:
                return streamName == null && shardId == null && isLeaseKeySingleStreamFormat;
            case SingleStreamCompatible:
                return true;
            case SingleStreamUpgrade:
            case MultiStream:
                return streamName != null && shardId != null && isLeaseKeyMultiStreamFormat;
            default:
                throw new Exception("Unable to verify lease type with the given mode: " + mode);
        }
    }

    private static int getLeaseCountOwnedBy(DynamoDbAsyncClient ddbClient, String tableName, String workerIdentifier)
            throws InterruptedException, ExecutionException, TimeoutException {
        ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).build();
        ScanResponse scanResponse = ddbClient.scan(scanRequest).get(30, TimeUnit.SECONDS);
        int cnt = 0;
        for (Map<String, AttributeValue> item : scanResponse.items()) {
            String leaseOwner = item.get("leaseOwner").s();
            if (leaseOwner.equals(workerIdentifier)) {
                ++cnt;
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
            Boolean result = this.recordValidator.validateRecordsDelivery(this.producer.successfulPutRecords);
            this.producer.successfulPutRecords = 0;
            this.recordValidator.clear();
            return result;
        }

        private void initialize() throws Exception {
            final UUID uniqueId = UUID.randomUUID();
            this.testName = "ModeUpdateFrom" + this.prevMode + "To" + this.nextMode + "Test_" + uniqueId;
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
