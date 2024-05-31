package software.amazon.kinesis.application;

import static org.junit.Assume.assumeTrue;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.kinesis.checkpoint.CheckpointConfig;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.config.KCLAppConfig;
import software.amazon.kinesis.config.RetrievalMode;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.lifecycle.LifecycleConfig;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.processor.ProcessorConfig;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.utils.LeaseTableManager;
import software.amazon.kinesis.utils.StreamExistenceManager;

@Slf4j
@Getter
public class ConsumerApplication {

    private KCLAppConfig consumerConfig;
    private MetricsConfig metricsConfig;
    private RetrievalConfig retrievalConfig;
    private CheckpointConfig checkpointConfig;
    private CoordinatorConfig coordinatorConfig;
    private LeaseManagementConfig leaseManagementConfig;
    private LifecycleConfig lifecycleConfig;
    private ProcessorConfig processorConfig;
    private Scheduler scheduler;
    private ScheduledExecutorService consumerExecutor;
    private ScheduledFuture<?> consumerFuture;
    private String workerIdSuffix;
    private String workerIdentifier;

    public ConsumerApplication(KCLAppConfig consumerConfig, String workerIdSuffix) {
        this.consumerConfig = consumerConfig;
        this.workerIdSuffix = workerIdSuffix;
    }

    public void run() throws Exception {
        // Skip cross account tests if no cross account credentials are provided
        if (this.consumerConfig.isCrossAccount()) {
            assumeTrue(this.consumerConfig.getCrossAccountCredentialsProvider() != null);
        }


        final StreamExistenceManager streamExistenceManager = new StreamExistenceManager(this.consumerConfig);
        Map<Arn, Arn> streamToConsumerArnsMap = streamExistenceManager.createCrossAccountConsumerIfNecessary();

        setUpConsumerResources(streamToConsumerArnsMap);
        startConsumer();
    }

    public void stop() throws Exception {
        stop(false);
    }

    public void stop(Boolean cleanupResources) throws Exception {
        awaitConsumerFinish();
        if (cleanupResources) {
            cleanupResources();
        }
    }

    public Boolean isTerminated() {
        return this.scheduler == null || this.scheduler.shutdownComplete();
    }

    private void setUpConsumerResources(Map<Arn, Arn> streamToConsumerArnsMap) throws Exception {
        // Setup configuration of KCL (including DynamoDB and CloudWatch)
        final ConfigsBuilder configsBuilder = consumerConfig.getConfigsBuilder(streamToConsumerArnsMap, workerIdSuffix);
        this.workerIdentifier = configsBuilder.workerIdentifier();

        // For polling mode in both CAA and non CAA, set retrievalSpecificConfig to use PollingConfig
        // For SingleStreamMode EFO CAA, must set the retrieval config to specify the consumerArn in FanoutConfig
        // For MultiStream EFO CAA, the consumerArn can be set in StreamConfig
        if (this.consumerConfig.getRetrievalMode().equals(RetrievalMode.POLLING)) {
            this.retrievalConfig = consumerConfig.getRetrievalConfig(configsBuilder, null);
        } else if (this.consumerConfig.isCrossAccount()) {
            this.retrievalConfig = this.consumerConfig.getRetrievalConfig(configsBuilder,
                    streamToConsumerArnsMap);
        } else {
            this.retrievalConfig = configsBuilder.retrievalConfig();
        }

        this.checkpointConfig = configsBuilder.checkpointConfig();
        this.coordinatorConfig = configsBuilder.coordinatorConfig();
        this.leaseManagementConfig = configsBuilder.leaseManagementConfig()
                .initialPositionInStream(
                    InitialPositionInStreamExtended.newInitialPosition(this.consumerConfig.getInitialPosition())
                )
                .initialLeaseTableReadCapacity(50).initialLeaseTableWriteCapacity(50);
        this.lifecycleConfig = configsBuilder.lifecycleConfig();
        this.processorConfig = configsBuilder.processorConfig();
        this.metricsConfig = configsBuilder.metricsConfig();

        // Create Scheduler
        this.scheduler = new Scheduler(
                this.checkpointConfig,
                this.coordinatorConfig,
                this.leaseManagementConfig,
                this.lifecycleConfig,
                this.metricsConfig,
                this.processorConfig,
                this.retrievalConfig
        );
    }

    private void startConsumer() {
        // Start record processing of dummy data
        this.consumerExecutor = Executors.newSingleThreadScheduledExecutor();
        this.consumerFuture = consumerExecutor.schedule(scheduler, 0, TimeUnit.SECONDS);
    }

    private void awaitConsumerFinish() throws Exception {
        if (this.scheduler != null) {
            Future<Boolean> gracefulShutdownFuture = this.scheduler.startGracefulShutdown();
            log.info("Waiting up to 20 seconds for shutdown to complete.");
            try {
                gracefulShutdownFuture.get(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.info("Interrupted while waiting for graceful shutdown. Continuing.");
            } catch (ExecutionException | TimeoutException e) {
                scheduler.shutdown();
            }
        } else {
            log.info("Scheduler is not found. Shutting down consumerFuture and consumerExecutor.");
            if (this.consumerFuture != null) {
                this.consumerFuture.cancel(false);
            }
            if (this.consumerExecutor != null) {
                this.consumerExecutor.shutdown();
            }
        }
        log.info("Completed, shutting down now.");
    }

    private void cleanupResources() {
        log.info("---------Start deleting lease table.---------");
        try {
            DynamoDbAsyncClient dynamoClient = consumerConfig.buildAsyncDynamoDbClient();
            LeaseTableManager leaseTableManager = new LeaseTableManager(dynamoClient);
            leaseTableManager.deleteResource(this.consumerConfig.getApplicationName());
        } catch (Exception e) {
            log.warn("Failed to delete lease table: {}, {}", this.consumerConfig.getApplicationName(), e.getMessage());
        }
        log.info("---------Finished deleting resources.---------");
    }
}
