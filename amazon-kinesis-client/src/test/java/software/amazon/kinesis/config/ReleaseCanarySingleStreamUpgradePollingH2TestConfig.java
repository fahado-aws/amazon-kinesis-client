package software.amazon.kinesis.config;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.http.Protocol;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.processor.SingleStreamUpgradeTracker;
import software.amazon.kinesis.processor.StreamTracker;

public class ReleaseCanarySingleStreamUpgradePollingH2TestConfig extends KCLAppConfig {
    private final UUID uniqueId = UUID.randomUUID();

    private final String applicationName = "PollingH2Test";
    private final String streamName = "2XPollingH2TestStream_" + uniqueId;

    @Override
    public String getTestName() {
        return applicationName;
    }

    @Override
    public List<Arn> getStreamArns() {
        return Collections.singletonList(buildStreamArn(streamName));
    }

    @Override
    public StreamTracker getStreamTracker(Map<Arn, Arn> streamToConsumerArnsMap) {
        Arn streamArn = getStreamArns().get(0);
        StreamIdentifier streamIdentifier = StreamIdentifier.multiStreamInstance(streamArn, getCreationEpoch(streamArn));
        InitialPositionInStreamExtended initialPosition = InitialPositionInStreamExtended
                 .newInitialPosition(getInitialPosition());
        StreamConfig streamConfig = new StreamConfig(streamIdentifier, initialPosition);
        return new SingleStreamUpgradeTracker(streamIdentifier, streamConfig);
    }

    @Override
    public Protocol getKinesisClientProtocol() {
        return Protocol.HTTP2;
    }

    @Override
    public RetrievalMode getRetrievalMode() {
        return RetrievalMode.POLLING;
    }
}
