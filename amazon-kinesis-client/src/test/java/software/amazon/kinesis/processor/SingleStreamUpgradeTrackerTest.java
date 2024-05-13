package software.amazon.kinesis.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.processor.StreamTracker.StreamProcessingMode;

public class SingleStreamUpgradeTrackerTest {
    @Test
    public void testInstantiation() {
        String streamIdentifierSer = "123456789012:TestStream:12345";
        StreamIdentifier streamIdentifier = StreamIdentifier.multiStreamInstance(streamIdentifierSer);
        StreamConfig streamConfig = new StreamConfig(streamIdentifier, 
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));
        SingleStreamUpgradeTracker singleStreamUpgradeTracker = new SingleStreamUpgradeTracker(streamIdentifier, streamConfig);
        assertEquals(false, singleStreamUpgradeTracker.isMultiStream());
        assertEquals(StreamProcessingMode.SINGLE_STREAM_UPGRADE_MODE, singleStreamUpgradeTracker.streamProcessingMode());
    }

    @Test
    public void testInstantiationFailsForSingleStreamIdentifier() {
        StreamIdentifier streamIdentifier = StreamIdentifier.singleStreamInstance("TestStream");
        StreamConfig streamConfig = new StreamConfig(streamIdentifier, 
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));
        assertThrows(IllegalArgumentException.class, () -> new SingleStreamUpgradeTracker(streamIdentifier, streamConfig));        
    }
}
