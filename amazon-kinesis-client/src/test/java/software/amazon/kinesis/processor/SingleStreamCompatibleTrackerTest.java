package software.amazon.kinesis.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.processor.StreamTracker.StreamProcessingMode;

public class SingleStreamCompatibleTrackerTest {
    @Test
    public void testInstantiation() {
        String streamIdentifierSer = "123456789012:TestStream:12345";
        StreamIdentifier streamIdentifier = StreamIdentifier.multiStreamInstance(streamIdentifierSer);
        StreamConfig streamConfig = new StreamConfig(streamIdentifier,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));
        SingleStreamCompatibleTracker singleStreamCompatibleTracker = new SingleStreamCompatibleTracker(streamIdentifier, streamConfig);
        assertEquals(false, singleStreamCompatibleTracker.isMultiStream());
        assertEquals(StreamProcessingMode.SINGLE_STREAM_COMPATIBLE_MODE, singleStreamCompatibleTracker.streamProcessingMode());
    }

    @Test
    public void testInstantiationFailsForSingleStreamIdentifier() {
        StreamIdentifier streamIdentifier = StreamIdentifier.singleStreamInstance("TestStream");
        StreamConfig streamConfig = new StreamConfig(streamIdentifier,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));
        assertThrows(IllegalArgumentException.class, () -> new SingleStreamCompatibleTracker(streamIdentifier, streamConfig));
    }
}
