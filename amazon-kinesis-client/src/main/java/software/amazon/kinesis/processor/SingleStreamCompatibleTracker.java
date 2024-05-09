package software.amazon.kinesis.processor;

import lombok.NonNull;
import software.amazon.awssdk.utils.Validate;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;

/**
 * StreamTracker implementation for consuming a single Kinesis stream in 
 * SINGLE_STREAM_COMPATIBLE_MODE.
 */
public class SingleStreamCompatibleTracker extends SingleStreamTracker {
    public SingleStreamCompatibleTracker(@NonNull StreamIdentifier streamIdentifier, @NonNull StreamConfig streamConfig) {
        super(streamIdentifier, streamConfig);
        Validate.isTrue(streamIdentifier.accountIdOptional().isPresent(), 
            "Account Id must be included in the stream identifier");
        Validate.isTrue(streamIdentifier.streamCreationEpochOptional().isPresent(), 
            "Stream creation epoch must be included in the stream identifier");
    }

    @Override
    public StreamProcessingMode streamProcessingMode() {
        return StreamProcessingMode.SINGLE_STREAM_COMPATIBLE_MODE;
    }
    
}
