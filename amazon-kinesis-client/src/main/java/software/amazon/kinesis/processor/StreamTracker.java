/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.kinesis.processor;

import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;

import java.util.List;

/**
 * Interface for stream trackers.
 * KCL will periodically probe this interface to learn about the new and old streams.
 */
public interface StreamTracker {

    /**
     * Default position to begin consuming records from a Kinesis stream.
     *
     * @see #orphanedStreamInitialPositionInStream()
     */
    InitialPositionInStreamExtended DEFAULT_POSITION_IN_STREAM =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);

    /**
     * Returns the list of stream config, to be processed by the current application.
     * <b>Note that the streams list CAN be changed during the application runtime.</b>
     * This method will be called periodically by the KCL to learn about the change in streams to process.
     *
     * @return List of StreamConfig
     */
    List<StreamConfig> streamConfigList();

    /**
     * Strategy to delete leases of old streams in the lease table.
     * <b>Note that the strategy CANNOT be changed during the application runtime.</b>
     *
     * @return StreamsLeasesDeletionStrategy
     */
    FormerStreamsLeasesDeletionStrategy formerStreamsLeasesDeletionStrategy();

    /**
     * The position for getting records from an "orphaned" stream that is in the lease table but not tracked
     * Default assumes that the stream no longer need to be tracked, so use LATEST for faster shard end.
     *
     * <p>Default value: {@link InitialPositionInStream#LATEST}</p>
     */
    default InitialPositionInStreamExtended orphanedStreamInitialPositionInStream() {
        return DEFAULT_POSITION_IN_STREAM;
    }

    /**
     * Returns a new {@link StreamConfig} for the provided stream identifier.
     *
     * @param streamIdentifier stream for which to create a new config
     */
    default StreamConfig createStreamConfig(StreamIdentifier streamIdentifier) {
        return new StreamConfig(streamIdentifier, orphanedStreamInitialPositionInStream());
    }

    /**
     * Returns true if this application should accommodate the consumption of
     * more than one Kinesis stream.
     * <p>
     * <b>This method must be consistent.</b> Varying the returned value will
     * have indeterminate, and likely problematic, effects on stream processing.
     * </p>
     */
    boolean isMultiStream();

    public enum StreamProcessingMode {
        /**
         * KCL application is processing a single stream.
         * All leases belong to the one stream being processed which is
         * provided to the KCL configuration.
         * Therefore KCL uses an efficient lease structure by not persisting
         * stream identifier into DDB Lease table.
         */
        SINGLE_STREAM_MODE,

        /**
         * This is essentially same as SINGLE_STREAM_MODE, however
         * is the phase 1 change to make to smoothly upgrade to 
         * MULTI_STREAM_MODE. KCL application
         * in this mode will be compatible with both lease structures
         * used by the SINGLE_STREAM_MODE and the MULTI_STREAM_MODE
         * operation
         */
        SINGLE_STREAM_COMPATIBLE_MODE,
        
        /**
         * This is also essentially same as SINGLE_STREAM_MODE where KCL
         * processes a single stream for the application. However this
         * wold be the phase 2 change to make to smoothly upgrade to
         */
        SINGLE_STREAM_UPGRADE_MODE,
        
        /**
         * KCL application is processing multiple streams, therefore
         * KCL uses a detailed lease structure to store in DDB to coordinate
         * stream consumption which can identify which lease belongs
         * to which stream, by persisting the stream identifier for each
         * lease in DDB.
         */
        MULTI_STREAM_MODE
    }
    
    /**
     * Returns the stream processing mode in which the application
     * will run. This will determine how the application will handle
     * the different lease record formats.
     * @return StreamProcessingMode
     */
    default StreamProcessingMode streamProcessingMode() {
        if (isMultiStream()) {
            return StreamProcessingMode.MULTI_STREAM_MODE;
        }
        
        return StreamProcessingMode.SINGLE_STREAM_MODE;
    }

}
