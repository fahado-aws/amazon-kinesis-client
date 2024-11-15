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

package software.amazon.kinesis.leases;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import software.amazon.kinesis.common.HashKeyRangeForLease;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import org.apache.commons.lang3.Validate;

import static com.google.common.base.Verify.verifyNotNull;

import java.util.Set;
import java.util.UUID;

@Setter
@NoArgsConstructor
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode(callSuper = true)
public class MultiStreamLease extends Lease {

    @NonNull private String streamIdentifier;
    @NonNull private String shardId;

    public MultiStreamLease(MultiStreamLease other) {
        super(other);
        streamIdentifier(other.streamIdentifier);
        shardId(other.shardId);
    }

    /**
     * Constructor to instantiate a MultiStreamLease from a single stream Lease
     * @param lease lease in the single stream lease format
     * @param leaseKey new lease key for the MultiStreamLease
     * @param streamIdentifier streamIdentifier field that is only used in a MultiStreamLease
     * @param shardId shardId field that is only used in a MultiStreamLease
     */
    public MultiStreamLease(Lease lease, String leaseKey, String streamIdentifier, String shardId) {
        super(leaseKey, lease.leaseOwner(), lease.leaseCounter(), lease.concurrencyToken(),
                lease.lastCounterIncrementNanos(), lease.checkpoint(), lease.pendingCheckpoint(),
                lease.ownerSwitchesSinceCheckpoint(), lease.parentShardIds(), lease.childShardIds(),
                lease.pendingCheckpointState(), lease.hashKeyRangeForLease());
        streamIdentifier(streamIdentifier);
        shardId(shardId);
    }

    /**
     * Constructor will all required parameters
     * @param leaseKey
     * @param leaseOwner
     * @param leaseCounter
     * @param concurrencyToken
     * @param lastCounterIncrementNanos
     * @param checkpoint
     * @param pendingCheckpoint
     * @param ownerSwitchesSinceCheckpoint
     * @param parentShardIds
     * @param childShardIds
     * @param pendingCheckpointState
     * @param hashKeyRangeForLease
     * @param streamIdentifier
     * @param shardId
     */
    public MultiStreamLease(String leaseKey, String leaseOwner, Long leaseCounter,
        UUID concurrencyToken, Long lastCounterIncrementNanos,
        ExtendedSequenceNumber checkpoint, ExtendedSequenceNumber pendingCheckpoint,
        Long ownerSwitchesSinceCheckpoint, Set<String> parentShardIds, Set<String> childShardIds,
        byte[] pendingCheckpointState, HashKeyRangeForLease hashKeyRangeForLease,
        String streamIdentifier, String shardId) {
        super(leaseKey, leaseOwner, leaseCounter, concurrencyToken,
                lastCounterIncrementNanos, checkpoint, pendingCheckpoint,
                ownerSwitchesSinceCheckpoint, parentShardIds, childShardIds,
                pendingCheckpointState, hashKeyRangeForLease);
        streamIdentifier(streamIdentifier);
        shardId(shardId);
    }

    @Override
    public void update(Lease other) {
        MultiStreamLease casted = validateAndCast(other);
        super.update(casted);
        streamIdentifier(casted.streamIdentifier);
        shardId(casted.shardId);
    }

    public static String getLeaseKey(String streamIdentifier, String shardId) {
        verifyNotNull(streamIdentifier, "streamIdentifier should not be null");
        verifyNotNull(shardId, "shardId should not be null");
        return streamIdentifier + ":" + shardId;
    }

    /**
     * Returns a deep copy of this object. Type-unsafe - there aren't good mechanisms for copy-constructing generics.
     *
     * @return A deep copy of this object.
     */
    @Override
    public MultiStreamLease copy() {
        return new MultiStreamLease(this);
    }

    /**
     * Validate and cast the lease to MultiStream lease
     * @param lease
     * @return MultiStreamLease
     */
    public static MultiStreamLease validateAndCast(Lease lease) {
        Validate.isInstanceOf(MultiStreamLease.class, lease);
        return (MultiStreamLease) lease;
    }

}
