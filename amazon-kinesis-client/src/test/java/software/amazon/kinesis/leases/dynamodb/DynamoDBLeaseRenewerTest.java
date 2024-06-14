/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
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
package software.amazon.kinesis.leases.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.common.HashKeyRangeForLease;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.MultiStreamLease;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.processor.StreamTracker.StreamProcessingMode;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBLeaseRenewerTest {
    private static final String WORKER_IDENTIFIER = "WorkerId";
    private static final long LEASE_DURATION_MILLIS = 10000;
    private DynamoDBLeaseRenewer renewer;
    private List<Lease> leasesToRenew;

    @Mock
    private LeaseRefresher leaseRefresher;
    @Mock
    private MetricsFactory metricsFactory;
    @Mock
    private MetricsScope metricsScope;

    private static Lease newLease(String leaseKey) {
        return new Lease(leaseKey, WORKER_IDENTIFIER, 0L, UUID.randomUUID(), System.nanoTime(), null, null, null,
                new HashSet<>(), new HashSet<>(), null, HashKeyRangeForLease.deserialize("1", "2"));
    }

    private static Lease newMultiStreamLease(String leaseKey) {
        return new MultiStreamLease(leaseKey, WORKER_IDENTIFIER, 0L, UUID.randomUUID(), System.nanoTime(), null, null, null,
                new HashSet<>(), new HashSet<>(), null, HashKeyRangeForLease.deserialize("1", "2"),
                "stream", "shard-id");
    }

    @Before
    public void before() {
        leasesToRenew = null;
        renewer = new DynamoDBLeaseRenewer(leaseRefresher, WORKER_IDENTIFIER, LEASE_DURATION_MILLIS,
                Executors.newCachedThreadPool(), new NullMetricsFactory());
    }

    @After
    public void after() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        if (leasesToRenew == null) {
            return;
        }
        for (Lease lease : leasesToRenew) {
            verify(leaseRefresher, times(1)).renewLease(eq(lease));
        }
    }

    @Test
    public void testLeaseRenewerHoldsGoodLeases()
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        /*
         * Prepare leases to be renewed
         * 2 Good
         */
        Lease lease1 = newLease("1");
        Lease lease2 = newLease("2");
        leasesToRenew = Arrays.asList(lease1, lease2);
        renewer.addLeasesToRenew(leasesToRenew);

        doReturn(true).when(leaseRefresher).renewLease(lease1);
        doReturn(true).when(leaseRefresher).renewLease(lease2);

        renewer.renewLeases();

        assertEquals(2, renewer.getCurrentlyHeldLeases().size());
    }

    @Test
    public void testLeaseRenewerDoesNotRenewExpiredLease() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        String leaseKey = "expiredLease";
        long initialCounterIncrementNanos = 5L; // "expired" time.
        Lease lease1 = newLease(leaseKey);
        lease1.lastCounterIncrementNanos(initialCounterIncrementNanos);

        leasesToRenew = new ArrayList<>();
        leasesToRenew.add(lease1);
        doReturn(true).when(leaseRefresher).renewLease(lease1);
        renewer.addLeasesToRenew(leasesToRenew);

        assertTrue(lease1.isExpired(1, System.nanoTime()));
        assertNull(renewer.getCurrentlyHeldLease(leaseKey));
        renewer.renewLeases();
        // Don't renew lease(s) with same key if getCurrentlyHeldLease returned null previously
        assertNull(renewer.getCurrentlyHeldLease(leaseKey));
        assertFalse(renewer.getCurrentlyHeldLeases().containsKey(leaseKey));

        // Clear the list to avoid triggering expectation mismatch in after().
        leasesToRenew.clear();
    }

    @Test
    public void testReplacesLeaseInStreamUpgradeMode()
        throws Exception {
        // given
        String streamIdentifierSer = "123456789012:TestStream:12345";
        StreamIdentifier streamIdentifier = StreamIdentifier.multiStreamInstance(streamIdentifierSer);
        StreamConfig streamConfig = new StreamConfig(streamIdentifier,
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));
        Map<StreamIdentifier, StreamConfig> streamConfigMap = new HashMap<>();
        streamConfigMap.put(streamIdentifier, streamConfig);
        when(metricsFactory.createMetrics()).thenReturn(metricsScope);
        renewer = new DynamoDBLeaseRenewer(leaseRefresher, WORKER_IDENTIFIER, LEASE_DURATION_MILLIS,
            Executors.newCachedThreadPool(), metricsFactory, StreamProcessingMode.SINGLE_STREAM_UPGRADE_MODE,
            streamConfigMap);
        /*
         * Prepare leases to be renewed
         * 2 Good
         */
        Lease lease1 = newLease("1");
        Lease lease2 = newMultiStreamLease("2");
        List<Lease> leases = Arrays.asList(lease1, lease2);
        Lease expectedMultiStreamLease = renewer.convertToMultiStreamLease(lease1);
        doReturn(true).when(leaseRefresher).renewLease(expectedMultiStreamLease);
        doReturn(true).when(leaseRefresher).renewLease(lease2);

        // when
        renewer.addLeasesToRenew(leases);

        renewer.renewLeases();

        // then
        Map<String, Lease> currentLeases = renewer.getCurrentlyHeldLeases();
        assertEquals(2, currentLeases.size());
        verify(leaseRefresher, times(1)).replaceLease(eq(lease1), eq(expectedMultiStreamLease));
        assertEquals(expectedMultiStreamLease, currentLeases.get(expectedMultiStreamLease.leaseKey()));
        assertEquals(lease2, currentLeases.get(lease2.leaseKey()));
        verify(metricsScope, times(1)).addDimension("Operation", "UpgradeLease");
        verify(metricsScope, times(1)).addData("Success", 1, StandardUnit.COUNT, MetricsLevel.DETAILED);
        verify(metricsScope, times(1)).addData(eq("Time"), anyDouble(), eq(StandardUnit.MILLISECONDS), eq(MetricsLevel.DETAILED));
    }

    @Test
    public void testReplacesLeaseInStreamUpgradeModeWhenInitialized()
        throws Exception {
        // given
        String streamIdentifierSer = "123456789012:TestStream:12345";
        StreamIdentifier streamIdentifier = StreamIdentifier.multiStreamInstance(streamIdentifierSer);
        StreamConfig streamConfig = new StreamConfig(streamIdentifier,
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));
        Map<StreamIdentifier, StreamConfig> streamConfigMap = new HashMap<>();
        streamConfigMap.put(streamIdentifier, streamConfig);
        when(metricsFactory.createMetrics()).thenReturn(metricsScope);
        renewer = new DynamoDBLeaseRenewer(leaseRefresher, WORKER_IDENTIFIER, LEASE_DURATION_MILLIS,
            Executors.newCachedThreadPool(), metricsFactory, StreamProcessingMode.SINGLE_STREAM_UPGRADE_MODE,
            streamConfigMap);
        /*
         * Prepare leases to be renewed
         * 2 Good
         */
        Lease lease1 = newLease("1");
        Lease lease2 = newMultiStreamLease("2");
        List<Lease> leases = Arrays.asList(lease1, lease2);
        Lease expectedMultiStreamLease = renewer.convertToMultiStreamLease(lease1);
        doReturn(true).when(leaseRefresher).renewLease(any(Lease.class));
        doReturn(leases).when(leaseRefresher).listLeases();

        // when
        renewer.initialize();

        // then
        Map<String, Lease> currentLeases = renewer.getCurrentlyHeldLeases();
        assertEquals(2, currentLeases.size());
        verify(leaseRefresher, times(1)).replaceLease(eq(lease1), eq(expectedMultiStreamLease));
        assertEquals(expectedMultiStreamLease, currentLeases.get(expectedMultiStreamLease.leaseKey()));
        assertEquals(lease2, currentLeases.get(lease2.leaseKey()));
        verify(metricsScope, times(1)).addDimension("Operation", "UpgradeLease");
        verify(metricsScope, times(1)).addData("Success", 1, StandardUnit.COUNT, MetricsLevel.DETAILED);
        verify(metricsScope, times(1)).addData(eq("Time"), anyDouble(), eq(StandardUnit.MILLISECONDS), eq(MetricsLevel.DETAILED));
    }

    @Test
    public void testInstantiationFailsWhenMoreThanOneStreamIsProvidedInSingleStreamUpgradeMode()
        throws Exception {
        // given
        String streamIdentifierSer1 = "123456789012:TestStream1:12345";
        StreamIdentifier streamIdentifier1 = StreamIdentifier.multiStreamInstance(streamIdentifierSer1);
        StreamConfig streamConfig1 = new StreamConfig(streamIdentifier1,
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));
        String streamIdentifierSer2 = "123456789012:TestStream2:12345";
            StreamIdentifier streamIdentifier2 = StreamIdentifier.multiStreamInstance(streamIdentifierSer2);
            StreamConfig streamConfig2 = new StreamConfig(streamIdentifier2,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));
        Map<StreamIdentifier, StreamConfig> streamConfigMap = new HashMap<>();
        streamConfigMap.put(streamIdentifier1, streamConfig1);
        streamConfigMap.put(streamIdentifier2, streamConfig2);

        // when & then
        assertThrows(IllegalArgumentException.class,
            () -> new DynamoDBLeaseRenewer(leaseRefresher, WORKER_IDENTIFIER, LEASE_DURATION_MILLIS,
            Executors.newCachedThreadPool(), new NullMetricsFactory(), StreamProcessingMode.SINGLE_STREAM_UPGRADE_MODE,
            streamConfigMap));
    }

    @Test
    public void testConvertToMultiStreamLease() {
        // given
        String streamIdentifierSer = "123456789012:TestStream:12345";
        String shardId = "1";
        StreamIdentifier streamIdentifier = StreamIdentifier.multiStreamInstance(streamIdentifierSer);
        StreamConfig streamConfig = new StreamConfig(streamIdentifier,
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));
        Map<StreamIdentifier, StreamConfig> streamConfigMap = new HashMap<>();
        streamConfigMap.put(streamIdentifier, streamConfig);
        renewer = new DynamoDBLeaseRenewer(leaseRefresher, WORKER_IDENTIFIER, LEASE_DURATION_MILLIS,
            Executors.newCachedThreadPool(), new NullMetricsFactory(), StreamProcessingMode.SINGLE_STREAM_UPGRADE_MODE,
            streamConfigMap);
        Lease lease = newLease(shardId);
        String expectedLeaseKey = MultiStreamLease.getLeaseKey(streamIdentifierSer, shardId);

        // when
        MultiStreamLease multiStreamLease = renewer.convertToMultiStreamLease(lease);

        // then
        assertEquals(expectedLeaseKey, multiStreamLease.leaseKey());
        assertEquals(shardId, multiStreamLease.shardId());
        assertEquals(streamIdentifierSer, multiStreamLease.streamIdentifier());
        assertEquals(lease.leaseOwner(), multiStreamLease.leaseOwner());
        assertEquals(lease.leaseCounter(), multiStreamLease.leaseCounter());
        assertEquals(lease.concurrencyToken(), multiStreamLease.concurrencyToken());
        assertEquals(lease.lastCounterIncrementNanos(), multiStreamLease.lastCounterIncrementNanos());
        assertEquals(lease.checkpoint(), multiStreamLease.checkpoint());
        assertEquals(lease.pendingCheckpoint(), multiStreamLease.pendingCheckpoint());
        assertEquals(lease.ownerSwitchesSinceCheckpoint(), multiStreamLease.ownerSwitchesSinceCheckpoint());
        assertEquals(lease.parentShardIds(), multiStreamLease.parentShardIds());
        assertEquals(lease.childShardIds(), multiStreamLease.childShardIds());
        assertEquals(lease.pendingCheckpointState(), multiStreamLease.pendingCheckpointState());
        assertEquals(lease.hashKeyRangeForLease(), multiStreamLease.hashKeyRangeForLease());
    }
    @Test
    public void testInitializeFailsWhenLeaseReplacementFails()
        throws Exception {
        // given
        String streamIdentifierSer = "123456789012:TestStream:12345";
        StreamIdentifier streamIdentifier = StreamIdentifier.multiStreamInstance(streamIdentifierSer);
        StreamConfig streamConfig = new StreamConfig(streamIdentifier,
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));
        Map<StreamIdentifier, StreamConfig> streamConfigMap = new HashMap<>();
        streamConfigMap.put(streamIdentifier, streamConfig);
        when(metricsFactory.createMetrics()).thenReturn(metricsScope);
        renewer = new DynamoDBLeaseRenewer(leaseRefresher, WORKER_IDENTIFIER, LEASE_DURATION_MILLIS,
            Executors.newCachedThreadPool(), metricsFactory, StreamProcessingMode.SINGLE_STREAM_UPGRADE_MODE,
            streamConfigMap);

        Lease lease1 = newLease("1");
        Lease lease2 = newMultiStreamLease("2");
        List<Lease> leases = Arrays.asList(lease1, lease2);
        Lease expectedMultiStreamLease = renewer.convertToMultiStreamLease(lease1);
        doThrow(InvalidStateException.class).when(leaseRefresher).replaceLease(eq(lease1), eq(expectedMultiStreamLease));
        doReturn(true).when(leaseRefresher).renewLease(any(Lease.class));
        doReturn(leases).when(leaseRefresher).listLeases();

        // when
        assertThrows(InvalidStateException.class, () -> renewer.initialize());

        // then
        Map<String, Lease> currentLeases = renewer.getCurrentlyHeldLeases();
        assertEquals(0, currentLeases.size());
        verify(leaseRefresher, times(1)).replaceLease(eq(lease1), eq(expectedMultiStreamLease));
        verify(metricsScope, times(1)).addDimension("Operation", "UpgradeLease");
        verify(metricsScope, times(1)).addData("Success", 0, StandardUnit.COUNT, MetricsLevel.DETAILED);
        verify(metricsScope, times(1)).addData(eq("Time"), anyDouble(), eq(StandardUnit.MILLISECONDS), eq(MetricsLevel.DETAILED));
    }
}
