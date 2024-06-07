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

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.MultiStreamLease;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.NullMetricsScope;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBLeaseTakerTest {

    private static final String WORKER_IDENTIFIER = "foo";
    private static final long LEASE_DURATION_MILLIS = 1000L;
    private static final int DEFAULT_VERY_OLD_LEASE_DURATION_MULTIPLIER = 3;
    private static final int VERY_OLD_LEASE_DURATION_MULTIPLIER = 5;
    private static final long MOCK_CURRENT_TIME = 10000000000L;

    private DynamoDBLeaseTaker dynamoDBLeaseTaker;

    @Mock
    private LeaseRefresher leaseRefresher;
    @Mock
    private MetricsFactory metricsFactory;
    @Mock
    private Callable<Long> timeProvider;
    @Mock
    private MetricsScope metricsScope;

    @Before
    public void setup() {
        this.dynamoDBLeaseTaker = new DynamoDBLeaseTaker(leaseRefresher, WORKER_IDENTIFIER, LEASE_DURATION_MILLIS, metricsFactory);
    }

    /**
     * Test method for {@link DynamoDBLeaseTaker#stringJoin(java.util.Collection, java.lang.String)}.
     */
    @Test
    public final void testStringJoin() {
        List<String> strings = new ArrayList<>();

        strings.add("foo");
        Assert.assertEquals("foo", DynamoDBLeaseTaker.stringJoin(strings, ", "));

        strings.add("bar");
        Assert.assertEquals("foo, bar", DynamoDBLeaseTaker.stringJoin(strings, ", "));
    }

    @Test
    public void test_computeLeaseCounts_noExpiredLease() throws Exception {
        final List<Lease> leases = new ImmutableList.Builder<Lease>()
                .add(createLease(null, "1"))
                .add(createLease("foo", "2"))
                .add(createLease("bar", "3"))
                .add(createLease("baz", "4"))
                .build();
        dynamoDBLeaseTaker.allLeases.putAll(
                leases.stream().collect(Collectors.toMap(Lease::leaseKey, Function.identity())));

        when(leaseRefresher.listLeases()).thenReturn(leases);
        when(metricsFactory.createMetrics()).thenReturn(new NullMetricsScope());
        when(timeProvider.call()).thenReturn(MOCK_CURRENT_TIME);

        final Map<String, Integer> actualOutput = dynamoDBLeaseTaker.computeLeaseCounts(ImmutableList.of());

        final Map<String, Integer> expectedOutput = new HashMap<>();
        expectedOutput.put(null, 1);
        expectedOutput.put("foo", 1);
        expectedOutput.put("bar", 1);
        expectedOutput.put("baz", 1);
        assertEquals(expectedOutput, actualOutput);
    }

    @Test
    public void test_computeLeaseCounts_withExpiredLease() throws Exception {
        final List<Lease> leases = new ImmutableList.Builder<Lease>()
                .add(createLease("foo", "2"))
                .add(createLease("bar", "3"))
                .add(createLease("baz", "4"))
                .build();
        dynamoDBLeaseTaker.allLeases.putAll(
                leases.stream().collect(Collectors.toMap(Lease::leaseKey, Function.identity())));

        when(leaseRefresher.listLeases()).thenReturn(leases);
        when(metricsFactory.createMetrics()).thenReturn(new NullMetricsScope());
        when(timeProvider.call()).thenReturn(MOCK_CURRENT_TIME);

        final Map<String, Integer> actualOutput = dynamoDBLeaseTaker.computeLeaseCounts(leases);

        final Map<String, Integer> expectedOutput = new HashMap<>();
        expectedOutput.put("foo", 0);
        assertEquals(expectedOutput, actualOutput);
    }

    @Test
    public void test_veryOldLeaseDurationNanosMultiplierGetsCorrectLeases() throws Exception {
        long veryOldThreshold = MOCK_CURRENT_TIME -
                (TimeUnit.MILLISECONDS.toNanos(LEASE_DURATION_MILLIS) * VERY_OLD_LEASE_DURATION_MULTIPLIER);
        DynamoDBLeaseTaker dynamoDBLeaseTakerWithCustomMultiplier =
                new DynamoDBLeaseTaker(leaseRefresher, WORKER_IDENTIFIER, LEASE_DURATION_MILLIS, metricsFactory)
                        .withVeryOldLeaseDurationNanosMultiplier(VERY_OLD_LEASE_DURATION_MULTIPLIER);
        final List<Lease> allLeases = new ImmutableList.Builder<Lease>()
                .add(createLease("foo", "2", MOCK_CURRENT_TIME))
                .add(createLease("bar", "3", veryOldThreshold - 1))
                .add(createLease("baz", "4", veryOldThreshold))
                .build();
        final List<Lease> expiredLeases = allLeases.subList(1, 3);

        dynamoDBLeaseTakerWithCustomMultiplier.allLeases.putAll(
                allLeases.stream().collect(Collectors.toMap(Lease::leaseKey, Function.identity())));
        when(leaseRefresher.listLeases()).thenReturn(allLeases);
        when(metricsFactory.createMetrics()).thenReturn(metricsScope);
        when(timeProvider.call()).thenReturn(MOCK_CURRENT_TIME);

        Set<Lease> output = dynamoDBLeaseTakerWithCustomMultiplier.computeLeasesToTake(expiredLeases, timeProvider);
        final Set<Lease> expectedOutput = new HashSet<>();
        expectedOutput.add(allLeases.get(1));
        assertEquals(expectedOutput, output);
        verify(metricsScope).addDimension("Operation", "TakeLeases");
        verify(metricsScope).addData("ExpiredLeases", 2, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        verify(metricsScope).addData("LeaseSpillover", 0, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        verify(metricsScope).addData("LeasesToTake", 0, StandardUnit.COUNT, MetricsLevel.DETAILED);
        verify(metricsScope).addData("NeededLeases", 2, StandardUnit.COUNT, MetricsLevel.DETAILED);
        verify(metricsScope).addData("NumWorkers", 1, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        verify(metricsScope).addData("TotalLeases", 3, StandardUnit.COUNT, MetricsLevel.DETAILED);
        verify(metricsScope).addData("VeryOldLeases", 1, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        verify(metricsScope).addData("NumSingleStreamLeases", 3, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        verify(metricsScope).addData("NumMultiStreamLeases", 0, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        verify(metricsScope).end();
    }

    @Test
    public void test_disableEnablePriorityLeaseAssignmentGetsCorrectLeases() throws Exception {
        long veryOldThreshold = MOCK_CURRENT_TIME -
                (TimeUnit.MILLISECONDS.toNanos(LEASE_DURATION_MILLIS) * DEFAULT_VERY_OLD_LEASE_DURATION_MULTIPLIER);
        DynamoDBLeaseTaker dynamoDBLeaseTakerWithDisabledPriorityLeaseAssignment =
                new DynamoDBLeaseTaker(leaseRefresher, WORKER_IDENTIFIER, LEASE_DURATION_MILLIS, metricsFactory)
                        .withEnablePriorityLeaseAssignment(false);
        final List<Lease> allLeases = new ArrayList<>();
        allLeases.add(createLease("bar", "2", MOCK_CURRENT_TIME));
        allLeases.add(createLease("bar", "3", MOCK_CURRENT_TIME));
        allLeases.add(createLease("bar", "4", MOCK_CURRENT_TIME));
        allLeases.add(createLease("baz", "5", veryOldThreshold - 1));
        allLeases.add(createLease("baz", "6", veryOldThreshold + 1));
        allLeases.add(createLease(null, "7"));
        final List<Lease> expiredLeases = allLeases.subList(3, 6);

        dynamoDBLeaseTakerWithDisabledPriorityLeaseAssignment.allLeases.putAll(
                allLeases.stream().collect(Collectors.toMap(Lease::leaseKey, Function.identity())));
        when(leaseRefresher.listLeases()).thenReturn(allLeases);
        when(metricsFactory.createMetrics()).thenReturn(metricsScope);
        when(timeProvider.call()).thenReturn(MOCK_CURRENT_TIME);

        Set<Lease> output = dynamoDBLeaseTakerWithDisabledPriorityLeaseAssignment.computeLeasesToTake(expiredLeases, timeProvider);
        final Set<Lease> expectedOutput = new HashSet<>();
        expectedOutput.add(createLease("baz", "5", veryOldThreshold - 1));
        expectedOutput.add(createLease("baz", "6", veryOldThreshold + 1));
        expectedOutput.add(createLease(null, "7"));
        assertEquals(expectedOutput, output);
        verify(metricsScope).addDimension("Operation", "TakeLeases");
        verify(metricsScope).addData("ExpiredLeases", 3, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        verify(metricsScope).addData("LeaseSpillover", 0, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        verify(metricsScope).addData("LeasesToTake", 3, StandardUnit.COUNT, MetricsLevel.DETAILED);
        verify(metricsScope).addData("NeededLeases", 0, StandardUnit.COUNT, MetricsLevel.DETAILED);
        verify(metricsScope).addData("NumWorkers", 2, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        verify(metricsScope).addData("TotalLeases", 6, StandardUnit.COUNT, MetricsLevel.DETAILED);
        verify(metricsScope).addData("VeryOldLeases", 0, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        verify(metricsScope).addData("NumSingleStreamLeases", 6, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        verify(metricsScope).addData("NumMultiStreamLeases", 0, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        verify(metricsScope).end();
    }

    @Test
    public void test_computesCorrectLeasesWhenInMultistreamFormat() throws Exception {
        long veryOldThreshold = MOCK_CURRENT_TIME -
                (TimeUnit.MILLISECONDS.toNanos(LEASE_DURATION_MILLIS) * DEFAULT_VERY_OLD_LEASE_DURATION_MULTIPLIER);
        DynamoDBLeaseTaker dynamoDBLeaseTaker =
                new DynamoDBLeaseTaker(leaseRefresher, WORKER_IDENTIFIER, LEASE_DURATION_MILLIS, metricsFactory)
                        .withEnablePriorityLeaseAssignment(false);
        final List<Lease> allLeases = new ArrayList<>();
        allLeases.add(createLease("bar", "2", MOCK_CURRENT_TIME));
        allLeases.add(createMultiStreamLease("bar", "3", "stream-1", MOCK_CURRENT_TIME, ExtendedSequenceNumber.TRIM_HORIZON));
        allLeases.add(createMultiStreamLease("bar", "4", "stream-2", MOCK_CURRENT_TIME, ExtendedSequenceNumber.SHARD_END));
        allLeases.add(createLease("baz", "5", veryOldThreshold - 1));
        allLeases.add(createMultiStreamLease("baz", "6", "stream-1", veryOldThreshold + 1, ExtendedSequenceNumber.TRIM_HORIZON));
        allLeases.add(createMultiStreamLease(null, "7", "stream-1", 0L, ExtendedSequenceNumber.TRIM_HORIZON));
        final List<Lease> expiredLeases = allLeases.subList(3, 6);

        dynamoDBLeaseTaker.allLeases.putAll(
                allLeases.stream().collect(Collectors.toMap(Lease::leaseKey, Function.identity())));
        when(leaseRefresher.listLeases()).thenReturn(allLeases);
        when(metricsFactory.createMetrics()).thenReturn(metricsScope);
        when(timeProvider.call()).thenReturn(MOCK_CURRENT_TIME);

        Set<Lease> output = dynamoDBLeaseTaker.computeLeasesToTake(expiredLeases, timeProvider);
        final Set<Lease> expectedOutput = new HashSet<>();
        expectedOutput.add(createLease("baz", "5", veryOldThreshold - 1));
        expectedOutput.add(createMultiStreamLease("baz", "6", "stream-1", veryOldThreshold + 1, ExtendedSequenceNumber.TRIM_HORIZON));
        expectedOutput.add(createMultiStreamLease(null, "7", "stream-1", 0L, ExtendedSequenceNumber.TRIM_HORIZON));
        assertEquals(expectedOutput, output);
        verify(metricsScope).addDimension("Operation", "TakeLeases");
        verify(metricsScope).addData("ExpiredLeases", 3, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        verify(metricsScope).addData("LeaseSpillover", 0, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        verify(metricsScope).addData("LeasesToTake", 3, StandardUnit.COUNT, MetricsLevel.DETAILED);
        verify(metricsScope).addData("NeededLeases", 0, StandardUnit.COUNT, MetricsLevel.DETAILED);
        verify(metricsScope).addData("NumWorkers", 2, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        verify(metricsScope).addData("TotalLeases", 6, StandardUnit.COUNT, MetricsLevel.DETAILED);
        verify(metricsScope).addData("VeryOldLeases", 0, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        verify(metricsScope).addData("NumSingleStreamLeases", 2, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        verify(metricsScope).addData("NumMultiStreamLeases", 3, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        verify(metricsScope).end();
    }

    private Lease createLease(String leaseOwner, String leaseKey) {
        final Lease lease = new Lease();
        lease.checkpoint(new ExtendedSequenceNumber("checkpoint"));
        lease.ownerSwitchesSinceCheckpoint(0L);
        lease.leaseCounter(0L);
        lease.leaseOwner(leaseOwner);
        lease.parentShardIds(Collections.singleton("parentShardId"));
        lease.childShardIds(new HashSet<>());
        lease.leaseKey(leaseKey);
        return lease;
    }

    private Lease createLease(String leaseOwner, String leaseKey, long lastCounterIncrementNanos) {
        final Lease lease = new Lease();
        lease.checkpoint(new ExtendedSequenceNumber("checkpoint"));
        lease.ownerSwitchesSinceCheckpoint(0L);
        lease.leaseCounter(0L);
        lease.leaseOwner(leaseOwner);
        lease.parentShardIds(Collections.singleton("parentShardId"));
        lease.childShardIds(new HashSet<>());
        lease.leaseKey(leaseKey);
        lease.lastCounterIncrementNanos(lastCounterIncrementNanos);
        return lease;
    }

    private Lease createMultiStreamLease(String leaseOwner, String shardId, String streamIdentifier, long lastCounterIncrementNanos,
        ExtendedSequenceNumber checkpoint) {
        final MultiStreamLease lease = new MultiStreamLease();
        lease.checkpoint(checkpoint);
        lease.ownerSwitchesSinceCheckpoint(0L);
        lease.leaseCounter(0L);
        lease.leaseOwner(leaseOwner);
        lease.parentShardIds(Collections.singleton("parentShardId"));
        lease.childShardIds(new HashSet<>());
        lease.leaseKey(String.join(":", streamIdentifier, shardId));
        lease.lastCounterIncrementNanos(lastCounterIncrementNanos);
        lease.streamIdentifier(streamIdentifier);
        lease.shardId(shardId);
        return lease;
    }
}
