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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import com.google.common.annotations.VisibleForTesting;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.utils.Validate;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.LeaseRenewer;
import software.amazon.kinesis.leases.MultiStreamLease;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.processor.StreamTracker.StreamProcessingMode;

/**
 * An implementation of {@link LeaseRenewer} that uses DynamoDB via {@link LeaseRefresher}.
 */
@Slf4j
@KinesisClientInternalApi
public class DynamoDBLeaseRenewer implements LeaseRenewer {
    private static final int RENEWAL_RETRIES = 2;
    private static final String RENEW_ALL_LEASES_DIMENSION = "RenewAllLeases";
    private static final String UPGRADE_LEASE_OPERATION = "UpgradeLease";
    private static final String AUDIT_UPGRADED_LEASE_OPERATION = "AuditUpgradedLease";

    private final LeaseRefresher leaseRefresher;
    private final String workerIdentifier;
    private final long leaseDurationNanos;
    private final ExecutorService executorService;
    private final MetricsFactory metricsFactory;
    private final StreamProcessingMode streamProcessingMode;
    private final Map<StreamIdentifier, StreamConfig> streamConfigMap;

    private final ConcurrentNavigableMap<String, Lease> ownedLeases = new ConcurrentSkipListMap<>();

    /**
     * Constructor.
     *
     * @param leaseRefresher
     *            LeaseRefresher to use
     * @param workerIdentifier
     *            identifier of this worker
     * @param leaseDurationMillis
     *            duration of a lease in milliseconds
     * @param executorService
     *            ExecutorService to use for renewing leases in parallel
     */
    public DynamoDBLeaseRenewer(final LeaseRefresher leaseRefresher, final String workerIdentifier,
            final long leaseDurationMillis, final ExecutorService executorService,
            final MetricsFactory metricsFactory) {
        this.leaseRefresher = leaseRefresher;
        this.workerIdentifier = workerIdentifier;
        this.leaseDurationNanos = TimeUnit.MILLISECONDS.toNanos(leaseDurationMillis);
        this.executorService = executorService;
        this.metricsFactory = metricsFactory;
        this.streamProcessingMode = null;
        this.streamConfigMap = null;
    }

    /**
     * Constructor.
     *
     * @param leaseRefresher
     *            LeaseRefresher to use
     * @param workerIdentifier
     *            identifier of this worker
     * @param leaseDurationMillis
     *            duration of a lease in milliseconds
     * @param executorService
     *            ExecutorService to use for renewing leases in parallel
     * @param streamProcessingMode
     *            StreamProcessingMode in which application is running
     * @param streamConfigMap
     *            Map of streams that application is configured to process
     */
    public DynamoDBLeaseRenewer(final LeaseRefresher leaseRefresher, final String workerIdentifier,
            final long leaseDurationMillis, final ExecutorService executorService,
            final MetricsFactory metricsFactory, final StreamProcessingMode streamProcessingMode,
            final Map<StreamIdentifier, StreamConfig> streamConfigMap) {
        this.leaseRefresher = leaseRefresher;
        this.workerIdentifier = workerIdentifier;
        this.leaseDurationNanos = TimeUnit.MILLISECONDS.toNanos(leaseDurationMillis);
        this.executorService = executorService;
        this.metricsFactory = metricsFactory;
        if (StreamProcessingMode.SINGLE_STREAM_UPGRADE_MODE == streamProcessingMode) {
            Validate.isTrue(streamConfigMap.size() == 1, "Lease cannot be converted to MultiStream"
                + " format when more than one stream is provided");
        }
        this.streamProcessingMode = streamProcessingMode;
        this.streamConfigMap = streamConfigMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void renewLeases() throws DependencyException, InvalidStateException {
        // Due to the eventually consistent nature of ConcurrentNavigableMap iterators, this log entry may become
        // inaccurate during iteration.
        log.debug("Worker {} holding {} leases: {}", workerIdentifier, ownedLeases.size(), ownedLeases);

        /*
         * Lease renewals are done in parallel so many leases can be renewed for short lease fail over time
         * configuration. In this case, metrics scope is also shared across different threads, so scope must be thread
         * safe.
         */
        final MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, RENEW_ALL_LEASES_DIMENSION);

        long startTime = System.currentTimeMillis();
        boolean success = false;

        try {
        /*
         * We iterate in descending order here so that the synchronized(lease) inside renewLease doesn't "lead" calls
         * to getCurrentlyHeldLeases. They'll still cross paths, but they won't interleave their executions.
         */
            int lostLeases = 0;
            List<Future<Boolean>> renewLeaseTasks = new ArrayList<>();
            for (Lease lease : ownedLeases.descendingMap().values()) {
                renewLeaseTasks.add(executorService.submit(new RenewLeaseTask(lease)));
            }
            int leasesInUnknownState = 0;
            Exception lastException = null;
            for (Future<Boolean> renewLeaseTask : renewLeaseTasks) {
                try {
                    if (!renewLeaseTask.get()) {
                        lostLeases++;
                    }
                } catch (InterruptedException e) {
                    log.info("Interrupted while waiting for a lease to renew.");
                    leasesInUnknownState += 1;
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    log.error("Encountered an exception while renewing a lease.", e.getCause());
                    leasesInUnknownState += 1;
                    lastException = e;
                }
            }

            scope.addData("LostLeases", lostLeases, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            scope.addData("CurrentLeases", ownedLeases.size(), StandardUnit.COUNT, MetricsLevel.SUMMARY);
            if (leasesInUnknownState > 0) {
                throw new DependencyException(
                        String.format("Encountered an exception while renewing leases. The number"
                                + " of leases which might not have been renewed is %d", leasesInUnknownState),
                        lastException);
            }
            success = true;
        } finally {
            MetricsUtil.addWorkerIdentifier(scope, workerIdentifier);
            MetricsUtil.addSuccessAndLatency(scope, success, startTime, MetricsLevel.SUMMARY);
            MetricsUtil.endScope(scope);
        }
    }

    @RequiredArgsConstructor
    private class RenewLeaseTask implements Callable<Boolean> {
        private final Lease lease;

        @Override
        public Boolean call() throws Exception {
            return renewLease(lease);
        }
    }

    private boolean renewLease(Lease lease) throws DependencyException, InvalidStateException {
        return renewLease(lease, false);
    }

    private boolean renewLease(Lease lease, boolean renewEvenIfExpired) throws DependencyException, InvalidStateException {
        String leaseKey = lease.leaseKey();

        final MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, RENEW_ALL_LEASES_DIMENSION);

        boolean success = false;
        boolean renewedLease = false;
        long startTime = System.currentTimeMillis();
        try {
            for (int i = 1; i <= RENEWAL_RETRIES; i++) {
                try {
                    synchronized (lease) {
                        // Don't renew expired lease during regular renewals. getCopyOfHeldLease may have returned null
                        // triggering the application processing to treat this as a lost lease (fail checkpoint with
                        // ShutdownException).
                        boolean isLeaseExpired = lease.isExpired(leaseDurationNanos, System.nanoTime());
                        if (renewEvenIfExpired || !isLeaseExpired) {
                            renewedLease = leaseRefresher.renewLease(lease);
                        }
                        if (renewedLease) {
                            lease.lastCounterIncrementNanos(System.nanoTime());
                        }
                    }

                    if (renewedLease) {
                        if (log.isDebugEnabled()) {
                            log.debug("Worker {} successfully renewed lease with key {}", workerIdentifier, leaseKey);
                        }
                    } else {
                        log.info("Worker {} lost lease with key {}", workerIdentifier, leaseKey);
                        ownedLeases.remove(leaseKey);
                    }

                    success = true;
                    break;
                } catch (ProvisionedThroughputException e) {
                    log.info("Worker {} could not renew lease with key {} on try {} out of {} due to capacity",
                            workerIdentifier, leaseKey, i, RENEWAL_RETRIES);
                }
            }
        } finally {
            MetricsUtil.addWorkerIdentifier(scope, workerIdentifier);
            MetricsUtil.addSuccessAndLatency(scope, "RenewLease", success, startTime, MetricsLevel.DETAILED);
            MetricsUtil.endScope(scope);
        }

        return renewedLease;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Lease> getCurrentlyHeldLeases() {
        Map<String, Lease> result = new HashMap<>();
        long now = System.nanoTime();

        for (String leaseKey : ownedLeases.keySet()) {
            Lease copy = getCopyOfHeldLease(leaseKey, now);
            if (copy != null) {
                result.put(copy.leaseKey(), copy);
            }
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Lease getCurrentlyHeldLease(String leaseKey) {
        return getCopyOfHeldLease(leaseKey, System.nanoTime());
    }

    /**
     * Internal method to return a lease with a specific lease key only if we currently hold it.
     *
     * @param leaseKey key of lease to return
     * @param now current timestamp for old-ness checking
     * @return non-authoritative copy of the held lease, or null if we don't currently hold it
     */
    private Lease getCopyOfHeldLease(String leaseKey, long now) {
        Lease authoritativeLease = ownedLeases.get(leaseKey);
        if (authoritativeLease == null) {
            return null;
        } else {
            Lease copy = null;
            synchronized (authoritativeLease) {
                copy = authoritativeLease.copy();
            }

            if (copy.isExpired(leaseDurationNanos, now)) {
                log.info("getCurrentlyHeldLease not returning lease with key {} because it is expired",
                        copy.leaseKey());
                return null;
            } else {
                return copy;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean updateLease(Lease lease, UUID concurrencyToken, @NonNull String operation, String singleStreamShardId)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        verifyNotNull(lease, "lease cannot be null");
        verifyNotNull(lease.leaseKey(), "leaseKey cannot be null");
        verifyNotNull(concurrencyToken, "concurrencyToken cannot be null");

        String leaseKey = lease.leaseKey();
        Lease authoritativeLease = ownedLeases.get(leaseKey);

        if (authoritativeLease == null) {
            log.info("Worker {} could not update lease with key {} because it does not hold it", workerIdentifier,
                    leaseKey);
            return false;
        }

        /*
         * If the passed-in concurrency token doesn't match the concurrency token of the authoritative lease, it means
         * the lease was lost and regained between when the caller acquired his concurrency token and when the caller
         * called update.
         */
        if (!authoritativeLease.concurrencyToken().equals(concurrencyToken)) {
            log.info("Worker {} refusing to update lease with key {} because concurrency tokens don't match",
                    workerIdentifier, leaseKey);
            return false;
        }

        final MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, operation);
        if (lease instanceof MultiStreamLease) {
            MetricsUtil.addStreamId(scope,
                    StreamIdentifier.multiStreamInstance(((MultiStreamLease) lease).streamIdentifier()));
            MetricsUtil.addShardId(scope, ((MultiStreamLease) lease).shardId());
        } else if (StringUtils.isNotEmpty(singleStreamShardId)) {
            MetricsUtil.addShardId(scope, singleStreamShardId);
        }

        long startTime = System.currentTimeMillis();
        boolean success = false;
        try {
            log.info("Updating lease from {} to {}", authoritativeLease, lease);
            synchronized (authoritativeLease) {
                authoritativeLease.update(lease);
                boolean updatedLease = leaseRefresher.updateLease(authoritativeLease);
                if (updatedLease) {
                    // Updates increment the counter
                    authoritativeLease.lastCounterIncrementNanos(System.nanoTime());
                } else {
                    /*
                     * If updateLease returns false, it means someone took the lease from us. Remove the lease
                     * from our set of owned leases pro-actively rather than waiting for a run of renewLeases().
                     */
                    log.info("Worker {} lost lease with key {} - discovered during update", workerIdentifier, leaseKey);

                    /*
                     * Remove only if the value currently in the map is the same as the authoritative lease. We're
                     * guarding against a pause after the concurrency token check above. It plays out like so:
                     *
                     * 1) Concurrency token check passes
                     * 2) Pause. Lose lease, re-acquire lease. This requires at least one lease counter update.
                     * 3) Unpause. leaseRefresher.updateLease fails conditional write due to counter updates, returns
                     * false.
                     * 4) ownedLeases.remove(key, value) doesn't do anything because authoritativeLease does not
                     * .equals() the re-acquired version in the map on the basis of lease counter. This is what we want.
                     * If we just used ownedLease.remove(key), we would have pro-actively removed a lease incorrectly.
                     *
                     * Note that there is a subtlety here - Lease.equals() deliberately does not check the concurrency
                     * token, but it does check the lease counter, so this scheme works.
                     */
                    ownedLeases.remove(leaseKey, authoritativeLease);
                }

                success = true;
                return updatedLease;
            }
        } finally {
            MetricsUtil.addSuccessAndLatency(scope, "UpdateLease", success, startTime, MetricsLevel.DETAILED);
            MetricsUtil.endScope(scope);
        }
    }

    /**
     * {@inheritDoc}
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws DependencyException
     */
    @Override
    public void addLeasesToRenew(Collection<Lease> newLeases) throws DependencyException, ProvisionedThroughputException,
        InvalidStateException {
        verifyNotNull(newLeases, "newLeases cannot be null");

        for (Lease lease : newLeases) {
            if (lease.lastCounterIncrementNanos() == null) {
                log.info("addLeasesToRenew ignoring lease with key {} because it does not have lastRenewalNanos set",
                        lease.leaseKey());
                continue;
            }

            if (streamProcessingMode == StreamProcessingMode.SINGLE_STREAM_UPGRADE_MODE
                && !(lease instanceof MultiStreamLease)) {
                // Upgrading lease to the multi-stream format
                MultiStreamLease multiStreamLease = upgradeLease(lease);
                // Adding new lease and removing old lease from the in-memory map of owned leases
                multiStreamLease.concurrencyToken(UUID.randomUUID());
                ownedLeases.put(multiStreamLease.leaseKey(), multiStreamLease);
                ownedLeases.remove(lease.leaseKey());
                // Auditing the newly created multi-stream lease. We do this as an extra validation
                // step to ensure that no corruption happened on the way to DDB. So we read back and
                // reconstruct the MultiStreamLease object.
                auditUpgradedLease(multiStreamLease);
            } else {
                Lease authoritativeLease = lease.copy();

                /*
                * Assign a concurrency token when we add this to the set of currently owned leases. This ensures that
                * every time we acquire a lease, it gets a new concurrency token.
                */
                authoritativeLease.concurrencyToken(UUID.randomUUID());
                ownedLeases.put(authoritativeLease.leaseKey(), authoritativeLease);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearCurrentlyHeldLeases() {
        ownedLeases.clear();
    }

    /**
     * {@inheritDoc}
     * @param lease the lease to drop.
     */
    @Override
    public void dropLease(Lease lease) {
        ownedLeases.remove(lease.leaseKey());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        Collection<Lease> leases = leaseRefresher.listLeases();
        List<Lease> myLeases = new LinkedList<>();
        boolean renewEvenIfExpired = true;

        for (Lease lease : leases) {
            if (workerIdentifier.equals(lease.leaseOwner())) {
                log.info(" Worker {} found lease {}", workerIdentifier, lease);
                // Okay to renew even if lease is expired, because we start with an empty list and we add the lease to
                // our list only after a successful renew. So we don't need to worry about the edge case where we could
                // continue renewing a lease after signaling a lease loss to the application.

                if (renewLease(lease, renewEvenIfExpired)) {
                    myLeases.add(lease);
                }
            } else {
                log.debug("Worker {} ignoring lease {} ", workerIdentifier, lease);
            }
        }

        addLeasesToRenew(myLeases);
    }

    private void verifyNotNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }

    @VisibleForTesting
    MultiStreamLease convertToMultiStreamLease(Lease lease) {
        StreamConfig streamConfig = streamConfigMap.values().iterator().next();
        MultiStreamLease multiStreamLease = new MultiStreamLease(lease,
            MultiStreamLease.getLeaseKey(streamConfig.streamIdentifier().serialize(), lease.leaseKey()),
            streamConfig.streamIdentifier().serialize(), lease.leaseKey());
        multiStreamLease.streamIdentifier(streamConfig.streamIdentifier().serialize());
        multiStreamLease.shardId(lease.leaseKey());
        return multiStreamLease;
    }

    private MultiStreamLease upgradeLease(final Lease lease) throws DependencyException, InvalidStateException,
        ProvisionedThroughputException {
        final MetricsScope metricsScope = MetricsUtil.createMetricsWithOperation(metricsFactory, UPGRADE_LEASE_OPERATION);
        MetricsUtil.addShardId(metricsScope, lease.leaseKey());
        final long startTime = System.currentTimeMillis();
        MultiStreamLease multiStreamLease = convertToMultiStreamLease(lease);
        boolean success = false;
        try {
            leaseRefresher.replaceLease(lease, multiStreamLease);
            success = true;
        } finally {
            MetricsUtil.addSuccessAndLatency(metricsScope, success, startTime, MetricsLevel.DETAILED);
            MetricsUtil.endScope(metricsScope);
        }
        return multiStreamLease;
    }

    private void auditUpgradedLease(final MultiStreamLease multiStreamLease) throws DependencyException, InvalidStateException,
        ProvisionedThroughputException {
        final MetricsScope metricsScope = MetricsUtil.createMetricsWithOperation(metricsFactory, AUDIT_UPGRADED_LEASE_OPERATION);
        MetricsUtil.addShardId(metricsScope, multiStreamLease.shardId());
        final long startTime = System.currentTimeMillis();
        boolean success = false;
        try {
            Lease upgradedLease = leaseRefresher.getLease(multiStreamLease.leaseKey());
            success = upgradedLease instanceof MultiStreamLease;
        } finally {
            MetricsUtil.addSuccessAndLatency(metricsScope, success, startTime, MetricsLevel.DETAILED);
            MetricsUtil.endScope(metricsScope);
        }
    }


}
