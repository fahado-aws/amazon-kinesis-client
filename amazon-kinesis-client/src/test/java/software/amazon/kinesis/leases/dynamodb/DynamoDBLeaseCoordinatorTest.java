package software.amazon.kinesis.leases.dynamodb;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import lombok.SneakyThrows;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.metrics.MetricsFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBLeaseCoordinatorTest {

    private static final String WORKER_ID = UUID.randomUUID().toString();
    private static final boolean ENABLE_PRIORITY_LEASE_ASSIGNMENT = true;
    private static final long LEASE_DURATION_MILLIS = 5000L;
    private static final long EPSILON_MILLIS = 25L;
    private static final int MAX_LEASES_FOR_WORKER = Integer.MAX_VALUE;
    private static final int MAX_LEASES_TO_STEAL_AT_ONE_TIME = 1;
    private static final int MAX_LEASE_RENEWER_THREAD_COUNT = 20;
    private static final long INITIAL_LEASE_TABLE_READ_CAPACITY = 10L;
    private static final long INITIAL_LEASE_TABLE_WRITE_CAPACITY = 10L;
    private static final long SECONDS_BETWEEN_POLLS = 10L;
    private static final long TIMEOUT_SECONDS = 600L;

    @Mock
    private LeaseRefresher leaseRefresher;
    @Mock
    private MetricsFactory metricsFactory;

    private DynamoDBLeaseCoordinator leaseCoordinator;

    @Before
    public void setup() {
        this.leaseCoordinator = new DynamoDBLeaseCoordinator(leaseRefresher, WORKER_ID, LEASE_DURATION_MILLIS,
                ENABLE_PRIORITY_LEASE_ASSIGNMENT, EPSILON_MILLIS, MAX_LEASES_FOR_WORKER,
                MAX_LEASES_TO_STEAL_AT_ONE_TIME, MAX_LEASE_RENEWER_THREAD_COUNT,
                INITIAL_LEASE_TABLE_READ_CAPACITY, INITIAL_LEASE_TABLE_WRITE_CAPACITY, metricsFactory);
    }

    @Test
    public void testInitialize_tableCreationSucceeds() throws Exception {
        when(leaseRefresher.createLeaseTableIfNotExists()).thenReturn(true);
        when(leaseRefresher.waitUntilLeaseTableExists(SECONDS_BETWEEN_POLLS, TIMEOUT_SECONDS)).thenReturn(true);

        leaseCoordinator.initialize();

        verify(leaseRefresher).createLeaseTableIfNotExists();
        verify(leaseRefresher).waitUntilLeaseTableExists(SECONDS_BETWEEN_POLLS, TIMEOUT_SECONDS);
    }

    @Test(expected = DependencyException.class)
    public void testInitialize_tableCreationFails() throws Exception {
        when(leaseRefresher.createLeaseTableIfNotExists()).thenReturn(false);
        when(leaseRefresher.waitUntilLeaseTableExists(SECONDS_BETWEEN_POLLS, TIMEOUT_SECONDS)).thenReturn(false);

        try {
            leaseCoordinator.initialize();
        } finally {
            verify(leaseRefresher).createLeaseTableIfNotExists();
            verify(leaseRefresher).waitUntilLeaseTableExists(SECONDS_BETWEEN_POLLS, TIMEOUT_SECONDS);
        }
    }

    /**
     * Validates a {@link NullPointerException} is not thrown when the lease taker
     * is stopped before it starts/exists.
     *
     * @see <a href="https://github.com/awslabs/amazon-kinesis-client/issues/745">issue #745</a>
     * @see <a href="https://github.com/awslabs/amazon-kinesis-client/issues/900">issue #900</a>
     */
    @Test
    public void testStopLeaseTakerBeforeStart() {
        leaseCoordinator.stopLeaseTaker();
        assertTrue(leaseCoordinator.getAssignments().isEmpty());
    }

    @SneakyThrows
    @Test
    public void testAddLeasesToRenew() {
        when(leaseRefresher.createLeaseTableIfNotExists()).thenReturn(true);
        when(leaseRefresher.waitUntilLeaseTableExists(SECONDS_BETWEEN_POLLS, TIMEOUT_SECONDS)).thenReturn(true);
        Lease lease1 = new Lease();
        lease1.leaseKey("shardId-001");
        lease1.lastCounterIncrementNanos(System.nanoTime());
        Lease lease2 = new Lease();
        lease2.leaseKey("shardId-002");
        lease2.lastCounterIncrementNanos(System.nanoTime());
        Collection<Lease> leases = Arrays.asList(lease1, lease2);

        leaseCoordinator.start();
        leaseCoordinator.addLeasesToRenew(leases);

        assertEquals(leases.size(), leaseCoordinator.getCurrentAssignments().size());
        assertNotNull(leaseCoordinator.getCurrentlyHeldLease(lease1.leaseKey()));
        assertNotNull(leaseCoordinator.getCurrentlyHeldLease(lease2.leaseKey()));

        leaseCoordinator.stop();
    }

    @Test
    public void testAddLeasesToRenewDoesNothingWhenCoordinatorIsNotRunning() {
        Lease lease1 = new Lease();
        lease1.leaseKey("shardId-001");
        lease1.lastCounterIncrementNanos(System.nanoTime());
        Lease lease2 = new Lease();
        lease2.leaseKey("shardId-002");
        lease2.lastCounterIncrementNanos(System.nanoTime());
        Collection<Lease> leases = Arrays.asList(lease1, lease2);

        leaseCoordinator.addLeasesToRenew(leases);

        assertEquals(0, leaseCoordinator.getCurrentAssignments().size());
    }

    @SneakyThrows
    @Test
    public void testDropLeases() {
        when(leaseRefresher.createLeaseTableIfNotExists()).thenReturn(true);
        when(leaseRefresher.waitUntilLeaseTableExists(SECONDS_BETWEEN_POLLS, TIMEOUT_SECONDS)).thenReturn(true);
        Lease lease1 = new Lease();
        lease1.leaseKey("shardId-001");
        lease1.lastCounterIncrementNanos(System.nanoTime());
        Lease lease2 = new Lease();
        lease2.leaseKey("shardId-002");
        lease2.lastCounterIncrementNanos(System.nanoTime());
        Lease lease3 = new Lease();
        lease3.leaseKey("shardId-003");
        lease3.lastCounterIncrementNanos(System.nanoTime());
        Collection<Lease> leases = Arrays.asList(lease1, lease2, lease3);

        leaseCoordinator.start();
        leaseCoordinator.addLeasesToRenew(leases);
        leaseCoordinator.dropLeases(Arrays.asList(lease2, lease3));

        assertEquals(1, leaseCoordinator.getCurrentAssignments().size());
        assertNotNull(leaseCoordinator.getCurrentlyHeldLease(lease1.leaseKey()));
        assertNull(leaseCoordinator.getCurrentlyHeldLease(lease2.leaseKey()));
        assertNull(leaseCoordinator.getCurrentlyHeldLease(lease3.leaseKey()));

        leaseCoordinator.stop();
    }

    @SneakyThrows
    @Test
    public void testDropLeasesDoesNothingWhenCoordinatorIsNotRunning() {
        Lease lease1 = new Lease();
        lease1.leaseKey("shardId-001");
        lease1.lastCounterIncrementNanos(System.nanoTime());
        Lease lease2 = new Lease();
        lease2.leaseKey("shardId-002");
        lease2.lastCounterIncrementNanos(System.nanoTime());
        Collection<Lease> leases = Arrays.asList(lease1, lease2);

        assertEquals(0, leaseCoordinator.getCurrentAssignments().size());
        leaseCoordinator.dropLeases(leases);
        assertEquals(0, leaseCoordinator.getCurrentAssignments().size());
    }

}
