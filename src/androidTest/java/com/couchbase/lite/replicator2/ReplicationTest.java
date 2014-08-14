package com.couchbase.lite.replicator2;

import com.couchbase.lite.Database;
import com.couchbase.lite.LiteTestCase;
import com.couchbase.lite.Manager;
import com.couchbase.lite.util.Log;

import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tests for the new state machine based replicator
 */
public class ReplicationTest extends LiteTestCase {

    public static final String TAG = com.couchbase.lite.replicator.ReplicationTest.TAG;

    /**
     * Start continuous replication with a closed db.
     *
     * Expected behavior:
     *   - Receive replication finished callback
     *   - Replication lastError will contain an exception
     */
    public void testStartReplicationClosedDb() throws Exception {

        Database closedDb = this.manager.getDatabase("closed");
        closedDb.close();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final Replication replication = new Replication(closedDb, new URL("http://fake.com/foo"));
        replication.setContinous(true);
        replication.addChangeListener(new Replication.ChangeListener() {
            @Override
            public void changed(Replication.ChangeEvent event) {
                Log.d(TAG, "changed event: %s", event);
                if (replication.isRunning() == false) {
                    countDownLatch.countDown();
                }

            }
        });
        replication.start();

        boolean success = countDownLatch.await(60, TimeUnit.SECONDS);
        assertTrue(success);

        assertTrue(replication.getLastError() != null);

    }

    /**
     * Start a replication and stop it immediately
     */
    public void testStartReplicationStartStop() throws Exception {

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final Replication replication = new Replication(database, new URL("http://fake.com/foo"));
        replication.setContinous(true);
        replication.addChangeListener(new Replication.ChangeListener() {
            @Override
            public void changed(Replication.ChangeEvent event) {
                if (replication.isRunning() == false) {
                    countDownLatch.countDown();
                }
            }
        });

        replication.start();
        replication.start();  // this should be ignored

        replication.stop();
        replication.stop();  // this should be ignored

        boolean success = countDownLatch.await(60, TimeUnit.SECONDS);
        assertTrue(success);

        assertTrue(replication.getLastError() == null);
    }

}
