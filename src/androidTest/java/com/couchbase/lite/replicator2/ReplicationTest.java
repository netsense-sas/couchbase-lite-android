package com.couchbase.lite.replicator2;

import com.couchbase.lite.Database;
import com.couchbase.lite.Document;
import com.couchbase.lite.LiteTestCase;
import com.couchbase.lite.Manager;
import com.couchbase.lite.mockserver.MockChangesFeed;
import com.couchbase.lite.mockserver.MockCheckpointPut;
import com.couchbase.lite.mockserver.MockDispatcher;
import com.couchbase.lite.mockserver.MockDocumentGet;
import com.couchbase.lite.mockserver.MockHelper;
import com.couchbase.lite.util.Log;
import com.github.oxo42.stateless4j.transitions.Transition;
import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.RecordedRequest;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

        Database db = this.manager.getDatabase("closed");
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final Replication replication = new Replication(db, new URL("http://fake.com/foo"));
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

        db.close();
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
        final List<ReplicationStateTransition> transitions = new ArrayList<ReplicationStateTransition>();
        final Replication replication = new Replication(database, new URL("http://fake.com/foo"));
        replication.setContinous(true);
        replication.addChangeListener(new Replication.ChangeListener() {
            @Override
            public void changed(Replication.ChangeEvent event) {
                if (event.getTransition() != null) {
                    transitions.add(event.getTransition());
                    if (event.getTransition().getDestination() == ReplicationState.STOPPED) {
                        countDownLatch.countDown();
                    }
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

        assertEquals(3, transitions.size());

        assertEquals(ReplicationState.INITIAL, transitions.get(0).getSource());
        assertEquals(ReplicationState.RUNNING, transitions.get(0).getDestination());

        assertEquals(ReplicationState.RUNNING, transitions.get(1).getSource());
        assertEquals(ReplicationState.STOPPING, transitions.get(1).getDestination());

        assertEquals(ReplicationState.STOPPING, transitions.get(2).getSource());
        assertEquals(ReplicationState.STOPPED, transitions.get(2).getDestination());

    }

    /**
     * Do a pull replication
     *
     * @param shutdownMockWebserver - should this test shutdown the mockwebserver
     *                              when done?  if another test wants to pick up
     *                              where this left off, you should pass false.
     * @param serverType - should the mock return the Sync Gateway server type in
     *                   the "Server" HTTP Header?  this changes the behavior of the
     *                   replicator to use bulk_get and POST reqeusts for _changes feeds.
     * @param addAttachments - should the mock sync gateway return docs with attachments?
     * @return a map that contains the mockwebserver (key="server") and the mock dispatcher
     *         (key="dispatcher")
     */
    public Map<String, Object> mockSinglePull(boolean shutdownMockWebserver, MockDispatcher.ServerType serverType, boolean addAttachments) throws Exception {

        // create mockwebserver and custom dispatcher
        MockDispatcher dispatcher = new MockDispatcher();
        MockWebServer server = MockHelper.getMockWebServer(dispatcher);
        dispatcher.setServerType(serverType);

        // mock documents to be pulled
        MockDocumentGet.MockDocument mockDoc1 = new MockDocumentGet.MockDocument("doc1", "1-5e38", 1);
        mockDoc1.setJsonMap(MockHelper.generateRandomJsonMap());
        mockDoc1.setAttachmentName("attachment.png");
        MockDocumentGet.MockDocument mockDoc2 = new MockDocumentGet.MockDocument("doc2", "1-563b", 2);
        mockDoc2.setJsonMap(MockHelper.generateRandomJsonMap());
        mockDoc2.setAttachmentName("attachment2.png");

        // checkpoint GET response w/ 404
        MockResponse fakeCheckpointResponse = new MockResponse();
        MockHelper.set404NotFoundJson(fakeCheckpointResponse);
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHECKPOINT, fakeCheckpointResponse);

        // _changes response
        MockChangesFeed mockChangesFeed = new MockChangesFeed();
        mockChangesFeed.add(new MockChangesFeed.MockChangedDoc(mockDoc1));
        mockChangesFeed.add(new MockChangesFeed.MockChangedDoc(mockDoc2));
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHANGES, mockChangesFeed.generateMockResponse());

        // doc1 response
        MockDocumentGet mockDocumentGet = new MockDocumentGet(mockDoc1);
        if (addAttachments) {
            mockDocumentGet.addAttachmentFilename(mockDoc1.getAttachmentName());
        }
        dispatcher.enqueueResponse(mockDoc1.getDocPathRegex(), mockDocumentGet.generateMockResponse());

        // doc2 response
        mockDocumentGet = new MockDocumentGet(mockDoc2);
        if (addAttachments) {
            mockDocumentGet.addAttachmentFilename(mockDoc2.getAttachmentName());
        }
        dispatcher.enqueueResponse(mockDoc2.getDocPathRegex(), mockDocumentGet.generateMockResponse());

        // respond to all PUT Checkpoint requests
        MockCheckpointPut mockCheckpointPut = new MockCheckpointPut();
        mockCheckpointPut.setSticky(true);
        mockCheckpointPut.setDelayMs(500);
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHECKPOINT, mockCheckpointPut);

        // start mock server
        server.play();

        // run pull replication
        Replication pullReplication = database.createPullReplication2(server.getUrl("/db"));
        runReplication2(pullReplication);

        // assert that we now have both docs in local db
        assertNotNull(database);
        Document doc1 = database.getDocument(mockDoc1.getDocId());
        assertNotNull(doc1);
        assertNotNull(doc1.getCurrentRevisionId());
        assertTrue(doc1.getCurrentRevisionId().equals(mockDoc1.getDocRev()));
        assertNotNull(doc1.getProperties());
        assertEquals(mockDoc1.getJsonMap(), doc1.getUserProperties());
        Document doc2 = database.getDocument(mockDoc2.getDocId());
        assertNotNull(doc2);
        assertNotNull(doc2.getCurrentRevisionId());
        assertNotNull(doc2.getProperties());
        assertTrue(doc2.getCurrentRevisionId().equals(mockDoc2.getDocRev()));
        assertEquals(mockDoc2.getJsonMap(), doc2.getUserProperties());

        // assert that docs have attachments (if applicable)
        if (addAttachments) {
            attachmentAsserts(mockDoc1.getAttachmentName(), doc1);
            attachmentAsserts(mockDoc2.getAttachmentName(), doc2);
        }

        // make assertions about outgoing requests from replicator -> mock
        RecordedRequest getCheckpointRequest = dispatcher.takeRequest(MockHelper.PATH_REGEX_CHECKPOINT);
        assertTrue(getCheckpointRequest.getMethod().equals("GET"));
        assertTrue(getCheckpointRequest.getPath().matches(MockHelper.PATH_REGEX_CHECKPOINT));
        RecordedRequest getChangesFeedRequest = dispatcher.takeRequest(MockHelper.PATH_REGEX_CHANGES);
        if (serverType == MockDispatcher.ServerType.SYNC_GW) {
            assertTrue(getChangesFeedRequest.getMethod().equals("POST"));

        } else {
            assertTrue(getChangesFeedRequest.getMethod().equals("GET"));
        }
        assertTrue(getChangesFeedRequest.getPath().matches(MockHelper.PATH_REGEX_CHANGES));
        RecordedRequest doc1Request = dispatcher.takeRequest(mockDoc1.getDocPathRegex());
        assertTrue(doc1Request.getMethod().equals("GET"));
        assertTrue(doc1Request.getPath().matches(mockDoc1.getDocPathRegex()));
        RecordedRequest doc2Request = dispatcher.takeRequest(mockDoc2.getDocPathRegex());
        assertTrue(doc2Request.getMethod().equals("GET"));
        assertTrue(doc2Request.getPath().matches(mockDoc2.getDocPathRegex()));

        // wait until the mock webserver receives a PUT checkpoint request with doc #2's sequence
        List<RecordedRequest> checkpointRequests = waitForPutCheckpointRequestWithSequence(dispatcher, mockDoc2.getDocSeq());
        validateCheckpointRequestsRevisions(checkpointRequests);
        assertEquals(1, checkpointRequests.size());

        // assert our local sequence matches what is expected
        String lastSequence = database.lastSequenceWithCheckpointId(pullReplication.remoteCheckpointDocID());
        assertEquals(Integer.toString(mockDoc2.getDocSeq()), lastSequence);

        // assert completed count makes sense
        assertEquals(pullReplication.getChangesCount(), pullReplication.getCompletedChangesCount());

        // Shut down the server. Instances cannot be reused.
        if (shutdownMockWebserver) {
            server.shutdown();
        }

        Map<String, Object> returnVal = new HashMap<String, Object>();
        returnVal.put("server", server);
        returnVal.put("dispatcher", dispatcher);

        return returnVal;

    }


}
