package com.couchbase.lite.replicator2;

import com.couchbase.lite.Database;
import com.couchbase.lite.Document;
import com.couchbase.lite.LiteTestCase;
import com.couchbase.lite.Manager;
import com.couchbase.lite.mockserver.MockChangesFeed;
import com.couchbase.lite.mockserver.MockCheckpointGet;
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
import java.util.Arrays;
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
        final Replication replication = db.createPullReplication2(new URL("http://fake.com/foo"));
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
    public void failingTestStartReplicationStartStop() throws Exception {

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final List<ReplicationStateTransition> transitions = new ArrayList<ReplicationStateTransition>();
        final Replication replication = database.createPullReplication2(new URL("http://fake.com/foo"));
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
     * Pull replication test:
     *
     * - Single one-shot pull replication
     * - Against simulated sync gateway
     * - Remote docs do not have attachments
     */
    public void testMockSinglePullSyncGw() throws Exception {

        boolean shutdownMockWebserver = true;
        boolean addAttachments = false;

        mockSinglePull(shutdownMockWebserver, MockDispatcher.ServerType.SYNC_GW, addAttachments);

    }

    /**
     * Pull replication test:
     *
     * - Single one-shot pull replication
     * - Against simulated couchdb
     * - Remote docs do not have attachments
     */
    public void testMockSinglePullCouchDb() throws Exception {

        boolean shutdownMockWebserver = true;
        boolean addAttachments = false;


        mockSinglePull(shutdownMockWebserver, MockDispatcher.ServerType.COUCHDB, addAttachments);

    }

    /**
     * Pull replication test:
     *
     * - Single one-shot pull replication
     * - Against simulated couchdb
     * - Remote docs have attachments
     */
    public void testMockSinglePullCouchDbAttachments() throws Exception {

        boolean shutdownMockWebserver = true;
        boolean addAttachments = true;


        mockSinglePull(shutdownMockWebserver, MockDispatcher.ServerType.COUCHDB, addAttachments);

    }

    /**
     * Pull replication test:
     *
     * - Single one-shot pull replication
     * - Against simulated sync gateway
     * - Remote docs have attachments
     */
    public void testMockSinglePullSyncGwAttachments() throws Exception {

        boolean shutdownMockWebserver = true;
        boolean addAttachments = true;

        mockSinglePull(shutdownMockWebserver, MockDispatcher.ServerType.SYNC_GW, addAttachments);

    }

    public void testMockMultiplePullSyncGw() throws Exception {

        boolean shutdownMockWebserver = true;

        mockMultiplePull(shutdownMockWebserver, MockDispatcher.ServerType.SYNC_GW);

    }

    public void testMockMultiplePullCouchDb() throws Exception {

        boolean shutdownMockWebserver = true;

        mockMultiplePull(shutdownMockWebserver, MockDispatcher.ServerType.COUCHDB);

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
        String checkpointId = pullReplication.remoteCheckpointDocID();
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
        assertNotNull(getCheckpointRequest);
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

        // give it some time to actually save checkpoint to db
        Thread.sleep(500);

        // assert our local sequence matches what is expected
        String lastSequence = database.lastSequenceWithCheckpointId(checkpointId);
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

    /**
     *
     * Simulate the following:
     *
     * - Add a few docs and do a pull replication
     * - One doc on sync gateway is now updated
     * - Do a second pull replication
     * - Assert we get the updated doc and save it locally
     *
     */
    public Map<String, Object> mockMultiplePull(boolean shutdownMockWebserver, MockDispatcher.ServerType serverType) throws Exception {

        String doc1Id = "doc1";

        // create mockwebserver and custom dispatcher
        boolean addAttachments = false;

        // do a pull replication
        Map<String, Object> serverAndDispatcher = mockSinglePull(false, serverType, addAttachments);

        MockWebServer server = (MockWebServer) serverAndDispatcher.get("server");
        MockDispatcher dispatcher = (MockDispatcher) serverAndDispatcher.get("dispatcher");

        // clear out any possible residue left from previous test, eg, mock responses queued up as
        // any recorded requests that have been logged.
        dispatcher.reset();

        String doc1Rev = "2-2e38";
        int doc1Seq = 3;
        String checkpointRev = "0-1";
        String checkpointLastSequence = "2";

        // checkpoint GET response w/ seq = 2
        MockCheckpointGet mockCheckpointGet = new MockCheckpointGet();
        mockCheckpointGet.setOk("true");
        mockCheckpointGet.setRev(checkpointRev);
        mockCheckpointGet.setLastSequence(checkpointLastSequence);
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHECKPOINT, mockCheckpointGet);

        // _changes response
        MockChangesFeed mockChangesFeed = new MockChangesFeed();
        MockChangesFeed.MockChangedDoc mockChangedDoc1 = new MockChangesFeed.MockChangedDoc()
                .setSeq(doc1Seq)
                .setDocId(doc1Id)
                .setChangedRevIds(Arrays.asList(doc1Rev));
        mockChangesFeed.add(mockChangedDoc1);
        MockResponse fakeChangesResponse = mockChangesFeed.generateMockResponse();
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHANGES, fakeChangesResponse);

        // doc1 response
        Map<String, Object> doc1JsonMap = MockHelper.generateRandomJsonMap();
        MockDocumentGet mockDocumentGet = new MockDocumentGet()
                .setDocId(doc1Id)
                .setRev(doc1Rev)
                .setJsonMap(doc1JsonMap);
        String doc1PathRegex = "/db/doc1.*";
        dispatcher.enqueueResponse(doc1PathRegex, mockDocumentGet.generateMockResponse());

        // checkpoint PUT response
        MockCheckpointPut mockCheckpointPut = new MockCheckpointPut();
        mockCheckpointGet.setSticky(true);
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHECKPOINT, mockCheckpointPut);

        // run pull replication
        Replication pullReplication = database.createPullReplication2(server.getUrl("/db"));
        runReplication2(pullReplication);

        // assert that we now have both docs in local db
        assertNotNull(database);
        Document doc1 = database.getDocument(doc1Id);
        assertNotNull(doc1);
        assertNotNull(doc1.getCurrentRevisionId());
        assertTrue(doc1.getCurrentRevisionId().startsWith("2-"));
        assertEquals(doc1JsonMap, doc1.getUserProperties());

        // make assertions about outgoing requests from replicator -> mock
        RecordedRequest getCheckpointRequest = dispatcher.takeRequest(MockHelper.PATH_REGEX_CHECKPOINT);
        assertNotNull(getCheckpointRequest);
        assertTrue(getCheckpointRequest.getMethod().equals("GET"));
        assertTrue(getCheckpointRequest.getPath().matches(MockHelper.PATH_REGEX_CHECKPOINT));
        RecordedRequest getChangesFeedRequest = dispatcher.takeRequest(MockHelper.PATH_REGEX_CHANGES);

        if (serverType == MockDispatcher.ServerType.SYNC_GW) {
            assertTrue(getChangesFeedRequest.getMethod().equals("POST"));

        } else {
            assertTrue(getChangesFeedRequest.getMethod().equals("GET"));
        }
        assertTrue(getChangesFeedRequest.getPath().matches(MockHelper.PATH_REGEX_CHANGES));
        if (serverType == MockDispatcher.ServerType.SYNC_GW) {
            Map <String, Object> jsonMap = Manager.getObjectMapper().readValue(getChangesFeedRequest.getUtf8Body(), Map.class);
            assertTrue(jsonMap.containsKey("since"));
            Integer since = (Integer) jsonMap.get("since");
            assertEquals(2, since.intValue());
        }

        RecordedRequest doc1Request = dispatcher.takeRequest(doc1PathRegex);
        assertTrue(doc1Request.getMethod().equals("GET"));
        assertTrue(doc1Request.getPath().matches("/db/doc1\\?rev=2-2e38.*"));

        // wait until the mock webserver receives a PUT checkpoint request with doc #2's sequence
        int expectedLastSequence = doc1Seq;
        List<RecordedRequest> checkpointRequests = waitForPutCheckpointRequestWithSequence(dispatcher, expectedLastSequence);
        assertEquals(1, checkpointRequests.size());

        // assert our local sequence matches what is expected
        String lastSequence = database.lastSequenceWithCheckpointId(pullReplication.remoteCheckpointDocID());
        assertEquals(Integer.toString(expectedLastSequence), lastSequence);

        // assert completed count makes sense
        assertEquals(pullReplication.getChangesCount(), pullReplication.getCompletedChangesCount());

        if (shutdownMockWebserver) {
            server.shutdown();
        }

        Map<String, Object> returnVal = new HashMap<String, Object>();
        returnVal.put("server", server);
        returnVal.put("dispatcher", dispatcher);

        return returnVal;

    }


}
