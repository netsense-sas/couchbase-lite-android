package com.couchbase.lite.replicator2;

import com.couchbase.lite.Database;
import com.couchbase.lite.Document;
import com.couchbase.lite.DocumentChange;
import com.couchbase.lite.Emitter;
import com.couchbase.lite.LiteTestCase;
import com.couchbase.lite.LiveQuery;
import com.couchbase.lite.Manager;
import com.couchbase.lite.Mapper;
import com.couchbase.lite.Query;
import com.couchbase.lite.QueryEnumerator;
import com.couchbase.lite.QueryOptions;
import com.couchbase.lite.QueryRow;
import com.couchbase.lite.SavedRevision;
import com.couchbase.lite.UnsavedRevision;
import com.couchbase.lite.View;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.mockserver.MockBulkDocs;
import com.couchbase.lite.mockserver.MockChangesFeed;
import com.couchbase.lite.mockserver.MockChangesFeedNoResponse;
import com.couchbase.lite.mockserver.MockCheckpointGet;
import com.couchbase.lite.mockserver.MockCheckpointPut;
import com.couchbase.lite.mockserver.MockDispatcher;
import com.couchbase.lite.mockserver.MockDocumentGet;
import com.couchbase.lite.mockserver.MockDocumentPut;
import com.couchbase.lite.mockserver.MockHelper;
import com.couchbase.lite.mockserver.MockRevsDiff;
import com.couchbase.lite.replicator.CustomizableMockHttpClient;
import com.couchbase.lite.replicator.ResponderChain;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.util.Log;
import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.RecordedRequest;

import junit.framework.Assert;

import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.cookie.Cookie;
import org.apache.http.entity.mime.MultipartEntity;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
        replication.setContinuous(true);
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
        replication.setContinuous(true);
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

    public void testMockContinuousPullCouchDb() throws Exception {
        boolean shutdownMockWebserver = true;
        mockContinuousPull(shutdownMockWebserver, MockDispatcher.ServerType.COUCHDB);
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

        // give it some time to actually save checkpoint to db
        workAroundSaveCheckpointRaceCondition();

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

    public Map<String, Object> mockContinuousPull(boolean shutdownMockWebserver, MockDispatcher.ServerType serverType) throws Exception {

        assertTrue(serverType == MockDispatcher.ServerType.COUCHDB);

        final int numMockRemoteDocs = 20;  // must be multiple of 10!
        final AtomicInteger numDocsPulledLocally = new AtomicInteger(0);

        MockDispatcher dispatcher = new MockDispatcher();
        dispatcher.setServerType(serverType);
        int numDocsPerChangesResponse = numMockRemoteDocs / 10;
        MockWebServer server = MockHelper.getPreloadedPullTargetMockCouchDB(dispatcher, numMockRemoteDocs, numDocsPerChangesResponse);

        server.play();

        final CountDownLatch receivedAllDocs = new CountDownLatch(1);

        // run pull replication
        Replication pullReplication = database.createPullReplication2(server.getUrl("/db"));
        pullReplication.setContinuous(true);

        final CountDownLatch replicationDoneSignal = new CountDownLatch(1);
        pullReplication.addChangeListener(new com.couchbase.lite.replicator2.Replication.ChangeListener() {
            @Override
            public void changed(com.couchbase.lite.replicator2.Replication.ChangeEvent event) {
                if (event.getTransition() != null) {
                    if (event.getTransition().getDestination() == ReplicationState.STOPPING) {
                        Log.d(TAG, "Replicator is stopping");
                    }
                    if (event.getTransition().getDestination() == ReplicationState.STOPPED) {

                        // assertEquals(event.getChangeCount(), event.getCompletedChangeCount());

                        Log.d(TAG, "Replicator is stopped");
                        replicationDoneSignal.countDown();
                    }
                }
            }
        });

        database.addChangeListener(new Database.ChangeListener() {
            @Override
            public void changed(Database.ChangeEvent event) {
                List<DocumentChange> changes = event.getChanges();
                for (DocumentChange change : changes) {
                    numDocsPulledLocally.addAndGet(1);
                }
                if (numDocsPulledLocally.get() == numMockRemoteDocs) {
                    receivedAllDocs.countDown();
                }
            }
        });

        pullReplication.start();

        // wait until we received all mock docs or timeout occurs
        boolean success = receivedAllDocs.await(60, TimeUnit.SECONDS);
        assertTrue(success);

        // make sure all docs in local db
        Map<String, Object> allDocs = database.getAllDocs(new QueryOptions());
        Integer totalRows = (Integer) allDocs.get("total_rows");
        List rows = (List) allDocs.get("rows");
        assertEquals(numMockRemoteDocs, totalRows.intValue());
        assertEquals(numMockRemoteDocs, rows.size());

        // cleanup / shutdown
        pullReplication.stop();

        success = replicationDoneSignal.await(30, TimeUnit.SECONDS);
        assertTrue(success);

        // wait until the mock webserver receives a PUT checkpoint request with last do's sequence,
        // this avoids ugly and confusing exceptions in the logs.
        List<RecordedRequest> checkpointRequests = waitForPutCheckpointRequestWithSequence(dispatcher, numMockRemoteDocs - 1);
        validateCheckpointRequestsRevisions(checkpointRequests);

        if (shutdownMockWebserver) {
            server.shutdown();
        }

        Map<String, Object> returnVal = new HashMap<String, Object>();
        returnVal.put("server", server);
        returnVal.put("dispatcher", dispatcher);
        return returnVal;

    }

    /**
     * https://github.com/couchbase/couchbase-lite-java-core/issues/257
     *
     * - Create local document with attachment
     * - Start continuous pull replication
     * - MockServer returns _changes with new rev of document
     * - MockServer returns doc multipart response: https://gist.github.com/tleyden/bf36f688d0b5086372fd
     * - Delete doc cache (not sure if needed)
     * - Fetch doc fresh from database
     * - Verify that it still has attachments
     *
     */
    public void testAttachmentsDeletedOnPull() throws Exception {

        String doc1Id = "doc1";
        int doc1Rev2Generation = 2;
        String doc1Rev2Digest = "b";
        String doc1Rev2 = String.format("%d-%s", doc1Rev2Generation, doc1Rev2Digest);
        int doc1Seq1 = 1;
        String doc1AttachName = "attachment.png";
        String contentType = "image/png";

        // create mockwebserver and custom dispatcher
        MockDispatcher dispatcher = new MockDispatcher();
        MockWebServer server = MockHelper.getMockWebServer(dispatcher);
        dispatcher.setServerType(MockDispatcher.ServerType.SYNC_GW);
        server.play();

        // add some documents - verify it has an attachment
        Document doc1 = createDocumentForPushReplication(doc1Id, doc1AttachName, contentType);
        String doc1Rev1 = doc1.getCurrentRevisionId();
        database.clearDocumentCache();
        doc1 = database.getDocument(doc1.getId());
        assertTrue(doc1.getCurrentRevision().getAttachments().size() > 0);

        // checkpoint GET response w/ 404
        MockResponse fakeCheckpointResponse = new MockResponse();
        MockHelper.set404NotFoundJson(fakeCheckpointResponse);
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHECKPOINT, fakeCheckpointResponse);

        // checkpoint PUT response
        MockCheckpointPut mockCheckpointPut = new MockCheckpointPut();
        mockCheckpointPut.setSticky(true);
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHECKPOINT, mockCheckpointPut);

        // add response to 1st _changes request
        final MockDocumentGet.MockDocument mockDocument1 = new MockDocumentGet.MockDocument(
                doc1Id, doc1Rev2, doc1Seq1);
        Map<String, Object> newProperties = new HashMap<String, Object>(doc1.getProperties());
        newProperties.put("_rev", doc1Rev2);
        mockDocument1.setJsonMap(newProperties);
        mockDocument1.setAttachmentName(doc1AttachName);

        MockChangesFeed mockChangesFeed = new MockChangesFeed();
        mockChangesFeed.add(new MockChangesFeed.MockChangedDoc(mockDocument1));
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHANGES, mockChangesFeed.generateMockResponse());

        // add sticky _changes response to feed=longpoll that just blocks for 60 seconds to emulate
        // server that doesn't have any new changes
        MockChangesFeedNoResponse mockChangesFeedNoResponse = new MockChangesFeedNoResponse();
        mockChangesFeedNoResponse.setDelayMs(60 * 1000);
        mockChangesFeedNoResponse.setSticky(true);
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHANGES, mockChangesFeedNoResponse);

        // add response to doc get
        MockDocumentGet mockDocumentGet = new MockDocumentGet(mockDocument1);
        mockDocumentGet.addAttachmentFilename(mockDocument1.getAttachmentName());
        mockDocumentGet.setIncludeAttachmentPart(false);
        Map<String, Object> revHistory = new HashMap<String, Object>();
        revHistory.put("start", doc1Rev2Generation);
        List ids = Arrays.asList(
                RevisionInternal.digestFromRevID(doc1Rev2),
                RevisionInternal.digestFromRevID(doc1Rev1)
        );
        revHistory.put("ids",ids);
        mockDocumentGet.setRevHistoryMap(revHistory);
        dispatcher.enqueueResponse(mockDocument1.getDocPathRegex(), mockDocumentGet.generateMockResponse());

        // create and start pull replication
        Replication pullReplication = database.createPullReplication2(server.getUrl("/db"));
        pullReplication.setContinuous(true);
        pullReplication.start();

        // wait for the next PUT checkpoint request/response
        waitForPutCheckpointRequestWithSeq(dispatcher, 1);

        stopReplication2(pullReplication);

        // clear doc cache
        database.clearDocumentCache();

        // make sure doc has attachments
        Document doc1Fetched = database.getDocument(doc1.getId());
        assertTrue(doc1Fetched.getCurrentRevision().getAttachments().size() > 0);

        server.shutdown();


    }

    /**
     * This is essentially a regression test for a deadlock
     * that was happening when the LiveQuery#onDatabaseChanged()
     * was calling waitForUpdateThread(), but that thread was
     * waiting on connection to be released by the thread calling
     * waitForUpdateThread().  When the deadlock bug was present,
     * this test would trigger the deadlock and never finish.
     */
    public void testPullerWithLiveQuery() throws Throwable {

        View view = database.getView("testPullerWithLiveQueryView");
        view.setMapReduce(new Mapper() {
            @Override
            public void map(Map<String, Object> document, Emitter emitter) {
                if (document.get("_id") != null) {
                    emitter.emit(document.get("_id"), null);
                }
            }
        }, null, "1");

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        LiveQuery allDocsLiveQuery = view.createQuery().toLiveQuery();
        allDocsLiveQuery.addChangeListener(new LiveQuery.ChangeListener() {
            @Override
            public void changed(LiveQuery.ChangeEvent event) {
                int numTimesCalled = 0;
                if (event.getError() != null) {
                    throw new RuntimeException(event.getError());
                }
                if (event.getRows().getCount() == 2) {
                    countDownLatch.countDown();
                }
            }
        });

        // kick off live query
        allDocsLiveQuery.start();

        // do pull replication against mock
        mockSinglePull(true, MockDispatcher.ServerType.SYNC_GW, true);

        // make sure we were called back with both docs
        boolean success = countDownLatch.await(30, TimeUnit.SECONDS);
        assertTrue(success);

        // clean up
        allDocsLiveQuery.stop();

    }


    public void testMockSinglePush() throws Exception {

        boolean shutdownMockWebserver = true;

        mockSinglePush(shutdownMockWebserver, MockDispatcher.ServerType.SYNC_GW);

    }


    /**
     * Do a push replication
     *
     * - Create docs in local db
     *   - One with no attachment
     *   - One with small attachment
     *   - One with large attachment
     *
     */

    public Map<String, Object> mockSinglePush(boolean shutdownMockWebserver, MockDispatcher.ServerType serverType) throws Exception {

        String doc1Id = "doc1";
        String doc2Id = "doc2";
        String doc3Id = "doc3";
        String doc4Id = "doc4";
        String doc2PathRegex = String.format("/db/%s.*", doc2Id);
        String doc3PathRegex = String.format("/db/%s.*", doc3Id);
        String doc2AttachName = "attachment.png";
        String doc3AttachName = "attachment2.png";
        String contentType = "image/png";

        // create mockwebserver and custom dispatcher
        MockDispatcher dispatcher = new MockDispatcher();
        MockWebServer server = MockHelper.getMockWebServer(dispatcher);
        dispatcher.setServerType(serverType);
        server.play();

        // add some documents
        Document doc1 = createDocumentForPushReplication(doc1Id, null, null);
        Document doc2 = createDocumentForPushReplication(doc2Id, doc2AttachName, contentType);
        Document doc3 = createDocumentForPushReplication(doc3Id, doc3AttachName, contentType);
        Document doc4 = createDocumentForPushReplication(doc4Id, null, null);
        doc4.delete();

        // checkpoint GET response w/ 404
        MockResponse fakeCheckpointResponse = new MockResponse();
        MockHelper.set404NotFoundJson(fakeCheckpointResponse);
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHECKPOINT, fakeCheckpointResponse);

        // _revs_diff response -- everything missing
        MockRevsDiff mockRevsDiff = new MockRevsDiff();
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_REVS_DIFF, mockRevsDiff);

        // _bulk_docs response -- everything stored
        MockBulkDocs mockBulkDocs = new MockBulkDocs();
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_BULK_DOCS, mockBulkDocs);

        // doc PUT responses for docs with attachments
        MockDocumentPut mockDoc2Put = new MockDocumentPut()
                .setDocId(doc2Id)
                .setRev(doc2.getCurrentRevisionId());
        dispatcher.enqueueResponse(doc2PathRegex, mockDoc2Put.generateMockResponse());
        MockDocumentPut mockDoc3Put = new MockDocumentPut()
                .setDocId(doc3Id)
                .setRev(doc3.getCurrentRevisionId());
        dispatcher.enqueueResponse(doc3PathRegex, mockDoc3Put.generateMockResponse());

        // run replication
        Replication replication = database.createPushReplication2(server.getUrl("/db"));
        replication.setContinuous(false);
        if (serverType != MockDispatcher.ServerType.SYNC_GW) {
            replication.setCreateTarget(true);
            Assert.assertTrue(replication.shouldCreateTarget());
        }
        runReplication2(replication);

        // make assertions about outgoing requests from replicator -> mock
        RecordedRequest getCheckpointRequest = dispatcher.takeRequest(MockHelper.PATH_REGEX_CHECKPOINT);
        assertTrue(getCheckpointRequest.getMethod().equals("GET"));
        assertTrue(getCheckpointRequest.getPath().matches(MockHelper.PATH_REGEX_CHECKPOINT));
        RecordedRequest revsDiffRequest = dispatcher.takeRequest(MockHelper.PATH_REGEX_REVS_DIFF);
        assertTrue(revsDiffRequest.getUtf8Body().contains(doc1Id));
        RecordedRequest bulkDocsRequest = dispatcher.takeRequest(MockHelper.PATH_REGEX_BULK_DOCS);
        assertTrue(bulkDocsRequest.getUtf8Body().contains(doc1Id));
        Map <String, Object> bulkDocsJson = Manager.getObjectMapper().readValue(bulkDocsRequest.getUtf8Body(), Map.class);
        Map <String, Object> doc4Map = MockBulkDocs.findDocById(bulkDocsJson, doc4Id);
        assertTrue(((Boolean)doc4Map.get("_deleted")).booleanValue() == true);

        assertFalse(bulkDocsRequest.getUtf8Body().contains(doc2Id));
        RecordedRequest doc2putRequest = dispatcher.takeRequest(doc2PathRegex);
        assertTrue(doc2putRequest.getUtf8Body().contains(doc2Id));
        assertFalse(doc2putRequest.getUtf8Body().contains(doc3Id));
        RecordedRequest doc3putRequest = dispatcher.takeRequest(doc3PathRegex);
        assertTrue(doc3putRequest.getUtf8Body().contains(doc3Id));
        assertFalse(doc3putRequest.getUtf8Body().contains(doc2Id));

        // wait until the mock webserver receives a PUT checkpoint request

        int expectedLastSequence = 5;
        List<RecordedRequest> checkpointRequests = waitForPutCheckpointRequestWithSequence(dispatcher, expectedLastSequence);

        // TODO: since this test's mock doesn't return valid responses to PUT checkpoint requests
        // TODO: the validation below does not work, since the second PUT checkpoint request
        // TODO: cannot pass in a _rev parameter.  Brings up the issue of verifying correct replicator
        // TODO: behavior in the scenario where the PUT checkpoint response is delayed by 30 seconds +.
        // TODO: uncomment -- validateCheckpointRequestsRevisions(checkpointRequests);

        // TODO: this is commented out because I'm seeing sporadic cases where there are 2 checkpoint requests
        // TODO: uncomment -- assertEquals(1, checkpointRequests.size());

        workAroundSaveCheckpointRaceCondition();

        // assert our local sequence matches what is expected
        String lastSequence = database.lastSequenceWithCheckpointId(replication.remoteCheckpointDocID());
        assertEquals(Integer.toString(expectedLastSequence), lastSequence);

        // assert completed count makes sense
        assertEquals(replication.getChangesCount(), replication.getCompletedChangesCount());

        // Shut down the server. Instances cannot be reused.
        if (shutdownMockWebserver) {
            server.shutdown();
        }

        Map<String, Object> returnVal = new HashMap<String, Object>();
        returnVal.put("server", server);
        returnVal.put("dispatcher", dispatcher);

        return returnVal;

    }

    private void workAroundSaveCheckpointRaceCondition() throws InterruptedException {

        // sleep a bit to give it a chance to save checkpoint to db
        Thread.sleep(2 * 1000);

    }

    /**
     * https://github.com/couchbase/couchbase-lite-java-core/issues/55
     */
    public void testContinuousPushReplicationGoesIdle() throws Exception {

        // make sure we are starting empty
        assertEquals(0, database.getLastSequenceNumber());

        // add docs
        Map<String,Object> properties1 = new HashMap<String,Object>();
        properties1.put("doc1", "testContinuousPushReplicationGoesIdle");
        final Document doc1 = createDocWithProperties(properties1);

        // create mockwebserver and custom dispatcher
        MockDispatcher dispatcher = new MockDispatcher();
        MockWebServer server = MockHelper.getMockWebServer(dispatcher);
        dispatcher.setServerType(MockDispatcher.ServerType.SYNC_GW);
        server.play();

        // checkpoint GET response w/ 404.  also receives checkpoint PUT's
        MockCheckpointPut mockCheckpointPut = new MockCheckpointPut();
        mockCheckpointPut.setSticky(true);
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHECKPOINT, mockCheckpointPut);

        // _revs_diff response -- everything missing
        MockRevsDiff mockRevsDiff = new MockRevsDiff();
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_REVS_DIFF, mockRevsDiff);

        // _bulk_docs response -- everything stored
        MockBulkDocs mockBulkDocs = new MockBulkDocs();
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_BULK_DOCS, mockBulkDocs);

        // replication to do initial sync up - has to be continuous replication so the checkpoint id
        // matches the next continuous replication we're gonna do later.

        Replication firstPusher = database.createPushReplication2(server.getUrl("/db"));
        firstPusher.setContinuous(true);
        final String checkpointId = firstPusher.remoteCheckpointDocID();  // save the checkpoint id for later usage

        // start the continuous replication
        CountDownLatch replicationIdleSignal = new CountDownLatch(1);
        ReplicationIdleObserver replicationIdleObserver = new ReplicationIdleObserver(replicationIdleSignal);
        firstPusher.addChangeListener(replicationIdleObserver);
        firstPusher.start();

        // wait until we get an IDLE event
        boolean successful = replicationIdleSignal.await(30, TimeUnit.SECONDS);
        assertTrue(successful);
        stopReplication2(firstPusher);

        // wait until replication does PUT checkpoint with lastSequence=1
        int expectedLastSequence = 1;
        waitForPutCheckpointRequestWithSeq(dispatcher, expectedLastSequence);

        // the last sequence should be "1" at this point.  we will use this later
        final String lastSequence = database.lastSequenceWithCheckpointId(checkpointId);
        assertEquals("1", lastSequence);

        // start a second continuous replication
        Replication secondPusher = database.createPushReplication2(server.getUrl("/db"));
        secondPusher.setContinuous(true);
        final String secondPusherCheckpointId = secondPusher.remoteCheckpointDocID();
        assertEquals(checkpointId, secondPusherCheckpointId);

        // remove current handler for the GET/PUT checkpoint request, and
        // install a new handler that returns the lastSequence from previous replication
        dispatcher.clearQueuedResponse(MockHelper.PATH_REGEX_CHECKPOINT);
        MockCheckpointGet mockCheckpointGet = new MockCheckpointGet();
        mockCheckpointGet.setLastSequence(lastSequence);
        mockCheckpointGet.setRev("0-2");
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHECKPOINT, mockCheckpointGet);

        // start second replication
        replicationIdleSignal = new CountDownLatch(1);
        replicationIdleObserver = new ReplicationIdleObserver(replicationIdleSignal);
        secondPusher.addChangeListener(replicationIdleObserver);
        secondPusher.start();

        // wait until we get an IDLE event
        successful = replicationIdleSignal.await(30, TimeUnit.SECONDS);
        assertTrue(successful);
        stopReplication2(secondPusher);

    }

    /**
     * https://github.com/couchbase/couchbase-lite-java-core/issues/241
     *
     * - Set the "retry time" to a short number
     * - Setup mock server to return 404 for all _changes requests
     * - Start continuous replication
     * - Sleep for 5X retry time
     * - Assert that we've received at least two requests to _changes feed
     * - Stop replication + cleanup
     *
     */
    public void testContinuousReplication404Changes() throws Exception {

        // create mockwebserver and custom dispatcher
        MockDispatcher dispatcher = new MockDispatcher();
        MockWebServer server = MockHelper.getMockWebServer(dispatcher);
        dispatcher.setServerType(MockDispatcher.ServerType.SYNC_GW);
        server.play();

        // mock checkpoint GET response w/ 404
        MockResponse fakeCheckpointResponse = new MockResponse();
        MockHelper.set404NotFoundJson(fakeCheckpointResponse);
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHECKPOINT, fakeCheckpointResponse);

        // mock _changes response
        for (int i=0; i<100; i++) {
            MockResponse mockChangesFeed = new MockResponse();
            MockHelper.set404NotFoundJson(mockChangesFeed);
            dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHANGES, mockChangesFeed);
        }

        // create new replication
        int retryDelaySeconds = 1;
        Replication pull = database.createPullReplication2(server.getUrl("/db"));
        pull.setContinuous(true);

        // add done listener to replication
        CountDownLatch replicationDoneSignal = new CountDownLatch(1);
        ReplicationFinishedObserver replicationFinishedObserver = new ReplicationFinishedObserver(replicationDoneSignal);
        pull.addChangeListener(replicationFinishedObserver);

        // start the replication
        pull.start();

        // wait until we get a few requests
        Log.d(TAG, "Waiting for a _changes request");
        RecordedRequest changesReq = dispatcher.takeRequestBlocking(MockHelper.PATH_REGEX_CHANGES);
        Log.d(TAG, "Got first _changes request, waiting for another _changes request");
        changesReq = dispatcher.takeRequestBlocking(MockHelper.PATH_REGEX_CHANGES);
        Log.d(TAG, "Got second _changes request, waiting for another _changes request");
        changesReq = dispatcher.takeRequestBlocking(MockHelper.PATH_REGEX_CHANGES);
        Log.d(TAG, "Got third _changes request, stopping replicator");

        // the replication should still be running
        assertEquals(1, replicationDoneSignal.getCount());

        // cleanup
        pull.stop();
        boolean success = replicationDoneSignal.await(60, TimeUnit.SECONDS);
        assertTrue(success);
        server.shutdown();


    }

    /**
     * Regression test for issue couchbase/couchbase-lite-android#174
     */
    public void testAllLeafRevisionsArePushed() throws Exception {

        final CustomizableMockHttpClient mockHttpClient = new CustomizableMockHttpClient();
        mockHttpClient.addResponderRevDiffsAllMissing();
        mockHttpClient.setResponseDelayMilliseconds(250);
        mockHttpClient.addResponderFakeLocalDocumentUpdate404();

        HttpClientFactory mockHttpClientFactory = new HttpClientFactory() {
            @Override
            public HttpClient getHttpClient() {
                return mockHttpClient;
            }

            @Override
            public void addCookies(List<Cookie> cookies) {

            }

            @Override
            public void deleteCookie(String name) {

            }

            @Override
            public CookieStore getCookieStore() {
                return null;
            }
        };
        manager.setDefaultHttpClientFactory(mockHttpClientFactory);

        Document doc = database.createDocument();
        SavedRevision rev1a = doc.createRevision().save();
        SavedRevision rev2a = createRevisionWithRandomProps(rev1a, false);
        SavedRevision rev3a = createRevisionWithRandomProps(rev2a, false);

        // delete the branch we've been using, then create a new one to replace it
        SavedRevision rev4a = rev3a.deleteDocument();
        SavedRevision rev2b = createRevisionWithRandomProps(rev1a, true);
        assertEquals(rev2b.getId(), doc.getCurrentRevisionId());

        // sync with remote DB -- should push both leaf revisions
        Replication push = database.createPushReplication2(getReplicationURL());

        runReplication2(push);
        assertNull(push.getLastError());

        // find the _revs_diff captured request and decode into json
        boolean foundRevsDiff = false;
        List<HttpRequest> captured = mockHttpClient.getCapturedRequests();
        for (HttpRequest httpRequest : captured) {

            if (httpRequest instanceof HttpPost) {
                HttpPost httpPost = (HttpPost) httpRequest;
                if (httpPost.getURI().toString().endsWith("_revs_diff")) {
                    foundRevsDiff = true;
                    Map<String, Object> jsonMap = CustomizableMockHttpClient.getJsonMapFromRequest(httpPost);

                    // assert that it contains the expected revisions
                    List<String> revisionIds = (List) jsonMap.get(doc.getId());
                    assertEquals(2, revisionIds.size());
                    assertTrue(revisionIds.contains(rev4a.getId()));
                    assertTrue(revisionIds.contains(rev2b.getId()));
                }

            }


        }
        assertTrue(foundRevsDiff);


    }


    /**
     * Verify that when a conflict is resolved on (mock) Sync Gateway
     * and a pull replication is done, the conflict is resolved locally.
     *
     * - Create local docs in conflict
     * - Simulate sync gw responses that resolve the conflict
     * - Do pull replication
     * - Assert conflict is resolved locally
     *
     */
    public void testRemoteConflictResolution() throws Exception {

        // Create a document with two conflicting edits.
        Document doc = database.createDocument();
        SavedRevision rev1 = doc.createRevision().save();
        SavedRevision rev2a = createRevisionWithRandomProps(rev1, false);
        SavedRevision rev2b = createRevisionWithRandomProps(rev1, true);

        // make sure we can query the db to get the conflict
        Query allDocsQuery = database.createAllDocumentsQuery();
        allDocsQuery.setAllDocsMode(Query.AllDocsMode.ONLY_CONFLICTS);
        QueryEnumerator rows = allDocsQuery.run();
        boolean foundDoc = false;
        assertEquals(1, rows.getCount());
        for (Iterator<QueryRow> it = rows; it.hasNext();) {
            QueryRow row = it.next();
            if (row.getDocument().getId().equals(doc.getId())) {
                foundDoc = true;
            }
        }
        assertTrue(foundDoc);

        // make sure doc in conflict
        assertTrue(doc.getConflictingRevisions().size() > 1);

        // create mockwebserver and custom dispatcher
        MockDispatcher dispatcher = new MockDispatcher();
        MockWebServer server = MockHelper.getMockWebServer(dispatcher);
        dispatcher.setServerType(MockDispatcher.ServerType.COUCHDB);

        // checkpoint GET response w/ 404
        MockResponse fakeCheckpointResponse = new MockResponse();
        MockHelper.set404NotFoundJson(fakeCheckpointResponse);
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHECKPOINT, fakeCheckpointResponse);

        int rev3PromotedGeneration = 3;
        String rev3PromotedDigest = "d46b";
        String rev3Promoted = String.format("%d-%s", rev3PromotedGeneration, rev3PromotedDigest);

        int rev3DeletedGeneration = 3;
        String rev3DeletedDigest = "e768";
        String rev3Deleted = String.format("%d-%s", rev3DeletedGeneration, rev3DeletedDigest);

        int seq = 4;

        // _changes response
        MockChangesFeed mockChangesFeed = new MockChangesFeed();
        MockChangesFeed.MockChangedDoc mockChangedDoc = new MockChangesFeed.MockChangedDoc();
        mockChangedDoc.setDocId(doc.getId());
        mockChangedDoc.setSeq(seq);
        mockChangedDoc.setChangedRevIds(Arrays.asList(rev3Promoted, rev3Deleted));
        mockChangesFeed.add(mockChangedDoc);
        MockResponse response = mockChangesFeed.generateMockResponse();
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHANGES, response);

        // docRev3Promoted response
        MockDocumentGet.MockDocument docRev3Promoted = new MockDocumentGet.MockDocument(doc.getId(), rev3Promoted, seq);
        docRev3Promoted.setJsonMap(MockHelper.generateRandomJsonMap());
        MockDocumentGet mockDocRev3PromotedGet = new MockDocumentGet(docRev3Promoted);
        Map<String, Object> rev3PromotedRevHistory = new HashMap<String, Object>();
        rev3PromotedRevHistory.put("start", rev3PromotedGeneration);
        List ids = Arrays.asList(
                rev3PromotedDigest,
                RevisionInternal.digestFromRevID(rev2a.getId()),
                RevisionInternal.digestFromRevID(rev2b.getId())
        );
        rev3PromotedRevHistory.put("ids", ids);
        mockDocRev3PromotedGet.setRevHistoryMap(rev3PromotedRevHistory);
        dispatcher.enqueueResponse(docRev3Promoted.getDocPathRegex(), mockDocRev3PromotedGet.generateMockResponse());

        // docRev3Deleted response
        MockDocumentGet.MockDocument docRev3Deleted = new MockDocumentGet.MockDocument(doc.getId(), rev3Deleted, seq);
        Map<String, Object> jsonMap = MockHelper.generateRandomJsonMap();
        jsonMap.put("_deleted", true);
        docRev3Deleted.setJsonMap(jsonMap);
        MockDocumentGet mockDocRev3DeletedGet = new MockDocumentGet(docRev3Deleted);
        Map<String, Object> rev3DeletedRevHistory = new HashMap<String, Object>();
        rev3DeletedRevHistory.put("start", rev3DeletedGeneration);
        ids = Arrays.asList(
                rev3DeletedDigest,
                RevisionInternal.digestFromRevID(rev2b.getId()),
                RevisionInternal.digestFromRevID(rev1.getId())
        );
        rev3DeletedRevHistory.put("ids", ids);
        mockDocRev3DeletedGet.setRevHistoryMap(rev3DeletedRevHistory);
        dispatcher.enqueueResponse(docRev3Deleted.getDocPathRegex(), mockDocRev3DeletedGet.generateMockResponse());

        // start mock server
        server.play();

        // run pull replication
        Replication pullReplication = database.createPullReplication2(server.getUrl("/db"));
        runReplication2(pullReplication);
        assertNull(pullReplication.getLastError());

        // assertions about outgoing requests
        RecordedRequest changesRequest = dispatcher.takeRequest(MockHelper.PATH_REGEX_CHANGES);
        assertNotNull(changesRequest);
        RecordedRequest docRev3DeletedRequest = dispatcher.takeRequest(docRev3Deleted.getDocPathRegex());
        assertNotNull(docRev3DeletedRequest);
        RecordedRequest docRev3PromotedRequest = dispatcher.takeRequest(docRev3Promoted.getDocPathRegex());
        assertNotNull(docRev3PromotedRequest);

        // Make sure the conflict was resolved locally.
        assertEquals(1, doc.getConflictingRevisions().size());

    }

    /**
     * https://github.com/couchbase/couchbase-lite-java-core/issues/95
     */
    public void testPushReplicationCanMissDocs() throws Exception {

        assertEquals(0, database.getLastSequenceNumber());

        Map<String,Object> properties1 = new HashMap<String,Object>();
        properties1.put("doc1", "testPushReplicationCanMissDocs");
        final Document doc1 = createDocWithProperties(properties1);

        Map<String,Object> properties2 = new HashMap<String,Object>();
        properties1.put("doc2", "testPushReplicationCanMissDocs");
        final Document doc2 = createDocWithProperties(properties2);

        UnsavedRevision doc2UnsavedRev = doc2.createRevision();
        InputStream attachmentStream = getAsset("attachment.png");
        doc2UnsavedRev.setAttachment("attachment.png", "image/png", attachmentStream);
        SavedRevision doc2Rev = doc2UnsavedRev.save();
        assertNotNull(doc2Rev);

        final CustomizableMockHttpClient mockHttpClient = new CustomizableMockHttpClient();
        mockHttpClient.addResponderFakeLocalDocumentUpdate404();
        mockHttpClient.setResponder("_bulk_docs", new CustomizableMockHttpClient.Responder() {
            @Override
            public HttpResponse execute(HttpUriRequest httpUriRequest) throws IOException {
                String json = "{\"error\":\"not_found\",\"reason\":\"missing\"}";
                return CustomizableMockHttpClient.generateHttpResponseObject(404, "NOT FOUND", json);
            }
        });

        mockHttpClient.setResponder(doc2.getId(), new CustomizableMockHttpClient.Responder() {
            @Override
            public HttpResponse execute(HttpUriRequest httpUriRequest) throws IOException {
                Map<String, Object> responseObject = new HashMap<String, Object>();
                responseObject.put("id", doc2.getId());
                responseObject.put("ok", true);
                responseObject.put("rev", doc2.getCurrentRevisionId());
                return CustomizableMockHttpClient.generateHttpResponseObject(responseObject);
            }
        });

        // create a replication obeserver to wait until replication finishes
        CountDownLatch replicationDoneSignal = new CountDownLatch(1);
        ReplicationFinishedObserver replicationFinishedObserver = new ReplicationFinishedObserver(replicationDoneSignal);

        // create replication and add observer
        manager.setDefaultHttpClientFactory(mockFactoryFactory(mockHttpClient));
        Replication pusher = database.createPushReplication2(getReplicationURL());
        pusher.addChangeListener(replicationFinishedObserver);

        // save the checkpoint id for later usage
        String checkpointId = pusher.remoteCheckpointDocID();

        // kick off the replication
        pusher.start();

        // wait for it to finish
        boolean success = replicationDoneSignal.await(60, TimeUnit.SECONDS);
        assertTrue(success);
        Log.d(TAG, "replicationDoneSignal finished");

        // we would expect it to have recorded an error because one of the docs (the one without the attachment)
        // will have failed.
        assertNotNull(pusher.getLastError());

        // workaround for the fact that the replicationDoneSignal.wait() call will unblock before all
        // the statements in Replication.stopped() have even had a chance to execute.
        // (specifically the ones that come after the call to notifyChangeListeners())
        Thread.sleep(500);

        String localLastSequence = database.lastSequenceWithCheckpointId(checkpointId);

        Log.d(TAG, "database.lastSequenceWithCheckpointId(): " + localLastSequence);
        Log.d(TAG, "doc2.getCurrentRevision().getSequence(): " + doc2.getCurrentRevision().getSequence());

        String msg = "Since doc1 failed, the database should _not_ have had its lastSequence bumped" +
                " to doc2's sequence number.  If it did, it's bug: github.com/couchbase/couchbase-lite-java-core/issues/95";
        assertFalse(msg, Long.toString(doc2.getCurrentRevision().getSequence()).equals(localLastSequence));
        assertNull(localLastSequence);
        assertTrue(doc2.getCurrentRevision().getSequence() > 0);


    }

    /**
     * https://github.com/couchbase/couchbase-lite-android/issues/66
     */

    public void testPushUpdatedDocWithoutReSendingAttachments() throws Exception {

        assertEquals(0, database.getLastSequenceNumber());

        Map<String,Object> properties1 = new HashMap<String,Object>();
        properties1.put("dynamic", 1);
        final Document doc = createDocWithProperties(properties1);
        SavedRevision doc1Rev = doc.getCurrentRevision();

        // Add attachment to document
        UnsavedRevision doc2UnsavedRev = doc.createRevision();
        InputStream attachmentStream = getAsset("attachment.png");
        doc2UnsavedRev.setAttachment("attachment.png", "image/png", attachmentStream);
        SavedRevision doc2Rev = doc2UnsavedRev.save();
        assertNotNull(doc2Rev);

        final CustomizableMockHttpClient mockHttpClient = new CustomizableMockHttpClient();

        mockHttpClient.addResponderFakeLocalDocumentUpdate404();

        // http://url/db/foo (foo==docid)
        mockHttpClient.setResponder(doc.getId(), new CustomizableMockHttpClient.Responder() {
            @Override
            public HttpResponse execute(HttpUriRequest httpUriRequest) throws IOException {
                Map<String, Object> responseObject = new HashMap<String, Object>();
                responseObject.put("id", doc.getId());
                responseObject.put("ok", true);
                responseObject.put("rev", doc.getCurrentRevisionId());
                return CustomizableMockHttpClient.generateHttpResponseObject(responseObject);
            }
        });

        // create replication and add observer
        manager.setDefaultHttpClientFactory(mockFactoryFactory(mockHttpClient));
        Replication pusher = database.createPushReplication2(getReplicationURL());

        runReplication2(pusher);

        List<HttpRequest> captured = mockHttpClient.getCapturedRequests();
        for (HttpRequest httpRequest : captured) {
            // verify that there are no PUT requests with attachments
            if (httpRequest instanceof HttpPut) {
                HttpPut httpPut = (HttpPut) httpRequest;
                HttpEntity entity=httpPut.getEntity();
                //assertFalse("PUT request with updated doc properties contains attachment", entity instanceof MultipartEntity);
            }
        }

        mockHttpClient.clearCapturedRequests();

        Document oldDoc =database.getDocument(doc.getId());
        UnsavedRevision aUnsavedRev = oldDoc.createRevision();
        Map<String,Object> prop = new HashMap<String,Object>();
        prop.putAll(oldDoc.getProperties());
        prop.put("dynamic", (Integer) oldDoc.getProperty("dynamic") +1);
        aUnsavedRev.setProperties(prop);
        final SavedRevision savedRev=aUnsavedRev.save();

        mockHttpClient.setResponder(doc.getId(), new CustomizableMockHttpClient.Responder() {
            @Override
            public HttpResponse execute(HttpUriRequest httpUriRequest) throws IOException {
                Map<String, Object> responseObject = new HashMap<String, Object>();
                responseObject.put("id", doc.getId());
                responseObject.put("ok", true);
                responseObject.put("rev", savedRev.getId());
                return CustomizableMockHttpClient.generateHttpResponseObject(responseObject);
            }
        });

        final String json = String.format("{\"%s\":{\"missing\":[\"%s\"],\"possible_ancestors\":[\"%s\",\"%s\"]}}",doc.getId(),savedRev.getId(),doc1Rev.getId(), doc2Rev.getId());
        mockHttpClient.setResponder("_revs_diff", new CustomizableMockHttpClient.Responder() {
            @Override
            public HttpResponse execute(HttpUriRequest httpUriRequest) throws IOException {
                return mockHttpClient.generateHttpResponseObject(json);
            }
        });

        pusher = database.createPushReplication2(getReplicationURL());
        runReplication2(pusher);


        captured = mockHttpClient.getCapturedRequests();
        for (HttpRequest httpRequest : captured) {
            // verify that there are no PUT requests with attachments
            if (httpRequest instanceof HttpPut) {
                HttpPut httpPut = (HttpPut) httpRequest;
                HttpEntity entity=httpPut.getEntity();
                assertFalse("PUT request with updated doc properties contains attachment", entity instanceof MultipartEntity);
            }
        }
    }

    /**
     * https://github.com/couchbase/couchbase-lite-java-core/issues/188
     */
    public void testServerDoesNotSupportMultipart() throws Exception {

        assertEquals(0, database.getLastSequenceNumber());

        Map<String,Object> properties1 = new HashMap<String,Object>();
        properties1.put("dynamic", 1);
        final Document doc = createDocWithProperties(properties1);
        SavedRevision doc1Rev = doc.getCurrentRevision();

        // Add attachment to document
        UnsavedRevision doc2UnsavedRev = doc.createRevision();
        InputStream attachmentStream = getAsset("attachment.png");
        doc2UnsavedRev.setAttachment("attachment.png", "image/png", attachmentStream);
        SavedRevision doc2Rev = doc2UnsavedRev.save();
        assertNotNull(doc2Rev);

        final CustomizableMockHttpClient mockHttpClient = new CustomizableMockHttpClient();

        mockHttpClient.addResponderFakeLocalDocumentUpdate404();

        Queue<CustomizableMockHttpClient.Responder> responders = new LinkedList<CustomizableMockHttpClient.Responder>();

        //first http://url/db/foo (foo==docid)
        //Reject multipart PUT with response code 415
        responders.add(new CustomizableMockHttpClient.Responder() {
            @Override
            public HttpResponse execute(HttpUriRequest httpUriRequest) throws IOException {
                String json = "{\"error\":\"Unsupported Media Type\",\"reason\":\"missing\"}";
                return CustomizableMockHttpClient.generateHttpResponseObject(415, "Unsupported Media Type", json);
            }
        });

        // second http://url/db/foo (foo==docid)
        // second call should be plain json, return good response
        responders.add(new CustomizableMockHttpClient.Responder() {
            @Override
            public HttpResponse execute(HttpUriRequest httpUriRequest) throws IOException {
                Map<String, Object> responseObject = new HashMap<String, Object>();
                responseObject.put("id", doc.getId());
                responseObject.put("ok", true);
                responseObject.put("rev", doc.getCurrentRevisionId());
                return CustomizableMockHttpClient.generateHttpResponseObject(responseObject);
            }
        });

        ResponderChain responderChain = new ResponderChain(responders);
        mockHttpClient.setResponder(doc.getId(), responderChain);

        // create replication and add observer
        manager.setDefaultHttpClientFactory(mockFactoryFactory(mockHttpClient));
        Replication pusher = database.createPushReplication2(getReplicationURL());

        runReplication2(pusher);

        List<HttpRequest> captured = mockHttpClient.getCapturedRequests();
        int entityIndex =0;
        for (HttpRequest httpRequest : captured) {
            // verify that there are no PUT requests with attachments
            if (httpRequest instanceof HttpPut) {
                HttpPut httpPut = (HttpPut) httpRequest;
                HttpEntity entity=httpPut.getEntity();
                if(entityIndex++ == 0) {
                    assertTrue("PUT request with attachment is not multipart", entity instanceof MultipartEntity);
                } else {
                    assertFalse("PUT request with attachment is multipart", entity instanceof MultipartEntity);
                }
            }
        }
    }


    public void testServerIsSyncGatewayVersion() {
        Replication pusher = database.createPushReplication2(getReplicationURL());
        assertFalse(pusher.serverIsSyncGatewayVersion("0.01"));
        pusher.setServerType("Couchbase Sync Gateway/0.93");
        assertTrue(pusher.serverIsSyncGatewayVersion("0.92"));
        assertFalse(pusher.serverIsSyncGatewayVersion("0.94"));
    }

    /**
     * https://github.com/couchbase/couchbase-lite-android/issues/243
     */
    public void testDifferentCheckpointsFilteredReplication() throws Exception {

        Replication pullerNoFilter = database.createPullReplication2(getReplicationURL());
        String noFilterCheckpointDocId = pullerNoFilter.remoteCheckpointDocID();

        Replication pullerWithFilter1 = database.createPullReplication2(getReplicationURL());
        pullerWithFilter1.setFilter("foo/bar");
        Map<String, Object> filterParams= new HashMap<String, Object>();
        filterParams.put("a", "aval");
        filterParams.put("b", "bval");
        pullerWithFilter1.setDocIds(Arrays.asList("doc3", "doc1", "doc2"));
        pullerWithFilter1.setFilterParams(filterParams);

        String withFilterCheckpointDocId = pullerWithFilter1.remoteCheckpointDocID();
        assertFalse(withFilterCheckpointDocId.equals(noFilterCheckpointDocId));

        Replication pullerWithFilter2 = database.createPullReplication2(getReplicationURL());
        pullerWithFilter2.setFilter("foo/bar");
        filterParams= new HashMap<String, Object>();
        filterParams.put("b", "bval");
        filterParams.put("a", "aval");
        pullerWithFilter2.setDocIds(Arrays.asList("doc2", "doc3", "doc1"));
        pullerWithFilter2.setFilterParams(filterParams);

        String withFilterCheckpointDocId2 = pullerWithFilter2.remoteCheckpointDocID();
        assertTrue(withFilterCheckpointDocId.equals(withFilterCheckpointDocId2));


    }

    public void testSetReplicationCookie() throws Exception {

        URL replicationUrl = getReplicationURL();
        Replication puller = database.createPullReplication2(replicationUrl);
        String cookieName = "foo";
        String cookieVal = "bar";
        boolean isSecure = false;
        boolean httpOnly = false;

        // expiration date - 1 day from now
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        int numDaysToAdd = 1;
        cal.add(Calendar.DATE, numDaysToAdd);
        Date expirationDate = cal.getTime();

        // set the cookie
        puller.setCookie(cookieName, cookieVal, "", expirationDate, isSecure, httpOnly);

        // make sure it made it into cookie store and has expected params
        CookieStore cookieStore = puller.getClientFactory().getCookieStore();
        List<Cookie> cookies = cookieStore.getCookies();
        assertEquals(1, cookies.size());
        Cookie cookie = cookies.get(0);
        assertEquals(cookieName, cookie.getName());
        assertEquals(cookieVal, cookie.getValue());
        assertEquals(replicationUrl.getHost(), cookie.getDomain());
        assertEquals(replicationUrl.getPath(), cookie.getPath());
        assertEquals(expirationDate, cookie.getExpiryDate());
        assertEquals(isSecure, cookie.isSecure());

        // add a second cookie
        String cookieName2 = "foo2";
        puller.setCookie(cookieName2, cookieVal, "", expirationDate, isSecure, false);
        assertEquals(2, cookieStore.getCookies().size());

        // delete cookie
        puller.deleteCookie(cookieName2);

        // should only have the original cookie left
        assertEquals(1, cookieStore.getCookies().size());
        assertEquals(cookieName, cookieStore.getCookies().get(0).getName());


    }

    /**
     * https://github.com/couchbase/couchbase-lite-android/issues/376
     *
     * This test aims to demonstrate that when the changes feed returns purged documents the
     * replicator is able to fetch all other documents but unable to finish the replication
     * (STOPPED OR IDLE STATE)
     */
    public void testChangesFeedWithPurgedDoc() throws Exception {
        //generate documents ids
        String doc1Id = "doc1-" + System.currentTimeMillis();
        String doc2Id = "doc2-" + System.currentTimeMillis();
        String doc3Id = "doc3-" + System.currentTimeMillis();

        //generate mock documents
        final MockDocumentGet.MockDocument mockDocument1 = new MockDocumentGet.MockDocument(
                doc1Id, "1-a", 1);
        mockDocument1.setJsonMap(MockHelper.generateRandomJsonMap());
        final MockDocumentGet.MockDocument mockDocument2 = new MockDocumentGet.MockDocument(
                doc2Id, "1-b", 2);
        mockDocument2.setJsonMap(MockHelper.generateRandomJsonMap());
        final MockDocumentGet.MockDocument mockDocument3 = new MockDocumentGet.MockDocument(
                doc3Id, "1-c", 3);
        mockDocument3.setJsonMap(MockHelper.generateRandomJsonMap());

        // create mockwebserver and custom dispatcher
        MockDispatcher dispatcher = new MockDispatcher();
        MockWebServer server = MockHelper.getMockWebServer(dispatcher);
        dispatcher.setServerType(MockDispatcher.ServerType.COUCHDB);

        //add response to _local request
        // checkpoint GET response w/ 404
        MockResponse fakeCheckpointResponse = new MockResponse();
        MockHelper.set404NotFoundJson(fakeCheckpointResponse);
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHECKPOINT, fakeCheckpointResponse);

        //add response to _changes request
        // _changes response
        MockChangesFeed mockChangesFeed = new MockChangesFeed();
        mockChangesFeed.add(new MockChangesFeed.MockChangedDoc(mockDocument1));
        mockChangesFeed.add(new MockChangesFeed.MockChangedDoc(mockDocument2));
        mockChangesFeed.add(new MockChangesFeed.MockChangedDoc(mockDocument3));
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHANGES, mockChangesFeed.generateMockResponse());

        // doc1 response
        MockDocumentGet mockDocumentGet1 = new MockDocumentGet(mockDocument1);
        dispatcher.enqueueResponse(mockDocument1.getDocPathRegex(), mockDocumentGet1.generateMockResponse());

        // doc2 missing reponse
        MockResponse missingDocumentMockResponse = new MockResponse();
        MockHelper.set404NotFoundJson(missingDocumentMockResponse);
        dispatcher.enqueueResponse(mockDocument2.getDocPathRegex(), missingDocumentMockResponse);

        // doc3 response
        MockDocumentGet mockDocumentGet3 = new MockDocumentGet(mockDocument3);
        dispatcher.enqueueResponse(mockDocument3.getDocPathRegex(), mockDocumentGet3.generateMockResponse());

        // checkpoint PUT response
        MockCheckpointPut mockCheckpointPut = new MockCheckpointPut();
        mockCheckpointPut.setSticky(true);
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHECKPOINT, mockCheckpointPut);

        // start mock server
        server.play();

        //create url for replication
        URL baseUrl = server.getUrl("/db");

        //create replication
        Replication pullReplication = database.createPullReplication2(baseUrl);
        pullReplication.setContinuous(false);

        //add change listener to notify when the replication is finished
        CountDownLatch replicationFinishedContCountDownLatch = new CountDownLatch(1);
        ReplicationFinishedObserver replicationFinishedObserver =
                new ReplicationFinishedObserver(replicationFinishedContCountDownLatch);
        pullReplication.addChangeListener(replicationFinishedObserver);

        //start replication
        pullReplication.start();

        boolean success = replicationFinishedContCountDownLatch.await(100, TimeUnit.SECONDS);
        assertTrue(success);

        if (pullReplication.getLastError() != null) {
            Log.d(TAG, "Replication had error: " + ((HttpResponseException) pullReplication.getLastError()).getStatusCode());
        }

        //assert document 1 was correctly pulled
        Document doc1 = database.getDocument(doc1Id);
        assertNotNull(doc1);
        assertNotNull(doc1.getCurrentRevision());

        //assert it was impossible to pull doc2
        Document doc2 = database.getDocument(doc2Id);
        assertNotNull(doc2);
        assertNull(doc2.getCurrentRevision());

        //assert it was possible to pull doc3
        Document doc3 = database.getDocument(doc3Id);
        assertNotNull(doc3);
        assertNotNull(doc3.getCurrentRevision());

        // wait until the replicator PUT's checkpoint with mockDocument3's sequence
        waitForPutCheckpointRequestWithSeq(dispatcher, mockDocument3.getDocSeq());

        workAroundSaveCheckpointRaceCondition();

        //last saved seq must be equal to last pulled document seq
        String doc3Seq = Integer.toString(mockDocument3.getDocSeq());
        String lastSequence = database.lastSequenceWithCheckpointId(pullReplication.remoteCheckpointDocID());
        assertEquals(doc3Seq, lastSequence);

        //stop mock server
        server.shutdown();

    }

    /**
     * https://github.com/couchbase/couchbase-lite-java-core/issues/253
     */
    public void testReplicationOnlineExtraneousChangeTrackers() throws Exception {
        throw new RuntimeException("needs porting");
    }

    public void testOneShotReplicationErrorNotification() throws Throwable {
        throw new RuntimeException("needs porting");
    }


    public void testContinuousReplicationErrorNotification() throws Throwable {
        throw new RuntimeException("needs porting");
    }

    /**
     * Reproduces https://github.com/couchbase/couchbase-lite-android/issues/167
     */
    public void testPushPurgedDoc() throws Throwable {

        int numBulkDocRequests = 0;
        HttpPost lastBulkDocsRequest = null;

        Map<String,Object> properties = new HashMap<String, Object>();
        properties.put("testName", "testPurgeDocument");

        Document doc = createDocumentWithProperties(database, properties);
        assertNotNull(doc);

        final CustomizableMockHttpClient mockHttpClient = new CustomizableMockHttpClient();
        mockHttpClient.addResponderRevDiffsAllMissing();
        mockHttpClient.setResponseDelayMilliseconds(250);
        mockHttpClient.addResponderFakeLocalDocumentUpdate404();

        HttpClientFactory mockHttpClientFactory = new HttpClientFactory() {
            @Override
            public HttpClient getHttpClient() {
                return mockHttpClient;
            }

            @Override
            public void addCookies(List<Cookie> cookies) {

            }

            @Override
            public void deleteCookie(String name) {

            }

            @Override
            public CookieStore getCookieStore() {
                return null;
            }
        };

        URL remote = getReplicationURL();

        manager.setDefaultHttpClientFactory(mockHttpClientFactory);
        Replication pusher = database.createPushReplication2(remote);
        pusher.setContinuous(true);

        final CountDownLatch replicationCaughtUpSignal = new CountDownLatch(1);

        pusher.addChangeListener(new Replication.ChangeListener() {
            @Override
            public void changed(Replication.ChangeEvent event) {
                final int changesCount = event.getSource().getChangesCount();
                final int completedChangesCount = event.getSource().getCompletedChangesCount();
                String msg = String.format("changes: %d completed changes: %d", changesCount, completedChangesCount);
                Log.d(TAG, msg);
                if (changesCount == completedChangesCount && changesCount != 0) {
                    replicationCaughtUpSignal.countDown();
                }
            }
        });

        pusher.start();

        // wait until that doc is pushed
        boolean didNotTimeOut = replicationCaughtUpSignal.await(60, TimeUnit.SECONDS);
        assertTrue(didNotTimeOut);

        // at this point, we should have captured exactly 1 bulk docs request
        numBulkDocRequests = 0;
        for (HttpRequest capturedRequest : mockHttpClient.getCapturedRequests()) {
            if (capturedRequest instanceof  HttpPost && ((HttpPost) capturedRequest).getURI().toString().endsWith("_bulk_docs")) {
                lastBulkDocsRequest = (HttpPost) capturedRequest;
                numBulkDocRequests += 1;
            }
        }
        assertEquals(1, numBulkDocRequests);

        // that bulk docs request should have the "start" key under its _revisions
        Map<String, Object> jsonMap = mockHttpClient.getJsonMapFromRequest((HttpPost) lastBulkDocsRequest);
        List docs = (List) jsonMap.get("docs");
        Map<String, Object> onlyDoc = (Map) docs.get(0);
        Map<String, Object> revisions = (Map) onlyDoc.get("_revisions");
        assertTrue(revisions.containsKey("start"));

        // now add a new revision, which will trigger the pusher to try to push it
        properties = new HashMap<String, Object>();
        properties.put("testName2", "update doc");
        UnsavedRevision unsavedRevision = doc.createRevision();
        unsavedRevision.setUserProperties(properties);
        unsavedRevision.save();

        // but then immediately purge it
        doc.purge();

        // wait for a while to give the replicator a chance to push it
        // (it should not actually push anything)
        Thread.sleep(5*1000);

        // we should not have gotten any more _bulk_docs requests, because
        // the replicator should not have pushed anything else.
        // (in the case of the bug, it was trying to push the purged revision)
        numBulkDocRequests = 0;
        for (HttpRequest capturedRequest : mockHttpClient.getCapturedRequests()) {
            if (capturedRequest instanceof  HttpPost && ((HttpPost) capturedRequest).getURI().toString().endsWith("_bulk_docs")) {
                numBulkDocRequests += 1;
            }
        }
        assertEquals(1, numBulkDocRequests);

        stopReplication2(pusher);


    }

    public static class ReplicationIdleObserver implements Replication.ChangeListener {

        private CountDownLatch doneSignal;

        public ReplicationIdleObserver(CountDownLatch doneSignal) {
            this.doneSignal = doneSignal;
        }

        @Override
        public void changed(Replication.ChangeEvent event) {

            if (event.getTransition() != null && event.getTransition().getDestination() == ReplicationState.IDLE) {
                doneSignal.countDown();
            }
        }

    }

    public static class ReplicationFinishedObserver implements Replication.ChangeListener {

        private CountDownLatch doneSignal;

        public ReplicationFinishedObserver(CountDownLatch doneSignal) {
            this.doneSignal = doneSignal;
        }

        @Override
        public void changed(Replication.ChangeEvent event) {

            if (event.getTransition() != null) {
                Log.d(TAG, "transition: %s", event.getTransition());
                if (event.getTransition().getDestination() == ReplicationState.STOPPED) {
                    doneSignal.countDown();
                }
            }
        }

    }

}
