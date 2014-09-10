package com.couchbase.lite.support;


import com.couchbase.lite.LiteTestCase;
import com.couchbase.lite.mockserver.MockCheckpointPut;
import com.couchbase.lite.mockserver.MockDispatcher;
import com.couchbase.lite.mockserver.MockHelper;
import com.couchbase.lite.mockserver.MockRevsDiff;
import com.couchbase.lite.mockserver.WrappedSmartMockResponse;
import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RemoteRequestTest extends LiteTestCase {

    /**
     * Make RemoteRequests will retry correctly.
     */
    public void testRetry() throws Exception {

        PersistentCookieStore cookieStore = database.getPersistentCookieStore();
        CouchbaseLiteHttpClientFactory factory = new CouchbaseLiteHttpClientFactory(cookieStore);

        // create mockwebserver and custom dispatcher
        MockDispatcher dispatcher = new MockDispatcher();
        MockWebServer server = MockHelper.getMockWebServer(dispatcher);
        dispatcher.setServerType(MockDispatcher.ServerType.SYNC_GW);

        // checkpoint GET response w/ 404 + respond to all PUT Checkpoint requests
        MockCheckpointPut mockCheckpointPut = new MockCheckpointPut();
        mockCheckpointPut.setSticky(true);
        dispatcher.enqueueResponse(MockHelper.PATH_REGEX_CHECKPOINT, mockCheckpointPut);

        server.play();

        String urlString = String.format("%s/%s", server.getUrl("/db"), "_local");
        URL url = new URL(urlString);

        Map<String, Object> requestBody = new HashMap<String, Object>();
        requestBody.put("foo", "bar");

        Map<String, Object> requestHeaders = new HashMap<String, Object>();

        int numRequests = 50;
        final CountDownLatch countDownLatch = new CountDownLatch(numRequests);
        RemoteRequestCompletionBlock completionBlock = new RemoteRequestCompletionBlock() {
            @Override
            public void onCompletion(Object result, Throwable e) {
                countDownLatch.countDown();
            }
        };

        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
        for (int i=0; i<numRequests; i++) {
            RemoteRequest request = new RemoteRequest(
                    executorService,
                    factory,
                    "GET",
                    url,
                    requestBody,
                    database,
                    requestHeaders,
                    completionBlock
            );
            Future future = executorService.submit(request);
        }

        boolean success = countDownLatch.await(30, TimeUnit.SECONDS);
        assertTrue(success);

    }



}
