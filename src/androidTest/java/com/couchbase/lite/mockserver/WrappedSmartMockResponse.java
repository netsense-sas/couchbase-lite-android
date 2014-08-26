package com.couchbase.lite.mockserver;

import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.RecordedRequest;

public class WrappedSmartMockResponse implements SmartMockResponse {

    private MockResponse mockResponse;
    private long delayMs;


    public WrappedSmartMockResponse(MockResponse mockResponse) {
        this.mockResponse = mockResponse;
    }

    @Override
    public MockResponse generateMockResponse(RecordedRequest request) {
        return mockResponse;
    }

    @Override
    public boolean isSticky() {
        return false;
    }

    @Override
    public long delayMs() {
        return delayMs;
    }

    public void setDelayMs(long delayMs) {
        this.delayMs = delayMs;
    }
}
