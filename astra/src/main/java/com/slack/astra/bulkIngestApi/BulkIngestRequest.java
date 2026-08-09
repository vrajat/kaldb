package com.slack.astra.bulkIngestApi;

import com.slack.service.murron.trace.Trace;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Wrapper object to enable building a bulk request and awaiting on an asynchronous response to be
 * populated. The response channel buffers exactly one result so the producer thread can finish
 * before the waiting caller starts consuming it.
 */
public class BulkIngestRequest {
  private final Map<String, List<Trace.Span>> inputDocs;
  private final BlockingQueue<BulkIngestResponse> internalResponse = new ArrayBlockingQueue<>(1);

  protected BulkIngestRequest(Map<String, List<Trace.Span>> inputDocs) {
    this.inputDocs = inputDocs;
  }

  Map<String, List<Trace.Span>> getInputDocs() {
    return inputDocs;
  }

  boolean setResponse(BulkIngestResponse response) {
    return internalResponse.offer(response);
  }

  public BulkIngestResponse getResponse() throws InterruptedException {
    return internalResponse.take();
  }
}
