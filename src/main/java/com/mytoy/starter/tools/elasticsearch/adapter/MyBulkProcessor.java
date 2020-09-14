package com.mytoy.starter.tools.elasticsearch.adapter;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

@Slf4j
public class MyBulkProcessor {

    private BulkProcessor.Builder builder;

    private DocWriteRequest[] docWriteRequests;

    private MyBulkProcessor(BulkProcessor.Listener listener) {
        builder = BulkProcessor.builder((bulkRequest, bulkResponseActionListener) -> {
        }, listener);
    }

    private MyBulkProcessor() {
        builder = BulkProcessor.builder((bulkRequest, bulkResponseActionListener) -> {
        }, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {

            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {

            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {

            }
        });
    }

    public static MyBulkProcessor builder(BulkProcessor.Listener listener) {
        return new MyBulkProcessor(listener);
    }

    public static MyBulkProcessor builder() {
        return new MyBulkProcessor();
    }

    public MyBulkProcessor setBulkActions(Integer bulkActions) {
        if (null != bulkActions) builder.setBulkActions(bulkActions);
        return this;
    }

    public MyBulkProcessor setBulkSize(ByteSizeValue bulkSize) {
        if (null != bulkSize) builder.setBulkSize(bulkSize);
        return this;
    }

    public MyBulkProcessor setFlushInterval(TimeValue flushInterval) {
        if (null != flushInterval) builder.setFlushInterval(flushInterval);
        return this;
    }


    public MyBulkProcessor setConcurrentRequests(Integer concurrentRequests) {
        if (null != concurrentRequests) builder.setConcurrentRequests(concurrentRequests);
        return this;
    }

    public MyBulkProcessor setBackoffPolicy(BackoffPolicy backoffPolicy) {
        if (null != backoffPolicy) builder.setBackoffPolicy(backoffPolicy);
        return this;
    }

    public MyBulkProcessor defaultConfig() {
        this.setBulkActions(10000)  // 1w次请求执行一次bulk
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))   // 1gb的数据刷新一次bulk
                .setFlushInterval(TimeValue.timeValueSeconds(30))   // 固定30s必须刷新一次
                .setConcurrentRequests(1) // 并发请求数量, 0不并发, 1并发允许执行
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)); // 设置退避, 100ms后执行, 最大请求3次
        return this;
    }

    public MyBulkProcessor add(DocWriteRequest... docWriteRequests) {
        this.docWriteRequests = docWriteRequests;
        return this;
    }


    public BulkProcessor build() {
        BulkProcessor bulkProcessor = builder.build();
        if (null != docWriteRequests && docWriteRequests.length > 0)
            for (DocWriteRequest docWriteRequest : docWriteRequests) bulkProcessor.add(docWriteRequest);
        return bulkProcessor;
    }
}
