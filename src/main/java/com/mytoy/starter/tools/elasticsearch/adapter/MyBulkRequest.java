package com.mytoy.starter.tools.elasticsearch.adapter;

import com.mytoy.starter.tools.MyArrays;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.unit.TimeValue;

import java.util.List;

public class MyBulkRequest {

    private BulkRequest bulkRequest;

    private boolean isSeven;

    private MyBulkRequest(Boolean isSeven) {
        this.isSeven = isSeven;
        bulkRequest = new BulkRequest();
    }

    public static MyBulkRequest builder(Boolean isSeven) {
        return new MyBulkRequest(isSeven);
    }

    public MyBulkRequest timeout(TimeValue timeout) {
        if (null != timeout) bulkRequest.timeout(timeout);
        return this;
    }

    public MyBulkRequest timeout(String timeout) {
        if (null != timeout) bulkRequest.timeout(timeout);
        return this;
    }

    public MyBulkRequest add(DocWriteRequest... docWriteRequest) {
        if (null != docWriteRequest && docWriteRequest.length > 0) bulkRequest.add(docWriteRequest);
        return this;
    }

    public MyBulkRequest add(List<DocWriteRequest> docWriteRequest) {
        if (null != docWriteRequest && docWriteRequest.size() > 0) {
            bulkRequest.add(MyArrays.toArray(docWriteRequest, DocWriteRequest.class));
        }
        return this;
    }

    public MyBulkRequest setRefreshPolicy(String refreshPolicy) {
        if (null != refreshPolicy) bulkRequest.setRefreshPolicy(refreshPolicy);
        return this;
    }

    public MyBulkRequest setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        if (null != refreshPolicy) bulkRequest.setRefreshPolicy(refreshPolicy);
        return this;
    }


    public MyBulkRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
        if (null != waitForActiveShards) bulkRequest.waitForActiveShards(waitForActiveShards);
        return this;
    }

    public MyBulkRequest defaultConfig() {
        this.timeout(TimeValue.timeValueMinutes(2))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                .waitForActiveShards(ActiveShardCount.ALL);
        return this;
    }

    public BulkRequest build() {
        return this.bulkRequest;
    }


}
