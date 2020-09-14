package com.mytoy.starter.tools.elasticsearch.adapter;

import org.apache.commons.collections.MapUtils;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.util.Map;

public class MyPutMappingRequest {
    private static PutMappingRequest putMappingRequest;
    private boolean isSeven;

    public static MyPutMappingRequest builder(Boolean isSeven) {
        return new MyPutMappingRequest(isSeven);
    }

    private MyPutMappingRequest(Boolean isSeven) {
        this.isSeven = isSeven;
        this.putMappingRequest = new PutMappingRequest();
    }

    public MyPutMappingRequest timeout(TimeValue timeout) {
        if (null != timeout) this.putMappingRequest.timeout(timeout);
        return this;

    }

    public MyPutMappingRequest masterNodeTimeout(TimeValue masterNodeTimeout) {
        if (null != masterNodeTimeout) this.putMappingRequest.masterNodeTimeout(masterNodeTimeout);
        return this;

    }

    public MyPutMappingRequest source(XContentBuilder xContentBuilder) {
        if (null != xContentBuilder) this.putMappingRequest.source(xContentBuilder);
        return this;

    }

    public MyPutMappingRequest source(Map<String, Object> map) {
        if (MapUtils.isNotEmpty(map)) this.putMappingRequest.source(map);
        return this;

    }


    public MyPutMappingRequest index(String... index) {
        this.putMappingRequest.indices(index);
        return this;

    }

    public MyPutMappingRequest type(String type) {
        if (!isSeven) this.putMappingRequest.type(type);
        return this;

    }

    public MyPutMappingRequest defaultConfig() {
        this.timeout(TimeValue.timeValueMinutes(2))
                .masterNodeTimeout(TimeValue.timeValueMinutes(1));
        return this;
    }

    public PutMappingRequest build() {
        return this.putMappingRequest;
    }
}
