package com.mytoy.starter.tools.elasticsearch.adapter;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.unit.TimeValue;

public class MyGetMappingsRequest {
    private static GetMappingsRequest getMappingsRequest;
    private boolean isSeven;

    public static MyGetMappingsRequest builder(Boolean isSeven) {
        return new MyGetMappingsRequest(isSeven);
    }

    private MyGetMappingsRequest(Boolean isSeven) {
        this.isSeven = isSeven;
        this.getMappingsRequest = new GetMappingsRequest();
    }

    public MyGetMappingsRequest masterNodeTimeout(TimeValue timeout) {
        if (null != timeout) this.getMappingsRequest.masterNodeTimeout(timeout);
        return this;

    }

    public MyGetMappingsRequest indicesOptions(IndicesOptions indicesOptions) {
        if (null != indicesOptions) this.getMappingsRequest.indicesOptions(indicesOptions);
        return this;

    }

    public MyGetMappingsRequest index(String... indices) {
//        if (MyArrays.isNotEmpty(indices)) this.getMappingsRequest.indices(indices);
        return this;
    }


    public MyGetMappingsRequest type(String type) {
        if (!isSeven) this.getMappingsRequest.types(type);
        return this;

    }

    public MyGetMappingsRequest defaultConfig() {
        this.masterNodeTimeout(TimeValue.timeValueMinutes(1));
        this.indicesOptions(IndicesOptions.lenientExpandOpen());
        return this;
    }

    public GetMappingsRequest build() {
        return this.getMappingsRequest;
    }
}
