package com.mytoy.starter.tools.elasticsearch.adapter;

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.support.IndicesOptions;

public class MyGetIndexRequest {
    private static GetIndexRequest getIndexRequest;
    private boolean isSeven;

    public static MyGetIndexRequest builder(Boolean isSeven) {
        return new MyGetIndexRequest(isSeven);
    }

    private MyGetIndexRequest(Boolean isSeven) {
        this.isSeven = isSeven;
        this.getIndexRequest = new GetIndexRequest();
    }


    public MyGetIndexRequest indices(String... index) {
        this.getIndexRequest.indices(index);
        return this;

    }

    public MyGetIndexRequest type(String... type) {
        if (!isSeven) this.getIndexRequest.types(type);
        return this;

    }

    public MyGetIndexRequest includeDefaults(Boolean includeDefaults) {
        if (null != includeDefaults) this.getIndexRequest.includeDefaults(includeDefaults);
        return this;
    }

    public MyGetIndexRequest humanReadable(Boolean humanReadable) {
        if (null != humanReadable) this.getIndexRequest.humanReadable(humanReadable);
        return this;
    }

    public MyGetIndexRequest local(Boolean local) {
        if (null != local) this.getIndexRequest.local(local);
        return this;
    }

    public MyGetIndexRequest indicesOptions(IndicesOptions indicesOptions) {
        if (null != indicesOptions) this.getIndexRequest.indicesOptions(indicesOptions);
        return this;
    }

    public GetIndexRequest build() {
        return this.getIndexRequest;
    }
}