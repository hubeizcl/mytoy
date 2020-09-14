package com.mytoy.starter.tools.elasticsearch.adapter;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;

public class MyUpdateByQueryRequest {
    private static UpdateByQueryRequest updateByQueryRequest;
    private boolean isSeven;
    public static MyUpdateByQueryRequest builder(Boolean isSeven) {
        return new MyUpdateByQueryRequest(isSeven);
    }

    private MyUpdateByQueryRequest(Boolean isSeven) {
        this.isSeven=isSeven;
        this.updateByQueryRequest = new UpdateByQueryRequest();
    }

    public MyUpdateByQueryRequest indices(String... indices) {
        this.updateByQueryRequest.indices(indices);
        return this;
    }

    public MyUpdateByQueryRequest setDocTypes(String... types) {
        if (!isSeven) {
            this.updateByQueryRequest.setDocTypes(types);
        }
        return this;
    }

    public MyUpdateByQueryRequest setQuery(QueryBuilder query) {
        if (null != query) this.updateByQueryRequest.setQuery(query);
        return this;
    }

    public MyUpdateByQueryRequest setScript(Script script) {
        if (null != script) this.updateByQueryRequest.setScript(script);
        return this;
    }

    public UpdateByQueryRequest build() {
        return this.updateByQueryRequest;
    }
}
