package com.mytoy.starter.tools.elasticsearch.adapter;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

public class MyDeleteByQueryRequest {
    private DeleteByQueryRequest deleteByQueryRequest;

    private boolean isSeven;

    public static MyDeleteByQueryRequest builder(Boolean isSeven) {
        return new MyDeleteByQueryRequest(isSeven);
    }

    private MyDeleteByQueryRequest(Boolean isSeven) {
        deleteByQueryRequest = new DeleteByQueryRequest();
        this.isSeven = isSeven;
    }

    public MyDeleteByQueryRequest setConflicts(String conflicts) {
        deleteByQueryRequest.setConflicts(conflicts);
        return this;
    }

    public MyDeleteByQueryRequest setSize(Integer size) {
        if (null != size) deleteByQueryRequest.setSize(size);
        return this;
    }

    public MyDeleteByQueryRequest setBatchSize(Integer size) {
        if (null != size) deleteByQueryRequest.setBatchSize(size);
        return this;
    }


    public MyDeleteByQueryRequest setSlices(Integer slices) {
        if (null != slices) deleteByQueryRequest.setSlices(slices);
        return this;
    }

    public MyDeleteByQueryRequest setScroll(TimeValue scroll) {
        if (null != scroll) deleteByQueryRequest.setScroll(scroll);
        return this;
    }


    public MyDeleteByQueryRequest setTimeout(TimeValue timeout) {
        if (null != timeout) deleteByQueryRequest.setTimeout(timeout);
        return this;
    }

    public MyDeleteByQueryRequest setRefresh(Boolean flag) {
        if (null != flag) deleteByQueryRequest.setRefresh(flag);
        return this;
    }


    public MyDeleteByQueryRequest indices(String... index) {
        this.deleteByQueryRequest.indices(index);
        return this;
    }

    public MyDeleteByQueryRequest types(String... type) {
        if (!isSeven) this.deleteByQueryRequest.setDocTypes(type);
        return this;
    }

    public MyDeleteByQueryRequest setQuery(QueryBuilder queryBuilder) {
        if (null != queryBuilder) this.deleteByQueryRequest.setQuery(queryBuilder);
        return this;
    }


    public MyDeleteByQueryRequest defaultConfig() {
        this.deleteByQueryRequest.setConflicts("proceed");
        this.deleteByQueryRequest.setSize(10).setBatchSize(1000).setSlices(2).setScroll(TimeValue.timeValueMinutes(10)).setTimeout(TimeValue.timeValueMinutes(2)).setRefresh(true);
        return this;
    }

    public DeleteByQueryRequest build() {
        return this.deleteByQueryRequest;
    }
}
