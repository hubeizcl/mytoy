package com.mytoy.starter.tools.elasticsearch.adapter;

import org.elasticsearch.action.index.IndexRequest;

/**
 * IndexRequest得适配类
 */
public class MyIndexRequest {
    private static IndexRequest indexRequest;
    private boolean isSeven;

    public static MyIndexRequest builder(Boolean isSeven) {
        return new MyIndexRequest(isSeven);
    }

    private MyIndexRequest(Boolean isSeven) {
        this.isSeven = isSeven;
        this.indexRequest = new IndexRequest();
    }


    public MyIndexRequest index(String index) {
        this.indexRequest.index(index);
        return this;

    }

    public MyIndexRequest type(String type) {
        if (!isSeven) this.indexRequest.type(type);
        else this.indexRequest.type("_doc");
        return this;

    }

    public MyIndexRequest id(String id) {
        if (null != id) this.indexRequest.id(id);
        return this;
    }

    public IndexRequest build() {
        return this.indexRequest;
    }
}
