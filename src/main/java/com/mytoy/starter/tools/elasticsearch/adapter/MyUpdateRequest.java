package com.mytoy.starter.tools.elasticsearch.adapter;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;

/**
 * IndexRequest得适配类
 */
public class MyUpdateRequest {
    private static UpdateRequest updateRequest;
    private boolean isSeven;

    public static MyUpdateRequest builder(Boolean isSeven) {
        return new MyUpdateRequest(isSeven);
    }

    private MyUpdateRequest(Boolean isSeven) {
        this.isSeven = isSeven;
        this.updateRequest = new UpdateRequest();
    }


    public MyUpdateRequest index(String index) {
        this.updateRequest.index(index);
        return this;

    }

    public MyUpdateRequest type(String type) {
        if (!isSeven) this.updateRequest.type(type);
        return this;

    }

    public MyUpdateRequest id(String id) {
        this.updateRequest.id(id);
        return this;
    }

    public MyUpdateRequest script(Script script) {
        this.updateRequest.script(script);
        return this;
    }

    public UpdateRequest build() {
        return this.updateRequest;
    }
}
