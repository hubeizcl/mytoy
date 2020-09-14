package com.mytoy.starter.tools.elasticsearch.adapter;

import org.elasticsearch.action.get.MultiGetRequest;

public class MyMultiGetRequest {

    private MultiGetRequest multiGetRequest;
    private boolean isSeven;

    public static MyMultiGetRequest builder(Boolean isSeven) {
        return new MyMultiGetRequest(isSeven);
    }

    private MyMultiGetRequest(Boolean isSeven) {
        this.isSeven = isSeven;
        this.multiGetRequest = new MultiGetRequest();
    }

    public MyMultiGetRequest add(MultiGetRequest.Item... items) {
        if (null != items && items.length > 0) for (MultiGetRequest.Item item : items) multiGetRequest.add(item);
        return this;
    }

    public MultiGetRequest build() {
        return multiGetRequest;
    }

}
