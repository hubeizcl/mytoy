package com.mytoy.starter.tools.elasticsearch.adapter;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * 适配SearchRequest
 */
public class MySearchRequest {
    private SearchRequest searchRequest;

    private boolean isSeven;

    public static MySearchRequest builder(Boolean isSeven) {
        return new MySearchRequest(isSeven);
    }

    private MySearchRequest(Boolean isSeven) {
        this.isSeven = isSeven;
        searchRequest = new SearchRequest();
    }

    public MySearchRequest indices(String... index) {
        this.searchRequest.indices(index);
        return this;
    }

    public MySearchRequest types(String... type) {
        if (!isSeven) {
            this.searchRequest.types(type);
        }
        return this;
    }

    public MySearchRequest source(SearchSourceBuilder searchSourceBuilder) {
        if (null != searchSourceBuilder) this.searchRequest.source(searchSourceBuilder);
        return this;
    }

    public SearchRequest build() {
        return this.searchRequest;
    }
}
