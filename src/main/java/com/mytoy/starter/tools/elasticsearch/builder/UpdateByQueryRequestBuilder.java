package com.mytoy.starter.tools.elasticsearch.builder;

import com.mytoy.starter.tools.MyString;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;

import java.util.function.BiFunction;

/**
 * 使用方法参照test的示例
 */
public class UpdateByQueryRequestBuilder {


    private UpdateByQueryRequest updateByQueryRequest;

    public static UpdateByQueryRequestBuilder builders() {
        return new UpdateByQueryRequestBuilder();
    }

    public UpdateByQueryRequestBuilder updateByQueryRequest(UpdateByQueryRequest updateByQueryRequest) {
        this.updateByQueryRequest = updateByQueryRequest;
        return this;
    }

    public long updateByQuery(BiFunction<UpdateByQueryRequest, RequestOptions, Long> function) {
        return function.apply(updateByQueryRequest, RequestOptions.DEFAULT);
    }

    public static class Utils {

    }

    public static class QueryDSLBuilders {
        private BoolQueryBuilder boolQueryBuilder;

        private UpdateByQueryRequest updateByQueryRequest;

        private boolean isSeven;

        public UpdateByQueryRequest builder() {
            return this.updateByQueryRequest;
        }

        public QueryDSLBuilders setScript(Script script) {
            if (null != script) updateByQueryRequest.setScript(script);
            return this;
        }

        public QueryDSLBuilders indices(String... indices) {
            if (null != indices) this.updateByQueryRequest.indices(indices);
            return this;
        }

        public QueryDSLBuilders types(String... types) {
            if (!isSeven) this.updateByQueryRequest.setDocTypes(types);
            return this;
        }

        public UpdateByQueryRequest build() {
            updateByQueryRequest.setQuery(boolQueryBuilder);
            return this.updateByQueryRequest;
        }


        public QueryDSLBuilders must(QueryBuilder... queryBuilders) {
            if (null != queryBuilders && queryBuilders.length > 0) {
                for (QueryBuilder queryBuilder : queryBuilders)
                    if (null != queryBuilder) this.boolQueryBuilder.must(queryBuilder);
            }
            return this;
        }

        public QueryDSLBuilders mustNot(QueryBuilder... queryBuilders) {
            if (null != queryBuilders && queryBuilders.length > 0) {
                for (QueryBuilder queryBuilder : queryBuilders)
                    if (null != queryBuilder) this.boolQueryBuilder.mustNot(queryBuilder);
            }
            return this;
        }

        public QueryDSLBuilders should(QueryBuilder... queryBuilders) {
            if (null != queryBuilders && queryBuilders.length > 0) {
                for (QueryBuilder queryBuilder : queryBuilders)
                    if (null != queryBuilder) {
                        String s = this.boolQueryBuilder.minimumShouldMatch();
                        if (MyString.isBlank(s)) this.boolQueryBuilder.minimumShouldMatch(1);
                        this.boolQueryBuilder.should(queryBuilder);
                    }
            }
            return this;
        }

        public QueryDSLBuilders filter(QueryBuilder... queryBuilders) {
            if (null != queryBuilders && queryBuilders.length > 0) {
                for (QueryBuilder queryBuilder : queryBuilders)
                    if (null != queryBuilder) this.boolQueryBuilder.filter(queryBuilder);
            }
            return this;
        }

        private QueryDSLBuilders() {
            this.boolQueryBuilder = org.elasticsearch.index.query.QueryBuilders.boolQuery();
            this.updateByQueryRequest = new UpdateByQueryRequest();
        }

        public static QueryDSLBuilders builders(Boolean isSeven) {
            QueryDSLBuilders queryDSLBuilders = new QueryDSLBuilders();
            queryDSLBuilders.isSeven = isSeven;
            return queryDSLBuilders;
        }
    }


}
