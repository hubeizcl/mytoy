package com.mytoy.starter.tools.elasticsearch.builder;

import com.mytoy.starter.tools.MyString;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

import java.util.function.BiFunction;

/**
 * 使用方法参照test的示例
 */

public class DeleteByQueryRequestBuilder {

    private DeleteByQueryRequest deleteByQueryRequest;

    public static DeleteByQueryRequestBuilder builders() {
        return new DeleteByQueryRequestBuilder();
    }


    public DeleteByQueryRequestBuilder deleteByQueryRequest(DeleteByQueryRequest deleteByQueryRequest) {
        this.deleteByQueryRequest = deleteByQueryRequest;
        return this;
    }

    public long deleteByQuery(BiFunction<DeleteByQueryRequest, RequestOptions, Long> function) {
        return function.apply(deleteByQueryRequest, RequestOptions.DEFAULT);
    }


    public static class utils {

    }


    public static class QueryDSLBuilders {

        private BoolQueryBuilder boolQueryBuilder;

        private DeleteByQueryRequest deleteByQueryRequest;

        private Boolean isSeven;

        public QueryDSLBuilders defaultConfig() {
            this.deleteByQueryRequest.setConflicts("proceed");
            this.deleteByQueryRequest.setSize(10)
                    .setBatchSize(1000)
                    .setSlices(2)
                    .setScroll(TimeValue.timeValueMinutes(10))
                    .setTimeout(TimeValue.timeValueMinutes(2))
                    .setRefresh(true);
            return this;
        }

        public QueryDSLBuilders indices(String... indices) {
            this.deleteByQueryRequest.indices(indices);
            return this;
        }

        public QueryDSLBuilders types(String... types) {
            if (!isSeven) {
                this.deleteByQueryRequest.setDocTypes(types);
            }
            return this;
        }

        public DeleteByQueryRequest build() {
            deleteByQueryRequest.setQuery(boolQueryBuilder);
            return deleteByQueryRequest;
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
            boolQueryBuilder = org.elasticsearch.index.query.QueryBuilders.boolQuery();
            deleteByQueryRequest = new DeleteByQueryRequest();
        }

        public static QueryDSLBuilders builders(Boolean isSeven) {
            QueryDSLBuilders queryDSLBuilders = new QueryDSLBuilders();
            queryDSLBuilders.isSeven=isSeven;
            return queryDSLBuilders;
        }

    }
}
