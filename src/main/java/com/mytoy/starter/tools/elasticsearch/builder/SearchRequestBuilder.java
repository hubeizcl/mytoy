package com.mytoy.starter.tools.elasticsearch.builder;

import com.google.common.collect.Maps;
import com.mytoy.starter.tools.MyArrays;
import com.mytoy.starter.tools.MyString;
import com.mytoy.starter.tools.MyMap;
import com.mytoy.starter.tools.objectconvert.Map2ObjectUtils;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * 使用方法参照test的示例
 */
public class SearchRequestBuilder {

    private SearchRequest[] searchRequests;

    private MultiSearchRequest multiSearchRequest;

    public static SearchRequestBuilder builders() {
        return new SearchRequestBuilder();
    }

    public SearchRequestBuilder searchRequest(SearchRequest... searchRequest) {
        SearchRequest[] searchRequests = MyArrays.merge(this.searchRequests, searchRequest, SearchRequest.class);
        this.searchRequests = searchRequests;
        return this;
    }

    public MultiSearchResponse msearch(BiFunction<MultiSearchRequest, RequestOptions, MultiSearchResponse> function) {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        if (MyArrays.isNotEmpty(searchRequests))
            for (SearchRequest searchRequest : searchRequests) multiSearchRequest.add(searchRequest);
        return function.apply(multiSearchRequest, RequestOptions.DEFAULT);
    }

    public SearchResponse search(BiFunction<SearchRequest, RequestOptions, SearchResponse> function) {
        SearchRequest searchRequest = null;
        if (MyArrays.isNotEmpty(searchRequests)) searchRequest = searchRequests[0];
        return function.apply(searchRequest, RequestOptions.DEFAULT);
    }

    public long count(BiFunction<SearchRequest, RequestOptions, Long> function) {
        SearchRequest searchRequest = null;
        if (MyArrays.isNotEmpty(searchRequests)) searchRequest = searchRequests[0];
        return function.apply(searchRequest, RequestOptions.DEFAULT);
    }

    /**
     * 解析返回值
     */
    public static class Utils {

        private Long totalNum = 0l;

        private List<Map<String, Object>> resultMapList = new ArrayList<>();

        public static Utils builders() {
            return new Utils();
        }

        public Utils searchResponse(SearchResponse... searchResponses) {
            if (MyArrays.isNotEmpty(searchResponses)) {
                for (SearchResponse SearchResponse : searchResponses) {
                    SearchHits searchHits = SearchResponse.getHits();
                    if (null != searchHits) {
                        this.totalNum += searchHits.totalHits;
                        List<Map<String, Object>> collect = Arrays.stream(searchHits.getHits()).map(SearchHit::getSourceAsMap).collect(Collectors.toList());
                        this.resultMapList.addAll(collect);
                    }
                }
            }
            return this;
        }

        public Utils multiSearchResponse(MultiSearchResponse multiSearchResponse) {
            if (null != multiSearchResponse) {
                MultiSearchResponse.Item[] responses = multiSearchResponse.getResponses();
                List<SearchResponse> collect = Arrays.asList(responses).stream().filter(vo -> null != vo.getResponse() && !vo.isFailure()).map(vo -> vo.getResponse()).collect(Collectors.toList());
                SearchResponse[] searchResponses = MyArrays.toArray(collect, SearchResponse.class);
                this.searchResponse(searchResponses);
            }
            return this;
        }

        public List<Map<String, Object>> group(SearchResponse searchResponse, String aggFiledName) {
            List<Map<String, Object>> group = new ArrayList<>();
            Aggregation aggregation = searchResponse.getAggregations().getAsMap().get(aggFiledName);
            if (null != aggregation)
                group = ((ParsedTerms) aggregation).getBuckets().stream().map(vo -> MyMap.<String, Object>builder().of("key", vo.getKey()).of("doc_count", vo.getDocCount()).build()).collect(Collectors.toList());
            return group;
        }

        public <T> List<T> getBean(Class<T> clazz) {
            return getQueryResult(clazz);
        }



        public List<Map<String, Object>> resultMap() {
            return this.resultMapList;
        }

        public Map<String, Object> resultMapWarp() {
            Integer total = Integer.MAX_VALUE;
            if (totalNum < total) total = Math.toIntExact(totalNum);
            return MyMap.<String, Object>builder().of("total", total).of("list", resultMapList).build();
        }


        private <T> List<T> getQueryResult(Class<T> clazz) {
            return resultMapList.stream().filter(vo -> null != vo).map(vo -> Map2ObjectUtils.deepClone(vo, clazz)).collect(Collectors.toList());
        }
    }

    /**
     * 构造查询语句
     */
    public static class QueryDSLBuilders {

        public static class Aggregation {

            private static final int AGG_MAX_NUM = 10000;//聚合查询最大数量

            public static TermsAggregationBuilder getAggregation(String groupField) {
                TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders
                        .terms("sexprof")
                        .script(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "doc['" + groupField + "'].value", Maps.newHashMap()))
                        .size(AGG_MAX_NUM);
                return termsAggregationBuilder;
            }
        }

        private BoolQueryBuilder boolQueryBuilder;

        private SearchSourceBuilder sourceBuilder;

        private SearchRequest searchRequest;

        private Boolean isSeven;

        public SearchRequest build() {
            this.sourceBuilder.query(this.boolQueryBuilder);
            this.searchRequest.source(this.sourceBuilder);
            return searchRequest;
        }


        private QueryDSLBuilders() {
            boolQueryBuilder = org.elasticsearch.index.query.QueryBuilders.boolQuery();
            sourceBuilder = new SearchSourceBuilder();
            searchRequest = new SearchRequest();
        }

        public QueryDSLBuilders indices(String... indices) {
            this.searchRequest.indices(indices);
            return this;
        }

        public QueryDSLBuilders types(String... types) {
            if (!isSeven) this.searchRequest.types(types);
            return this;
        }

        public QueryDSLBuilders scroll(Scroll scroll) {
            this.searchRequest.scroll(scroll);
            return this;
        }

        public QueryDSLBuilders fetchSource(String... includes) {
            return fetchSource(includes, null);
        }

        public QueryDSLBuilders fetchSource(String[] includes, String... excludes) {
            this.sourceBuilder.fetchSource(includes, excludes);
            return this;
        }

        public QueryDSLBuilders from(Integer from) {
            if (null != from) this.sourceBuilder.from(from);
            return this;
        }

        public QueryDSLBuilders size(Integer size) {
            if (null != size) this.sourceBuilder.size(size);
            return this;
        }

        public QueryDSLBuilders aggregation(AggregationBuilder aggregationBuilder) {
            if (null != aggregationBuilder) this.sourceBuilder.aggregation(aggregationBuilder);
            return this;
        }


        public QueryDSLBuilders sort(FieldSortBuilder... fieldSortBuilders) {
            if (null != fieldSortBuilders && fieldSortBuilders.length > 0) {
                for (FieldSortBuilder fieldSortBuilder : fieldSortBuilders)
                    if (null != fieldSortBuilder) this.sourceBuilder.sort(fieldSortBuilder);
            }
            return this;
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

        public static QueryDSLBuilders builders(Boolean isSeven) {
            QueryDSLBuilders queryDSLBuilders = new QueryDSLBuilders();
            queryDSLBuilders.isSeven = isSeven;
            return queryDSLBuilders;
        }
    }


}
