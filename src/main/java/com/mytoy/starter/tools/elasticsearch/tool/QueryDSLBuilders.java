package com.mytoy.starter.tools.elasticsearch.tool;


import com.mytoy.starter.tools.MyArrays;
import com.mytoy.starter.tools.elasticsearch.adapter.MyDeleteByQueryRequest;
import com.mytoy.starter.tools.elasticsearch.adapter.MySearchRequest;
import com.mytoy.starter.tools.elasticsearch.adapter.MyUpdateByQueryRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Collection;

public class QueryDSLBuilders {

    private String index;

    private String type;

    private QueryBuilder[] mustQueryBuilders;//query语句最外层套接的must

    private QueryBuilder[] shouldQueryBuilders;//query语句最外层套接的should

    private QueryBuilder[] filterQueryBuilders;//query语句最外层套接的filter

    private QueryBuilder[] mustNotQueryBuilders;//query语句最外层套接的must_not

    private String[] includes = null;//需要返回的字段

    private String[] excludes = null;//不需要的字段

    private FieldSortBuilder[] fieldSortBuilders;

    private Integer from;

    private Integer size;

    private Scroll scroll;

    private Script script;

    private Boolean isSeven;

    private AggregationBuilder aggregationBuilder;

    private BoolQueryBuilder boolQuery;//用于保存拼接出来的query语句

    private QueryDSLBuilders() {
    }


    public static QueryDSLBuilders builder(Boolean isSeven) {
        QueryDSLBuilders queryDSLBuilders = new QueryDSLBuilders();
        queryDSLBuilders.isSeven = isSeven;
        return queryDSLBuilders;
    }

    public QueryDSLBuilders index(String index) {
        this.index = index;

        return this;
    }

    public QueryDSLBuilders type(String type) {
        this.index = type;
        return this;
    }


    public QueryDSLBuilders must(QueryBuilder... mustQueryBuilders) {
        QueryBuilder[] array = this.mustQueryBuilders;
        this.mustQueryBuilders = MyArrays.merge(QueryBuilder.class).apply(array).apply(mustQueryBuilders);
        return this;
    }

    public QueryDSLBuilders must(Collection<QueryBuilder> mustQueryBuilders) {
        this.must(MyArrays.toArray(mustQueryBuilders, QueryBuilder.class));
        return this;
    }

    public QueryDSLBuilders should(QueryBuilder... shouldQueryBuilders) {
        QueryBuilder[] array = this.shouldQueryBuilders;
        this.shouldQueryBuilders = MyArrays.merge(QueryBuilder.class).apply(array).apply(shouldQueryBuilders);
        return this;
    }

    public QueryDSLBuilders should(Collection<QueryBuilder> shouldQueryBuilders) {
        this.should(MyArrays.toArray(shouldQueryBuilders, QueryBuilder.class));
        return this;
    }


    public QueryDSLBuilders filter(QueryBuilder... filterQueryBuilders) {
        QueryBuilder[] array = this.filterQueryBuilders;
        this.filterQueryBuilders = MyArrays.merge(QueryBuilder.class).apply(array).apply(filterQueryBuilders);
        return this;
    }

    public QueryDSLBuilders filter(Collection<QueryBuilder> filterQueryBuilders) {
        this.filter(MyArrays.toArray(filterQueryBuilders, QueryBuilder.class));
        return this;
    }


    public QueryDSLBuilders mustNot(QueryBuilder... mustNotQueryBuilders) {
        QueryBuilder[] array = this.mustNotQueryBuilders;
        this.mustNotQueryBuilders = MyArrays.merge(QueryBuilder.class).apply(array).apply(mustNotQueryBuilders);
        return this;
    }

    public QueryDSLBuilders mustNot(Collection<QueryBuilder> mustNotQueryBuilders) {
        this.mustNot(MyArrays.toArray(mustNotQueryBuilders, QueryBuilder.class));
        return this;
    }

    public QueryDSLBuilders sort(FieldSortBuilder... fieldSortBuilders) {
        FieldSortBuilder[] array = this.fieldSortBuilders;
        this.fieldSortBuilders = MyArrays.merge(FieldSortBuilder.class).apply(array).apply(fieldSortBuilders);
        return this;
    }

    public QueryDSLBuilders sort(Collection<FieldSortBuilder> fieldSortBuilders) {
        this.sort(MyArrays.toArray(fieldSortBuilders, FieldSortBuilder.class));
        return this;
    }

    public QueryDSLBuilders sort(String filedName, SortOrder order) {
        FieldSortBuilder sortBuilder = SortBuilders.fieldSort(filedName).order(order);
        FieldSortBuilder[] array = new FieldSortBuilder[1];
        array[1] = sortBuilder;
        this.fieldSortBuilders = MyArrays.merge(FieldSortBuilder.class).apply(array).apply(fieldSortBuilders);
        return this;
    }

    public QueryDSLBuilders setScript(Script script) {
        this.script = script;
        return this;
    }

    public QueryDSLBuilders aggregation(AggregationBuilder aggregationBuilder) {
        this.aggregationBuilder = aggregationBuilder;
        return this;
    }

    public QueryDSLBuilders from(Integer from) {
        this.from = from;
        return this;
    }

    public QueryDSLBuilders size(Integer size) {
        this.size = size;
        return this;
    }

    public QueryDSLBuilders scroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    public QueryDSLBuilders includes(String... includes) {
        String[] array = this.includes;
        this.includes = MyArrays.merge(String.class).apply(array).apply(includes);
        return this;
    }

    public QueryDSLBuilders excludes(String... excludes) {
        String[] array = this.excludes;
        this.excludes = MyArrays.merge(String.class).apply(array).apply(excludes);
        return this;
    }

    public SearchRequest getSearchRequest() {
        this.createQuery();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolQuery);
        if (null != from) sourceBuilder.from(from);
        if (null != size) sourceBuilder.size(size);
        if (null != aggregationBuilder) sourceBuilder.aggregation(aggregationBuilder).from(0).size(0);
        SearchRequest searchRequest = MySearchRequest.builder(isSeven).indices(index).types(type).source(sourceBuilder).build();
        if (null != scroll) searchRequest.scroll(scroll);
        sourceBuilder.fetchSource(includes, excludes);
        return searchRequest;
    }

    public UpdateByQueryRequest getUpdateByQueryRequest() {
        this.createQuery();
        UpdateByQueryRequest updateByQueryRequest = MyUpdateByQueryRequest.builder(isSeven).indices(index).setDocTypes(type).setScript(script).setQuery(boolQuery).build();
        return updateByQueryRequest;
    }

    public DeleteByQueryRequest getDeleteByQueryRequest() {
        this.createQuery();
        DeleteByQueryRequest deleteByQueryRequest = MyDeleteByQueryRequest.builder(isSeven).indices(index).types(type).defaultConfig().setQuery(boolQuery).build();
        return deleteByQueryRequest;
    }

    public BoolQueryBuilder getBoolQuery() {
        this.createQuery();
        return this.boolQuery;
    }

    public void createQuery() {
        this.boolQuery = QueryBuilders.boolQuery();
        if (MyArrays.isNotEmpty(mustQueryBuilders))
            for (QueryBuilder vo : mustQueryBuilders) if (null != vo) boolQuery.must(vo);
        if (MyArrays.isNotEmpty(shouldQueryBuilders)) {
            for (QueryBuilder vo : shouldQueryBuilders)
                if (null != vo) boolQuery.should(vo);
            boolQuery.minimumShouldMatch(1);
        }
        if (MyArrays.isNotEmpty(mustNotQueryBuilders))
            for (QueryBuilder vo : mustNotQueryBuilders) if (null != vo) boolQuery.mustNot(vo);
        if (MyArrays.isNotEmpty(filterQueryBuilders))
            for (QueryBuilder vo : filterQueryBuilders) if (null != vo) boolQuery.filter(vo);
    }

}
