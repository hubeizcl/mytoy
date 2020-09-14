package com.mytoy.starter.tools.esquery;

import com.alibaba.fastjson.JSON;
import com.carrotsearch.hppc.ObjectLookupContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.Lists;
import com.mytoy.starter.tools.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.*;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.rankeval.RankEvalRequest;
import org.elasticsearch.index.reindex.*;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.PutWatchRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.mustache.MultiSearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.tasks.TaskId;
import org.javatuples.Pair;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;


@Slf4j
public class ESQueryExecute {

    GenericObjectPool<RestHighLevelClient> genericObjectPool;

    String hosts;

    Boolean isSeven;

    /**
     * 开关:打印es错误的堆栈信息
     */
    Boolean printStackTraceButton = true;

    /**
     * 开关:将所有的es的query查询转换为filter查询
     */
    Boolean switchFilterButton = false;

    /**
     * 开关:可以将es查询语句中的一些字段错误按简单的规则做一些转化，避免报错，这个要损失一些性能
     */
    Boolean esQueryCheckButton = false;

    /**
     * 开关:是否使用简单的转换方法对es查询语句中的字段进行转换
     */
    Boolean commConvertButton = false;

    RestClientBuilder restClientBuilder;

    Integer esRequestErrorCode = 10000;

    private Map<String, Set<String>> indexMapping = new HashMap<>();

    public static class ParseTools extends RestHighLevelClient {

        public ParseTools(RestClientBuilder restClientBuilder) {
            super(restClientBuilder);
        }

        public SearchResponse parseEntity(Response response) throws IOException {
            CheckedFunction checkedFunction = parser -> SearchResponse.fromXContent((XContentParser) parser);
            return (SearchResponse) this.parseEntity(response.getEntity(), checkedFunction);
        }
    }

    public static class Builders {

        ESQueryExecute esQueryExecute;

        private Builders() {
            esQueryExecute = new ESQueryExecute();
        }

        public static Builders builder() {
            return new Builders();
        }

        public Builders genericObjectPool(GenericObjectPool<RestHighLevelClient> genericObjectPool) {
            esQueryExecute.genericObjectPool = genericObjectPool;
            return this;
        }

        public Builders hosts(String hosts) {
            esQueryExecute.hosts = hosts;
            return this;
        }

        public Builders switchFilterButton(Boolean switchFilterButton) {
            esQueryExecute.switchFilterButton = switchFilterButton;
            return this;
        }

        public Builders esQueryCheckButton(Boolean esQueryCheckButton) {
            esQueryExecute.esQueryCheckButton = esQueryCheckButton;
            return this;
        }

        public Builders printStackTraceButton(Boolean printStackTraceButton) {
            esQueryExecute.printStackTraceButton = printStackTraceButton;
            return this;
        }

        public Builders commConvertButton(Boolean commConvertButton) {
            esQueryExecute.commConvertButton = commConvertButton;
            return this;
        }

        public Builders restClientBuilder(RestClientBuilder restClientBuilder) {
            esQueryExecute.restClientBuilder = restClientBuilder;
            return this;
        }

        public ESQueryExecute build() {
            esQueryExecute.isSeven();
            return esQueryExecute;
        }
    }


    public static class Tools {

        public static final Function<LocalDateTime, String> printEsQueryTime = start -> {
            String runTime = "";
            if (null != start) {
                Duration duration = Duration.between(start, LocalDateTime.now(Clock.systemDefaultZone()));
                runTime = "耗时" + duration.toMillis() + "毫秒";
            }
            return runTime;
        };

        private static final Function<SearchSourceBuilder, BoolQueryBuilder> getBoolQueryBuilder = source -> {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            QueryBuilder query = source.query();
            QueryBuilder filter = source.postFilter();
            if (null != query) boolQueryBuilder.filter(query);
            if (null != filter) boolQueryBuilder.filter(filter);
            return boolQueryBuilder;
        };

        ESQueryExecute esQueryExecute;

        Boolean switchFilterButton = false;

        Boolean esQueryCheckButton = false;

        Boolean commConvertButton = false;

        private Tools() {
        }

        public static Tools builder() {
            return new Tools();
        }

        public Tools esQueryExecute(ESQueryExecute esQueryExecute) {
            this.esQueryExecute = esQueryExecute;
            return this;
        }

        public Tools switchFilterButton(Boolean switchFilterButton) {
            this.switchFilterButton = switchFilterButton;
            return this;
        }

        public Tools esQueryCheckButton(Boolean esQueryCheckButton) {
            this.esQueryCheckButton = esQueryCheckButton;
            return this;
        }

        public Tools commConvertButton(Boolean commConvertButton) {
            this.commConvertButton = commConvertButton;
            return this;
        }

        private void checkQuery(SearchRequest searchRequest) {
            SearchSourceBuilder source = searchRequest.source();
            BoolQueryBuilder newBoolQueryBuilder = CheckAndSimpleFix.builder().searchRequest(searchRequest, esQueryExecute::getIndexMapping).commConvert(commConvertButton).build();
            source.query(newBoolQueryBuilder);
        }

        public SearchRequest count(SearchRequest searchRequest) {
            if (esQueryCheckButton) checkQuery(searchRequest);
            if (switchFilterButton) {
                SearchSourceBuilder source = searchRequest.source();
                source.query(getBoolQueryBuilder.apply(source));
                searchRequest.source(source);
            }
            return searchRequest;
        }

        public SearchRequest search(SearchRequest searchRequest) {
            if (esQueryCheckButton) checkQuery(searchRequest);
            if (switchFilterButton) {
                SearchSourceBuilder source = searchRequest.source();
                source.query(getBoolQueryBuilder.apply(source));
                searchRequest.source(source);
            }
            return searchRequest;
        }

        public DeleteByQueryRequest deleteByQuery(DeleteByQueryRequest deleteByQueryRequest) {
            if (esQueryCheckButton) checkQuery(deleteByQueryRequest.getSearchRequest());
            if (switchFilterButton)
                deleteByQueryRequest.setQuery(getBoolQueryBuilder.apply(deleteByQueryRequest.getSearchRequest().source()));
            return deleteByQueryRequest;
        }

        public UpdateByQueryRequest updateByQuery(UpdateByQueryRequest updateRequest) {
            if (esQueryCheckButton) checkQuery(updateRequest.getSearchRequest());
            if (switchFilterButton)
                updateRequest.setQuery(getBoolQueryBuilder.apply(updateRequest.getSearchRequest().source()));
            return updateRequest;
        }
    }

    private RestHighLevelClient getTheConnectionObject() throws Exception {
        return genericObjectPool.borrowObject();
    }

    private <T> T returnObjectsToTheConnectionPool(RestHighLevelClient restHighLevelClient, T t) {
        try {
            return t;
        } finally {
            if (null != restHighLevelClient) genericObjectPool.returnObject(restHighLevelClient);
        }
    }

    private void returnObjectsToTheConnectionPool(RestHighLevelClient restHighLevelClient) {
        try {
        } finally {
            genericObjectPool.returnObject(restHighLevelClient);
        }
    }

    public void setSeven(Boolean seven) {
        isSeven = seven;
    }

    public Boolean getSeven() {
        return isSeven;
    }

    private Set<String> getIndexMapping(String index, String type) {
        Set<String> set = indexMapping.get(index);
        if (null == set || set.size() == 0) {
            GetAliasesResponse aliasesResponse = getAlias(new GetAliasesRequest(index), RequestOptions.DEFAULT);
            Set<String> indices = new HashSet<>();
            if (null != aliasesResponse) indices = aliasesResponse.getAliases().keySet();
            if (null != indices && indices.size() > 1) {
                Iterator<String> iterator = indices.iterator();
                while (iterator.hasNext()) {
                    List<Pair<String, Set<String>>> pairs = getMapping(iterator.next(), type);
                    Set<String> sets = new HashSet<>();
                    pairs.stream().forEach(vo -> {
                        sets.add(vo.getValue0());
                        indexMapping.put(vo.getValue0(), vo.getValue1());
                    });
                    if (MyCollection.isNotEmpty(pairs)) {
                        Pair<String, Set<String>> pair = pairs.get(0);
                        indices.removeAll(sets);
                        indices.forEach(vo -> indexMapping.put(vo, pair.getValue1()));
                    }
                    break;
                }
            }
            List<Pair<String, Set<String>>> pairs = getMapping(index, type);
            pairs.stream().forEach(vo -> indexMapping.put(vo.getValue0(), vo.getValue1()));
        }
        set = indexMapping.get(index);
        return set;
    }

    private List<Pair<String, Set<String>>> getMapping(String index, String type) {
        List<Pair<String, Set<String>>> list = new ArrayList<>();
        try {
            if (MyString.isNotBlank(index)) {
                GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
                getMappingsRequest.indices(index);
                if (!isSeven) getMappingsRequest.types(type);
                getMappingsRequest.masterNodeTimeout(TimeValue.timeValueMinutes(1));
                getMappingsRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
                GetMappingsResponse getMappingResponse = getMapping(getMappingsRequest, RequestOptions.DEFAULT);
                ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> allMappings = getMappingResponse.mappings();
                ObjectLookupContainer<String> keys = allMappings.keys();
                int i = 0;
                for (ObjectCursor<String> key : keys) {
                    String trueIndexName = key.value;
                    Map<String, Object> mapping = allMappings.get(trueIndexName).get(type).sourceAsMap();
                    Set<String> set;
                    if (MyMap.isNotEmpty(mapping)) {
                        Object properties = mapping.get("properties");
                        if (properties instanceof Map) {
                            Map<String, Object> map = (Map<String, Object>) properties;
                            set = map.keySet();
                            list.add(new Pair(trueIndexName, set));
                            if (i < 1) {
                                list.add(new Pair(index, set));
                            }
                        }
                    }
                    i++;
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return list;
    }

    public boolean isSeven() {
        try {
            RestHighLevelClient restHighLevelClient = genericObjectPool.borrowObject();
            MainResponse mainResponse = restHighLevelClient.info(RequestOptions.DEFAULT);
            genericObjectPool.returnObject(restHighLevelClient);
            String version = mainResponse.getVersion().toString();
            return StringUtils.isNotBlank(version) && version.startsWith("7");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }



    public SearchResponse search(SearchRequest searchRequest, RequestOptions options) {
        searchRequest = Tools.builder().esQueryExecute(this).switchFilterButton(switchFilterButton).esQueryCheckButton(esQueryCheckButton).commConvertButton(commConvertButton).search(searchRequest);
        RestHighLevelClient restHighLevelClient = null;
        try {
            restHighLevelClient = getTheConnectionObject();
            String[] indices = searchRequest.indices();
            String[] types = searchRequest.types();
            Scroll scroll = searchRequest.scroll();
            SearchSourceBuilder source = searchRequest.source();
            log.info(Curl4ESOperator.builders().hostList(hosts).indices(indices).types(types).queryStr(source.toString()).search().build());
            LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
            if (isSeven) {
                Request request = RequestConverters.search(searchRequest, isSeven);
                Response response = restHighLevelClient.getLowLevelClient().performRequest(request);
                ParseTools parseTools = new ParseTools(restClientBuilder);
                SearchResponse searchResponse = parseTools.parseEntity(response);
                log.info("成功调起es查询操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
                log.debug("查询结果{}", searchResponse);
                return returnObjectsToTheConnectionPool(restHighLevelClient, searchResponse);
            } else {
                SearchResponse searchResponse = restHighLevelClient.search(searchRequest, options);
                String scrollId = searchResponse.getScrollId();
                log.info("成功调起es查询操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
                log.debug("查询结果{}", searchResponse);
                return returnObjectsToTheConnectionPool(restHighLevelClient, searchResponse);
            }
        } catch (Exception e) {
            if (printStackTraceButton) e.printStackTrace();
            log.error("es查询失败，原因 {}", e.getMessage());
            throw returnObjectsToTheConnectionPool(restHighLevelClient, new NewBusinessException(esRequestErrorCode, "es查询失败！"));
        }
    }

    /**
     * @param deleteByQueryRequest
     * @return
     */
    public long deleteByQuery(DeleteByQueryRequest deleteByQueryRequest, RequestOptions requestOptions) {
        deleteByQueryRequest = Tools.builder().esQueryExecute(this).switchFilterButton(switchFilterButton).esQueryCheckButton(esQueryCheckButton).commConvertButton(commConvertButton).deleteByQuery(deleteByQueryRequest);
        RestHighLevelClient restHighLevelClient = null;
        try {
            restHighLevelClient = getTheConnectionObject();
            String[] indices = deleteByQueryRequest.indices();
            String[] types = deleteByQueryRequest.types();
            String queryStr = deleteByQueryRequest.getSearchRequest().toString();
            log.info(Curl4ESOperator.builders().hostList(hosts).indices(indices).types(types).queryStr(queryStr).deleteByQuery().build());
            LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
            if (isSeven) {
                Request request = RequestConverters.deleteByQuery(deleteByQueryRequest);
                Response response = restHighLevelClient.getLowLevelClient().performRequest(request);
                String responseBody = EntityUtils.toString(response.getEntity());
                Map map = (Map) JSON.parse(responseBody);
                Long deleted = (Long) map.get("deleted");
                log.info("成功调起es查询删除操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
                log.debug("成功删除{}个索引", deleted);
                return returnObjectsToTheConnectionPool(restHighLevelClient, deleted);
            } else {
                BulkByScrollResponse bulkByScrollResponse = restHighLevelClient.deleteByQuery(deleteByQueryRequest, requestOptions);
                log.info("成功调起es查询删除操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
                log.debug("成功删除{}个索引", bulkByScrollResponse.getDeleted());
                return returnObjectsToTheConnectionPool(restHighLevelClient, bulkByScrollResponse.getDeleted());
            }
        } catch (Exception e) {
            if (printStackTraceButton) e.printStackTrace();
            log.error("es查询删除失败，原因{}", e.getMessage());
            throw returnObjectsToTheConnectionPool(restHighLevelClient, new NewBusinessException(esRequestErrorCode, "es查询删除失败！"));
        }
    }

    /**
     * 根据官网的api，6.5的和7.5的这个bulkAsync方法参数和方法返回值都是一样的
     * 因此这个方法不需要区分
     *
     * @param request
     * @param requestOptions
     * @param bulkListener
     */
    public void bulkAsync(BulkRequest request, RequestOptions requestOptions, ActionListener<BulkResponse> bulkListener) {
        RestHighLevelClient restHighLevelClient = null;
        try {
            restHighLevelClient = getTheConnectionObject();
            log.info(Curl4ESOperator.builders().hostList(hosts).bulk(request).build());
            LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
            restHighLevelClient.bulkAsync(request, requestOptions, bulkListener);
            returnObjectsToTheConnectionPool(restHighLevelClient);
            log.info("成功调起es的bulkAsync操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
        } catch (Exception e) {
            if (printStackTraceButton) e.printStackTrace();
            log.error("es批查询失败，原因{}", e.getMessage());
            throw returnObjectsToTheConnectionPool(restHighLevelClient, new NewBusinessException(esRequestErrorCode, "es批查询失败！"));
        }
    }

    public void bulkAsync(BulkProcessor bulkProcessor) {
        try {
            bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
            bulkProcessor.close();
            log.info("成功调起es的bulkAsync操作>>>>>>>>>>>>>>>>>>>>>>>");
        } catch (Exception e) {
            if (printStackTraceButton) e.printStackTrace();
            log.error("es批量操作失败，原因{}", e.getMessage());
            throw returnObjectsToTheConnectionPool(null, new NewBusinessException(esRequestErrorCode, "es批量操作失败！"));
        }
    }

    public BulkResponse bulk(BulkRequest request, RequestOptions requestOptions) {
        RestHighLevelClient restHighLevelClient = null;
        try {
            restHighLevelClient = getTheConnectionObject();
            log.info(Curl4ESOperator.builders().hostList(hosts).bulk(request).build());
            LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
            BulkResponse bulkResponse = restHighLevelClient.bulk(request, requestOptions);
            log.info("成功调起es的bulk操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
            return returnObjectsToTheConnectionPool(restHighLevelClient, bulkResponse);
        } catch (Exception e) {
            if (printStackTraceButton) e.printStackTrace();
            log.error("es批查询失败，原因{}", e.getMessage());
            throw returnObjectsToTheConnectionPool(restHighLevelClient, new NewBusinessException(esRequestErrorCode, "es批查询失败！"));
        }
    }

    /**
     * 根据官网的api，6.5的和7.5的这个exists方法参数和方法返回值都是一样的
     * 因此这个方法不需要区分
     *
     * @param request
     * @return
     */
    public boolean exist(GetIndexRequest request, RequestOptions requestOptions) {
        String[] indices = request.indices();
        String[] types = request.types();
        log.info(Curl4ESOperator.builders().hostList(hosts).indices(indices).types(types).exist4Index().build());
        RestHighLevelClient restHighLevelClient = null;
        try {
            restHighLevelClient = getTheConnectionObject();
            LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
            boolean exists = restHighLevelClient.indices().exists(request, requestOptions);
            log.info("成功调起es查询索引存在操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
            log.debug("查询索引" + (exists ? "存在" : "不存在"));
            return returnObjectsToTheConnectionPool(restHighLevelClient, exists);
        } catch (Exception e) {
            if (printStackTraceButton) e.printStackTrace();
            log.error("es索引存在查询失败。原因 {}", e.getMessage());
            throw returnObjectsToTheConnectionPool(restHighLevelClient, new NewBusinessException(esRequestErrorCode, "es索引存在查询失败！"));
        }
    }

    /**
     * 根据官网的api，6.5的和7.5的这个putMapping方法参数和方法返回值都是一样的
     * 因此这个方法不需要区分
     * 使用这个方法的前提是必须要有索引，没有就会报错
     *
     * @param request
     * @param requestOptions
     * @return
     */
    public boolean putMapping(PutMappingRequest request, RequestOptions requestOptions) {
        RestHighLevelClient restHighLevelClient = null;
        try {
            restHighLevelClient = getTheConnectionObject();
            String[] indices = request.indices();
            String type = request.type();
            log.info(Curl4ESOperator.builders().hostList(hosts).indices(indices).type(type).queryStr(request.source()).createMapping().build());
            LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
            AcknowledgedResponse putMappingResponse = restHighLevelClient.indices().putMapping(request, requestOptions);
            boolean acknowledged = putMappingResponse.isAcknowledged();
            log.info("成功调起es的mapping创建操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
            log.debug("es Index创建" + (acknowledged ? "成功" : "失败"));
            return returnObjectsToTheConnectionPool(restHighLevelClient, acknowledged);
        } catch (Exception e) {
            if (printStackTraceButton) e.printStackTrace();
            log.error("es索引创建失败，原因{}", e.getMessage());
            throw returnObjectsToTheConnectionPool(restHighLevelClient, new NewBusinessException(esRequestErrorCode, "es索引创建失败！"));
        }
    }

    /**
     * 根据官网的api，6.5的和7.5的这个mget方法参数和方法返回值都是一样的
     * 因此这个方法不需要区分
     *
     * @param request
     * @param requestOptions
     * @return
     */

    public MultiGetResponse mget(MultiGetRequest request, RequestOptions requestOptions) {
        Map<String, List<MultiGetRequest.Item>> map = new HashMap<>();
        map.put("docs", request.getItems());
        log.info(Curl4ESOperator.builders().hostList(hosts).queryStr(JSON.toJSONString(map)).mGet().build());
        RestHighLevelClient restHighLevelClient = null;
        try {
            restHighLevelClient = getTheConnectionObject();
            LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
            MultiGetResponse multiGetItemResponses = restHighLevelClient.mget(request, requestOptions);
            List<String> list = Arrays.asList(multiGetItemResponses.getResponses()).stream().map(vo -> vo.getResponse().toString()).collect(Collectors.toList());
            log.info("成功调起es的mget操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
            log.debug("mget批量查询结果:{}", JSON.toJSONString(list));
            return returnObjectsToTheConnectionPool(restHighLevelClient, multiGetItemResponses);
        } catch (Exception e) {
            if (printStackTraceButton) e.printStackTrace();
            log.error("es索引批量查询失败。原因{}", e.getMessage());
            throw returnObjectsToTheConnectionPool(restHighLevelClient, new NewBusinessException(esRequestErrorCode, "es索引批量查询失败！"));
        }
    }

    /**
     * ES6 request.setSize(10);
     * ES7 request.setMaxDocs(10);
     * 不要设置这个参数，其他的都是一樣的
     *
     * @param updateByQueryRequest
     * @param requestOptions
     * @return
     */
    public long updateByQuery(UpdateByQueryRequest updateByQueryRequest, RequestOptions requestOptions) {
        updateByQueryRequest = Tools.builder().esQueryExecute(this).switchFilterButton(switchFilterButton).esQueryCheckButton(esQueryCheckButton).commConvertButton(commConvertButton).updateByQuery(updateByQueryRequest);
        String[] indices = updateByQueryRequest.indices();
        String[] docTypes = null;
        if (isSeven) docTypes = updateByQueryRequest.getDocTypes();
        SearchSourceBuilder source = updateByQueryRequest.getSearchRequest().source();
        Script script = updateByQueryRequest.getScript();
        log.info(Curl4ESOperator.builders().hostList(hosts).indices(indices).types(docTypes).queryStr(source.toString()).script(script).update().build());
        RestHighLevelClient restHighLevelClient = null;
        try {
            restHighLevelClient = getTheConnectionObject();
            updateByQueryRequest.setConflicts("proceed");//解决版本冲突
            updateByQueryRequest.setBatchSize(100).setScroll(TimeValue.timeValueMinutes(10)).setTimeout(TimeValue.timeValueMinutes(2)).setRefresh(true).setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
            LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
            BulkByScrollResponse bulkResponse = restHighLevelClient.updateByQuery(updateByQueryRequest, requestOptions);
            log.info("成功调起es的查询更新操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
            log.debug("成功更新{}个索引", bulkResponse.getUpdated());
            return returnObjectsToTheConnectionPool(restHighLevelClient, bulkResponse.getUpdated());
        } catch (Exception e) {
            if (printStackTraceButton) e.printStackTrace();
            log.error("es索引查询更新失败，原因{}", e.getMessage());
            throw returnObjectsToTheConnectionPool(restHighLevelClient, new NewBusinessException(esRequestErrorCode, "es索引查询更新失败！"));
        }

    }

    /**
     * 之所以这么写，是因为es6.6之前的版本并没有count方法，所以用search方法代替
     *
     * @param searchRequest
     * @return
     */
    public long count(SearchRequest searchRequest, RequestOptions requestOptions) {
        searchRequest = Tools.builder().esQueryExecute(this).switchFilterButton(switchFilterButton).esQueryCheckButton(esQueryCheckButton).commConvertButton(commConvertButton).count(searchRequest);
        String[] indices = searchRequest.indices();
        String[] types = searchRequest.types();
        SearchSourceBuilder source = searchRequest.source();
        source.size(0);
        searchRequest.source(source);
        log.info(Curl4ESOperator.builders().hostList(hosts).indices(indices).types(types).queryStr(source.toString()).count().build());
        LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
        SearchResponse response = search(searchRequest, requestOptions);
        long length = 0l;
        if (null != response || null != response.getHits()) length = response.getHits().getTotalHits();
        log.info("成功查询{}个索引,{}", length, Tools.printEsQueryTime.apply(start));
        return length;
    }


    /**
     * @param requestWithAlias
     * @param requestOptions
     * @return
     */
    public GetAliasesResponse getAlias(GetAliasesRequest requestWithAlias, RequestOptions requestOptions) {
        RestHighLevelClient restHighLevelClient = null;
        try {
            restHighLevelClient = getTheConnectionObject();
            String[] alias = requestWithAlias.aliases();
            if (ArrayUtils.isNotEmpty(alias))
                Arrays.asList(alias).stream().map(s -> Curl4ESOperator.builders().hostList(hosts).getAlias(s).build()).forEach(log::info);
            LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
            GetAliasesResponse response = restHighLevelClient.indices().getAlias(requestWithAlias, requestOptions);
            Map<String, Set<AliasMetaData>> aliases = response.getAliases();
            log.info("成功调起es获取别名操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
            log.debug("查询别名" + (aliases.size() > 0 ? "成功" + JSON.toJSONString(aliases) : "失败"));
            return returnObjectsToTheConnectionPool(restHighLevelClient, response);
        } catch (Exception e) {
            if (printStackTraceButton) e.printStackTrace();
            log.error("es查询别名失败。原因{}", e.getMessage());
            throw returnObjectsToTheConnectionPool(restHighLevelClient, new NewBusinessException(esRequestErrorCode, "es查询别名失败！"));
        }
    }

    /**
     * 增改别名
     * {
     * "actions" : [{"remove" : {"index" : "student" , "alias" : "in1"}}],
     * "actions" : [{"add" : {"index" : "student" , "alias" : "in2"}}]
     * }
     * ​
     *
     * @param request
     * @param requestOptions
     * @return
     */
    public AcknowledgedResponse updateAliases(IndicesAliasesRequest request, RequestOptions requestOptions) {
        AcknowledgedResponse indicesAliasesResponse;
        RestHighLevelClient restHighLevelClient = null;
        try {
            restHighLevelClient = getTheConnectionObject();
            List<IndicesAliasesRequest.AliasActions> aliasActions = request.getAliasActions();
            if (CollectionUtils.isNotEmpty(aliasActions)) {
                List<Map<String, Object>> list = Lists.newArrayList();
                for (IndicesAliasesRequest.AliasActions aliasAction : aliasActions) {
                    IndicesAliasesRequest.AliasActions.Type type = aliasAction.actionType();
                    String[] indices = aliasAction.indices();
                    String[] aliases = aliasAction.aliases();
                    Map<String, String> myMap = new HashMap<>();
                    if (ArrayUtils.isNotEmpty(indices)) myMap.put("index", indices[0]);
                    if (ArrayUtils.isNotEmpty(aliases)) myMap.put("alias", aliases[0]);
                    Map<String, Object> myMap2 = new HashMap<>();
                    if (type == IndicesAliasesRequest.AliasActions.Type.ADD) myMap2.put("add", myMap);
                    if (type == IndicesAliasesRequest.AliasActions.Type.REMOVE)
                        myMap2.put("remove", myMap);
                    if (type == IndicesAliasesRequest.AliasActions.Type.REMOVE_INDEX)
                        myMap2.put("remove", myMap);
                    list.add(myMap2);
                }
                Map<String, Object> actions = new HashMap<>();
                actions.put("actions", list);
                log.info(Curl4ESOperator.builders().hostList(hosts).queryStr(JSON.toJSONString(actions)).operateAlias().build());
            }
            LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
            indicesAliasesResponse = restHighLevelClient.indices().updateAliases(request, requestOptions);
            boolean acknowledged = indicesAliasesResponse.isAcknowledged();
            log.info("成功调起es增改别名操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
            log.debug("增改别名" + (acknowledged ? "成功" : "失败"));
        } catch (Exception e) {
            if (printStackTraceButton) e.printStackTrace();
            log.error("es增改别名失败，失败原因：{}", e.getMessage());
            throw returnObjectsToTheConnectionPool(restHighLevelClient, new NewBusinessException(esRequestErrorCode, "es增改别名失败！"));
        }
        return returnObjectsToTheConnectionPool(restHighLevelClient, indicesAliasesResponse);
    }

    /**
     * 创建空索引
     *
     * @param request
     * @param requestOptions
     * @return
     */
    public CreateIndexResponse create(CreateIndexRequest request, RequestOptions requestOptions) {
        CreateIndexResponse createIndexResponse;
        RestHighLevelClient restHighLevelClient = null;
        log.info(Curl4ESOperator.builders().hostList(hosts).index(request.index()).createIndex().build());
        LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
        try {
            restHighLevelClient = getTheConnectionObject();
            createIndexResponse = restHighLevelClient.indices().create(request, requestOptions);
            log.info("成功调起es的索引创建操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
            log.debug("成功创建索引{}", request.index());
        } catch (Exception e) {
            if (printStackTraceButton) e.printStackTrace();
            log.error("es索引创建失败，异常信息是 {}", e.getMessage());
            throw returnObjectsToTheConnectionPool(restHighLevelClient, new NewBusinessException(esRequestErrorCode, "es索引创建失败！"));
        }
        return returnObjectsToTheConnectionPool(restHighLevelClient, createIndexResponse);
    }


    public SearchResponse scroll(SearchScrollRequest scrollRequest, RequestOptions requestOptions) {
        SearchResponse response;
        RestHighLevelClient restHighLevelClient = null;
        String scrollId = scrollRequest.scrollId();
        log.info(Curl4ESOperator.builders().hostList(hosts).scroll(scrollId).build());
        LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
        try {
            restHighLevelClient = getTheConnectionObject();
            response = restHighLevelClient.scroll(scrollRequest, requestOptions);
            log.info("成功调起es的滚动查询操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
            log.debug("滚动查询结果{}", JSON.toJSONString(response));
        } catch (Exception e) {
            if (printStackTraceButton) e.printStackTrace();
            log.error("es索引scroll查询失败。原因{}", e.getMessage());
            throw returnObjectsToTheConnectionPool(restHighLevelClient, new NewBusinessException(esRequestErrorCode, "ES查询出错！"));
        }
        return returnObjectsToTheConnectionPool(restHighLevelClient, response);
    }


    public ClearScrollResponse clearScroll(ClearScrollRequest clearScrollRequest, RequestOptions requestOptions) {
        ClearScrollResponse response = null;
        RestHighLevelClient restHighLevelClient = null;
        List<String> scrollIds = clearScrollRequest.scrollIds();
        log.info(Curl4ESOperator.builders().hostList(hosts).cleanScroll(scrollIds).build());
        LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
        try {
            restHighLevelClient = getTheConnectionObject();
            response = restHighLevelClient.clearScroll(clearScrollRequest, requestOptions);
            log.info("成功调起es的清除滚动查询操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
            log.debug("清除滚动查询{}", response.isSucceeded() ? "成功" : "失败");
        } catch (Exception e) {
            if (printStackTraceButton) e.printStackTrace();
            log.error("es索引清除scrollId失败，原因{}", e.getMessage());
            throw returnObjectsToTheConnectionPool(restHighLevelClient, new NewBusinessException(esRequestErrorCode, "清除滚动查询失败！"));
        }
        return returnObjectsToTheConnectionPool(restHighLevelClient, response);
    }


    public MultiSearchResponse msearch(MultiSearchRequest multiSearchRequest, RequestOptions options) {
        RestHighLevelClient restHighLevelClient = null;
        try {
            restHighLevelClient = getTheConnectionObject();
            List<SearchRequest> requests = multiSearchRequest.requests();
            if (CollectionUtils.isNotEmpty(requests)) {
                for (SearchRequest searchRequest : requests) {
                    String[] indices = searchRequest.indices();
                    String[] types = searchRequest.types();
                    SearchSourceBuilder source = searchRequest.source();
                    log.info(Curl4ESOperator.builders().hostList(hosts).indices(indices).types(types).queryStr(source.toString()).search().build());
                }
            }
            LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
            MultiSearchResponse multiSearchResponse = restHighLevelClient.msearch(multiSearchRequest, options);
            log.info("成功调起es的msearch查询操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
            log.debug("查询结果{}", multiSearchResponse);
            return returnObjectsToTheConnectionPool(restHighLevelClient, multiSearchResponse);

        } catch (Exception e) {
            if (printStackTraceButton) e.printStackTrace();
            log.error("es-msearch查询失败，原因 {}", e.getMessage());
            throw returnObjectsToTheConnectionPool(restHighLevelClient, new NewBusinessException(esRequestErrorCode, "es查询失败！"));
        }
    }

    public GetMappingsResponse getMapping(GetMappingsRequest getMappingsRequest, RequestOptions requestOptions) {
        RestHighLevelClient restHighLevelClient = null;
        try {
            restHighLevelClient = getTheConnectionObject();
            LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
            String[] indices = getMappingsRequest.indices();
            log.info(Curl4ESOperator.builders().hostList(hosts).indices(indices).getMapping().build());
            GetMappingsResponse getMappingsResponse = restHighLevelClient.indices().getMapping(getMappingsRequest, requestOptions);
            log.info("成功调起es的getMapping查询操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
            log.debug("查询结果{}", getMappingsResponse);
            return returnObjectsToTheConnectionPool(restHighLevelClient, getMappingsResponse);
        } catch (Exception e) {
            if (printStackTraceButton) e.printStackTrace();
            log.error("es-getMapping查询失败，原因 {}", e.getMessage());
            throw returnObjectsToTheConnectionPool(restHighLevelClient, new NewBusinessException(esRequestErrorCode, "es查询失败！"));
        }
    }

    public AcknowledgedResponse delete(DeleteIndexRequest deleteIndexRequest, RequestOptions requestOptions) {
        AcknowledgedResponse acknowledgedResponse;
        RestHighLevelClient restHighLevelClient = null;
        log.info(Curl4ESOperator.builders().hostList(hosts).indices(deleteIndexRequest.indices()).delete().build());
        LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
        try {
            restHighLevelClient = getTheConnectionObject();
            acknowledgedResponse = restHighLevelClient.indices().delete(deleteIndexRequest, requestOptions);
            log.info("成功调起es的索引删除操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
            log.debug("成功删除索引{}", acknowledgedResponse.isAcknowledged());
        } catch (Exception e) {
            if (printStackTraceButton) e.printStackTrace();
            log.error("es索引删除失败，异常信息是 {}", e.getMessage());
            throw returnObjectsToTheConnectionPool(restHighLevelClient, new NewBusinessException(esRequestErrorCode, "es索引删除失败！"));
        }
        return returnObjectsToTheConnectionPool(restHighLevelClient, acknowledgedResponse);
    }

    public ClusterHealthResponse health(ClusterHealthRequest request, RequestOptions requestOptions) {
        ClusterHealthResponse clusterHealthResponse = null;
        RestHighLevelClient restHighLevelClient = null;
        LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
        try {
            restHighLevelClient = getTheConnectionObject();
            ClusterHealthResponse response = restHighLevelClient.cluster().health(request, requestOptions);
            log.info("成功调起es的索引删除操作,{}>>>>>>>>>>>>>>>>>>>>>>>", Tools.printEsQueryTime.apply(start));
            log.debug("成功删除索引{}", clusterHealthResponse.getStatus().name());
        } catch (Exception e) {
            if (printStackTraceButton) e.printStackTrace();
            log.error("es索引删除失败，异常信息是 {}", e.getMessage());
            throw returnObjectsToTheConnectionPool(restHighLevelClient, new NewBusinessException(esRequestErrorCode, "es索引删除失败！"));
        }
        return returnObjectsToTheConnectionPool(restHighLevelClient, clusterHealthResponse);
    }

    /**
     * 校验查询语句并且简单修正的方法,具体修正方法可以在查看转换方法
     */
    public static class CheckAndSimpleFix {

        private String index;

        private BoolQueryBuilder boolQueryBuilder;

        private Set<String> filedNames;

        private Function<String, String>[] commFiledProcess;//统一的filedName处理方式

        private BiFunction<String, String, String>[] everyFiledProcess;//针对索引不同字段的映射处理方式

        private boolean commConvert = true;

        private CheckAndSimpleFix() {
        }

        public static CheckAndSimpleFix builder() {
            return new CheckAndSimpleFix();
        }

        public CheckAndSimpleFix boolQueryBuilder(BoolQueryBuilder boolQueryBuilder) {
            this.boolQueryBuilder = boolQueryBuilder;
            return this;
        }

        public CheckAndSimpleFix searchSourceBuilder(SearchSourceBuilder searchSourceBuilder, String index, String type, BiFunction<String, String, Set<String>> getMapping) {
            if (null != searchSourceBuilder) {
                QueryBuilder queryBuilder = searchSourceBuilder.query();
                if (null != queryBuilder && queryBuilder instanceof BoolQueryBuilder)
                    this.boolQueryBuilder = (BoolQueryBuilder) queryBuilder;
                this.filedNames = getMapping.apply(index, type);
                this.index = index;
            }
            return this;
        }

        public CheckAndSimpleFix searchRequest(SearchRequest searchRequest, BiFunction<String, String, Set<String>> getMapping) {
            if (null != searchRequest) {
                String[] indices = searchRequest.indices();
                String[] types = searchRequest.types();
                SearchSourceBuilder source = searchRequest.source();
                searchSourceBuilder(source, indices[0], types[0], getMapping);
            }
            return this;
        }

        public CheckAndSimpleFix filedNames(String index, String type, BiFunction<String, String, Set<String>> getMapping) {
            this.filedNames = getMapping.apply(index, type);
            this.index = index;
            return this;
        }

        public CheckAndSimpleFix commConvert(boolean commConvert) {
            this.commConvert = commConvert;
            return this;
        }

        public CheckAndSimpleFix commFiledProcess(Function<String, String>... functions) {
            Function<String, String>[] array = this.commFiledProcess;
            this.commFiledProcess = MyArrays.merge(Function.class).apply(array).apply(functions);
            return this;
        }

        public CheckAndSimpleFix everyFiledProcess(BiFunction<String, String, String>... biFunctions) {
            this.everyFiledProcess = MyArrays.merge(BiFunction.class).apply(this.everyFiledProcess).apply(biFunctions);
            return this;
        }

        public BoolQueryBuilder build() {
            LocalDateTime start = LocalDateTime.now(Clock.systemDefaultZone());
            BoolQueryBuilder convert = convert(boolQueryBuilder);
            Duration duration = Duration.between(start, LocalDateTime.now(Clock.systemDefaultZone()));
            long runTime = duration.toMillis();
            log.info("本次校验，耗时{}毫秒", runTime);
            return convert;
        }

        private String convert(String fieldName) {
            if (MyArrays.isNotEmpty(commFiledProcess))
                for (Function<String, String> function : commFiledProcess) {
                    String inputStr = fieldName;
                    String outputStr = function.apply(fieldName);
                    if (!MyString.equals(inputStr, outputStr)) return outputStr;
                }
            if (MyArrays.isNotEmpty(everyFiledProcess))
                for (BiFunction<String, String, String> biFunction : everyFiledProcess) {
                    String inputStr = fieldName;
                    String outputStr = biFunction.apply(index, fieldName);
                    if (!MyString.equals(inputStr, outputStr)) return outputStr;
                }
            if (commConvert) {
                if (MyCollection.isNotEmpty(filedNames)) {//自定义转换规则,如果通用的规则无法转换，可以尝试自定义转换规则
                    if (!filedNames.contains(fieldName) && !filedNames.contains(fieldName.replace(".keyword", ""))) {
                        String camel2Underline = MyString.camel2Underline(fieldName);
                        if (!filedNames.contains(camel2Underline)) {//驼峰转下划线
                            String underline2Camel = MyString.underline2Camel(fieldName);
                            if (!filedNames.contains(underline2Camel)) {//下划线转驼峰
                                String lowerCase = fieldName.toLowerCase();
                                if (!filedNames.contains(lowerCase)) {//大写转小写
                                    String replaceStr = fieldName.replaceAll("_", "");
                                    if (!filedNames.contains(replaceStr)) {//去除下划线
                                        //TODO 如果有其他转换规则可以继续套娃下去，甚者可以指定特定索引特定字段的轉換規則
                                        log.error("对[{}]索引的es查询语句中的字段名[{}]在索引中未找到对应字段,这可能导致查询条件不生效,请检查代码!!!!!", index, fieldName);
                                    } else {
                                        log.warn("对[{}]索引的es查询语句中的字段名[{}]在索引中未找到对应字段,但根据去除下划线规则做了适配转换,从[{}]转换为[{}],虽能正常查询,但也请检查代码!!!!!", index, fieldName, fieldName, replaceStr);
                                        fieldName = replaceStr;
                                    }
                                } else {
                                    log.warn("对[{}]索引的es查询语句中的字段名[{}]在索引中未找到对应字段,但根据大写转小写规则做了适配转换,从[{}]转换为[{}],虽能正常查询,但也请检查代码!!!!!", index, fieldName, fieldName, lowerCase);
                                    fieldName = lowerCase;
                                }
                            } else {
                                log.warn("对[{}]索引的es查询语句中的字段名[{}]在索引中未找到对应字段,但根据下划线转驼峰规则做了适配转换,从[{}]转换为[{}],虽能正常查询,但也请检查代码!!!!!", index, fieldName, fieldName, underline2Camel);
                                fieldName = underline2Camel;
                            }
                        } else {
                            log.warn("对[{}]索引的es查询语句中的字段名[{}]在索引中未找到对应字段,但根据驼峰转下划线规则做了适配转换,从[{}]转换为[{}],虽能正常查询,但也请检查代码!!!!!", index, fieldName, fieldName, camel2Underline);
                            fieldName = camel2Underline;
                        }
                    }
                }
            }
            return fieldName;
        }

        private QueryBuilder convert(QueryBuilder queryBuilder) {
            QueryBuilder builder = null;
            if (null != queryBuilder) {
                if (queryBuilder instanceof TermQueryBuilder) {
                    TermQueryBuilder termQueryBuilder = (TermQueryBuilder) queryBuilder;
                    String fieldName = termQueryBuilder.fieldName();
                    String convert = convert(fieldName);
                    if (MyString.equals(fieldName, convert)) builder = termQueryBuilder;
                    else builder = QueryBuilders.termsQuery(convert, termQueryBuilder.value());
                } else if (queryBuilder instanceof TermsQueryBuilder) {
                    TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) queryBuilder;
                    String fieldName = termsQueryBuilder.fieldName();
                    String convert = convert(fieldName);
                    if (MyString.equals(fieldName, convert)) builder = termsQueryBuilder;
                    else builder = QueryBuilders.termsQuery(convert, termsQueryBuilder.values());
                } else if (queryBuilder instanceof RangeQueryBuilder) {
                    RangeQueryBuilder rangeQueryBuilder = (RangeQueryBuilder) queryBuilder;
                    String fieldName = rangeQueryBuilder.fieldName();
                    String convert = convert(fieldName);
                    if (MyString.equals(fieldName, convert)) builder = rangeQueryBuilder;
                    else {
                        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery(convert(fieldName)).includeLower(rangeQueryBuilder.includeLower()).includeLower(rangeQueryBuilder.includeUpper());
                        Object from = rangeQueryBuilder.from();
                        Object to = rangeQueryBuilder.to();
                        if (null != from) rangeQuery.from(from);
                        if (null != to) rangeQuery.from(to);
                        builder = rangeQuery;
                    }
                } else if (queryBuilder instanceof MatchQueryBuilder) {
                    MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) queryBuilder;
                    String fieldName = matchQueryBuilder.fieldName();
                    String convert = convert(fieldName);
                    if (MyString.equals(fieldName, convert)) builder = matchQueryBuilder;
                    else builder = QueryBuilders.matchQuery(convert, matchQueryBuilder.value());
                } else if (queryBuilder instanceof ExistsQueryBuilder) {
                    ExistsQueryBuilder existsQueryBuilder = (ExistsQueryBuilder) queryBuilder;
                    String fieldName = existsQueryBuilder.fieldName();
                    String convert = convert(fieldName);
                    if (MyString.equals(fieldName, convert)) builder = existsQueryBuilder;
                    else builder = QueryBuilders.existsQuery(convert);
                } else if (queryBuilder instanceof WildcardQueryBuilder) {
                    WildcardQueryBuilder wildcardQueryBuilder = (WildcardQueryBuilder) queryBuilder;
                    String fieldName = wildcardQueryBuilder.fieldName();
                    String convert = convert(fieldName);
                    if (MyString.equals(fieldName, convert)) builder = wildcardQueryBuilder;
                    else builder = QueryBuilders.wildcardQuery(convert, wildcardQueryBuilder.value());
                } else {

                }
            }
            return builder;
        }

        private BoolQueryBuilder convert(BoolQueryBuilder boolQueryBuilder) {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            if (MyCollection.isEmpty(filedNames)) return boolQueryBuilder;
            if (null != boolQueryBuilder) {
                List<QueryBuilder> must = boolQueryBuilder.must();
                if (MyCollection.isNotEmpty(must)) {
                    for (QueryBuilder queryBuilder : must)
                        if (queryBuilder instanceof BoolQueryBuilder)
                            boolQuery.must(convert((BoolQueryBuilder) queryBuilder));
                        else boolQuery.must(convert(queryBuilder));
                }
                List<QueryBuilder> should = boolQueryBuilder.should();
                if (MyCollection.isNotEmpty(should)) {
                    for (QueryBuilder queryBuilder : should)
                        if (queryBuilder instanceof BoolQueryBuilder)
                            boolQuery.should(convert((BoolQueryBuilder) queryBuilder));
                        else boolQuery.should(convert(queryBuilder));
                    boolQuery.minimumShouldMatch(1);
                }
                List<QueryBuilder> mustNot = boolQueryBuilder.mustNot();
                if (MyCollection.isNotEmpty(mustNot)) {
                    for (QueryBuilder queryBuilder : mustNot)
                        if (queryBuilder instanceof BoolQueryBuilder)
                            boolQuery.mustNot(convert((BoolQueryBuilder) queryBuilder));
                        else boolQuery.mustNot(convert(queryBuilder));
                }
                List<QueryBuilder> filter = boolQueryBuilder.filter();
                if (MyCollection.isNotEmpty(filter)) {
                    for (QueryBuilder queryBuilder : filter)
                        if (queryBuilder instanceof BoolQueryBuilder)
                            boolQuery.filter(convert((BoolQueryBuilder) queryBuilder));
                        else boolQuery.filter(convert(queryBuilder));
                }
            }
            return boolQuery;
        }
    }

    /**
     * 拼接查询语句
     */
    public static class Curl4ESOperator {

        private Curl4ESOperator() {
        }

        private String hostList;
        private String index;
        private String type;
        private String queryStr;
        private String[] indices;
        private String[] types;

        private Script script;

        private String logStr;


        public Curl4ESOperator index(String index) {
            this.index = index;
            return this;
        }

        public Curl4ESOperator type(String type) {
            this.type = type;
            return this;
        }

        public Curl4ESOperator queryStr(String queryStr) {
            this.queryStr = queryStr;
            return this;
        }

        public Curl4ESOperator indices(String[] indices) {
            this.indices = indices;
            return this;
        }

        public Curl4ESOperator types(String[] types) {
            this.types = types;
            return this;
        }

        public Curl4ESOperator hostList(String hostList) {
            this.hostList = hostList;
            return this;
        }

        public Curl4ESOperator script(Script script) {
            this.script = script;
            return this;
        }

        public static Curl4ESOperator builders() {
            return new Curl4ESOperator();
        }

        public String build() {
            return logStr;
        }


        private String spliceHttpUrl() {
            String[] split = hostList.split(",");
            StringBuffer sb = new StringBuffer();
            sb.append("http://");
            if (StringUtils.isNotBlank(split[0])) sb.append(split[0]);
            if (StringUtils.isNotBlank(index)) sb.append("/").append(index);
            if (null != indices && indices.length > 0) sb.append("/").append(indices[0]);
            if (StringUtils.isNotBlank(type)) sb.append("/").append(type);
            if (null != types && types.length > 0) sb.append("/").append(types[0]);
            return sb.toString();
        }


        private String getIndex0(String[] strs) {
            String str = null;
            if (ArrayUtils.isNotEmpty(strs)) str = strs[0];
            return str;
        }

        private String warpResult(String topic, String str) {
            StringBuffer sb = new StringBuffer(topic);
            sb.append(">>>>>>").append("[").append(str).append("]");
            return sb.toString();
        }

        /**
         * curl -XGET http://hadoop137:9200/upuptop/stu/_search?pretty  -H 'Content-Type: application/json' -d '{"query":{"match":{"name":"upuptop"}}}'
         *
         * @return
         */
        public Curl4ESOperator search() {
            StringBuffer sb = new StringBuffer();
            sb.append("curl -XGET ").append(spliceHttpUrl()).append("/_search?pretty  -H 'Content-Type: application/json' -d '");
            if (StringUtils.isNotBlank(queryStr)) sb.append(queryStr);
            sb.append("'");
            logStr = warpResult("search", sb.toString());
            return this;
        }


        /**
         * curl -XPUT 'http://192.168.1.105:9200/bank/item2/_mapping' -d ' { "item2": { "properties": { "title": { "type": "string", "boost": 2.0, "index": "analyzed", "store": "yes", "term_vector" : "with_positions_offsets" }, "description": { "type": "string", "boost": 1.0, "index": "analyzed", "store": "yes", "term_vector" : "with_positions_offsets" }, "link": { "type": "geo_point" }, "ip": { "store": true, "type": "ip" } } } }'
         */
        public Curl4ESOperator createMapping() {
            StringBuffer sb = new StringBuffer();
            sb.append("curl -XPUT ").append(spliceHttpUrl()).append("/_mapping' -d '");
            if (StringUtils.isNotBlank(queryStr)) sb.append(queryStr);
            sb.append("'");
            logStr = warpResult("mapping", sb.toString());
            return this;
        }


        /**
         * curl -X POST "localhost:9200/twitter/_delete_by_query" -H 'Content-Type: application/json' -d' { "query": { "match": { "name": "测试删除" } } } '
         */
        public Curl4ESOperator deleteByQuery() {
            StringBuffer sb = new StringBuffer();
            sb.append("curl -X POST ").append(spliceHttpUrl()).append("/_delete_by_query\" -H 'Content-Type: application/json' -d'");
            if (StringUtils.isNotBlank(queryStr)) sb.append(queryStr);
            sb.append("'");
            logStr = warpResult("delete_by_query", sb.toString());
            return this;
        }


        /**
         * curl -XGET http://localhost:9200/_mget?pretty -d '{"docs":[{"_index":“hello","_type":"emp","_id":2,"_source":"name"},
         */
        public Curl4ESOperator mGet() {
            StringBuffer sb = new StringBuffer();
            sb.append("curl -XGET ").append(spliceHttpUrl()).append("/_mget?pretty -d '");
            if (StringUtils.isNotBlank(queryStr)) sb.append(queryStr);
            sb.append("'");
            logStr = warpResult("mGet", sb.toString());
            return this;
        }


        /**
         * curl -XPOST 'localhost:9200/twitter/_update_by_query' -d ' { "query" : { "term" : {"message" : "you"}},"script" : "ctx._source.likes += 1" }'
         */
        public Curl4ESOperator update() {
            StringBuffer sb = new StringBuffer();
            sb.append("curl -XPOST ").append(spliceHttpUrl()).append("/_update_by_query' -d '");
            if (StringUtils.isNotBlank(queryStr)) {
                if (null != script) {
                    Map map = JSON.parseObject(queryStr, Map.class);
                    String idOrCode = script.getIdOrCode();
                    map.put("script", idOrCode);
                    queryStr = JSON.toJSONString(map);
                }
                sb.append(queryStr);
            }
            sb.append("'");
            logStr = warpResult("update_by_query", sb.toString());
            return this;
        }

        private String header(String opType, String index, String type, String id) {
            Map<String, String> map = new HashMap<>();
            map.put("_index", index);
            map.put("_type", type);
            map.put("_id", id);
            Map<String, Map<String, String>> mapMap = new HashMap<>();
            mapMap.put(opType, map);
            return JSON.toJSONString(map);
        }

        /**
         * curl -X POST "localhost:9200/customer/_doc/_bulk?pretty" -H 'Content-Type: application/json' -d' {"update":{"_id":"1"}} {"doc": { "name": "John Doe becomes Jane Doe" } } {"delete":{"_id":"2"}}
         */
        public Curl4ESOperator bulk(BulkRequest request) {
            StringBuffer sb = new StringBuffer();
            sb.append("curl -XPOST ").append(spliceHttpUrl()).append("/_bulk?pretty\" -H 'Content-Type: application/json' -d'");
            List<DocWriteRequest<?>> requests = request.requests();
            if (null != request && requests.size() > 0) {
                /**
                 *
                 * index 和 create  第二行是source数据体
                 * delete 没有第二行
                 * update 第二行可以是partial doc，upsert或者是script
                 * { "index" : { "_index" : "test", "_type" : "_doc", "_id" : "1" } }
                 * { "field1" : "value1" }
                 * { "delete" : { "_index" : "test", "_type" : "_doc", "_id" : "2" } }
                 * { "create" : { "_index" : "test", "_type" : "_doc", "_id" : "3" } }
                 * { "field1" : "value3" }
                 * { "update" : {"_id" : "1", "_type" : "_doc", "_index" : "test"} }
                 * { "doc" : {"field2" : "value2"} }
                 *
                 *
                 * { "update" : {"_id" : "1", "_type" : "_doc", "_index" : "index1", "retry_on_conflict" : 3} }
                 * { "doc" : {"field" : "value"} }
                 * { "update" : { "_id" : "0", "_type" : "_doc", "_index" : "index1", "retry_on_conflict" : 3} }
                 * { "script" : { "source": "ctx._source.counter += params.param1", "lang" : "painless", "params" : {"param1" : 1}}, "upsert" : {"counter" : 1}}
                 * { "update" : {"_id" : "2", "_type" : "_doc", "_index" : "index1", "retry_on_conflict" : 3} }
                 * { "doc" : {"field" : "value"}, "doc_as_upsert" : true }
                 * { "update" : {"_id" : "3", "_type" : "_doc", "_index" : "index1", "_source" : true} }
                 * { "doc" : {"field" : "value"} }
                 * { "update" : {"_id" : "4", "_type" : "_doc", "_index" : "index1"} }
                 * { "doc" : {"field" : "value"}, "_source": true}
                 */
                for (DocWriteRequest<?> docWriteRequest : requests) {
                    if (docWriteRequest instanceof IndexRequest) {
                        IndexRequest indexRequest = (IndexRequest) docWriteRequest;
                        String header = header("index", indexRequest.index(), indexRequest.type(), indexRequest.id());
                        String source = indexRequest.source().utf8ToString();
                        sb.append("\r\n").append(header).append("\r\n").append(source);
                    }
                    if (docWriteRequest instanceof DeleteRequest) {
                        DeleteRequest deleteRequest = (DeleteRequest) docWriteRequest;
                        String header = header("delete", deleteRequest.index(), deleteRequest.type(), deleteRequest.id());
                        sb.append("\r\n").append(header);
                    }
                    if (docWriteRequest instanceof UpdateRequest) {
                        UpdateRequest updateRequest = (UpdateRequest) docWriteRequest;
                        String header = header("update", updateRequest.index(), updateRequest.type(), updateRequest.id());
                        sb.append("\r\n").append(header);
                        Script script = updateRequest.script();
                        if (null != script) {
                            String idOrCode = script.getIdOrCode();
                            Map<String, Object> map = new HashMap<>();
                            map.put("source", idOrCode);
                            map.put("lang", script.getLang());
                            map.put("params", script.getParams());
                            Map<String, Map<String, Object>> mapMap = new HashMap<>();
                            mapMap.put("script", map);
                            sb.append("\r\n").append(MyJSON.toJSONString(mapMap));
                        }
                        IndexRequest doc = updateRequest.doc();
                        if (null != doc) {
                            sb.append("\r\n").append(doc.source().utf8ToString());
                        }
                        IndexRequest upsertRequest = updateRequest.upsertRequest();
                        if (null != upsertRequest) {
                            sb.append("\r\n").append(upsertRequest.source().utf8ToString());
                        }
                    }
                }
            }
            sb.append("'");
            logStr = warpResult("bulk", sb.toString());
            return this;
        }

        /**
         * curl -XGET http://hadoop137:9200/upuptop/stu/_count?pretty  -H 'Content-Type: application/json' -d '{"query":{"match": {"name":"upuptop"} }}'
         *
         * @return
         */
        public Curl4ESOperator count() {
            StringBuffer sb = new StringBuffer();
            sb.append("curl -XGET ").append(spliceHttpUrl()).append("/_count?pretty  -H 'Content-Type: application/json' -d '");
            if (StringUtils.isNotBlank(queryStr)) sb.append(queryStr);
            sb.append("'");
            logStr = warpResult("count", sb.toString());
            return this;
        }


        /**
         * curl -XPUT http://192.168.1.105:9200/bank/item2
         */
        public Curl4ESOperator createIndex() {
            StringBuffer sb = new StringBuffer();
            sb.append("curl -XPUT ").append(spliceHttpUrl());
            logStr = warpResult("create", sb.toString());
            return this;
        }

        /**
         * curl -XGET 'localhost:9200/twitter/tweet/_search?scroll=1m&pretty' -H 'Content-Type: application/json' -d' { "slice": { "field": "date", "id": 0, "max": 10 }, "query": { "match" : { "title" : "elasticsearch" } } } '
         */
        public Curl4ESOperator preScroll() {
            StringBuffer sb = new StringBuffer();
            sb.append("curl -XGET ").append(spliceHttpUrl()).append("/_search?scroll=1m&pretty' -H 'Content-Type: application/json' -d'");
            if (StringUtils.isNotBlank(queryStr)) sb.append(queryStr);
            sb.append("'");
            logStr = warpResult("preScroll", sb.toString());
            return this;
        }

        /**
         * curl -XGET  'localhost:9200/_search/scroll'  -d' { "scroll" : "1m", "scroll_id" : "c2Nhbjs2OzM0NDg1ODpzRlBLc0FXNlNyNm5JWUc1" }'
         */
        public Curl4ESOperator scroll(String scrollId) {
            StringBuffer sb = new StringBuffer();
            sb.append("curl -XGET ").append(spliceHttpUrl());
            if (!StringUtils.isBlank(scrollId)) {
                Map<String, String> map = MyMap.<String, String>builder().of("scroll", "1m").of("scroll_id", scrollId).build();
//                Map<String, String> map = new HashMap<>();
                map.put("scroll", "1m");
                sb.append("/_search/scroll'  -d'").append(JSON.toJSONString(map)).append("'");
            }
            logStr = warpResult("scroll", sb.toString());
            return this;
        }

        /**
         * curl -XDELETE localhost:9200/_search/scroll  -d' { "scroll_id" : ["c2Nhbjs2OzM0NDg1ODpzRlBLc0FXNlNyNm5JWUc1", "aGVuRmV0Y2g7NTsxOnkxaDZ"] }'
         */
        public Curl4ESOperator cleanScroll(List<String> scrollIds) {
            StringBuffer sb = new StringBuffer();
            if (CollectionUtils.isNotEmpty(scrollIds)) {
                scrollIds.forEach(scrollId -> {
                    Map<String, String> map = MyMap.<String, String>builder().of("scroll_id", scrollId).build();
                    sb.append("curl -XDELETE ").append(spliceHttpUrl()).append("/_search/scroll  -d'").append(JSON.toJSONString(map)).append("'");
                });
            }
            logStr = warpResult("cleanScroll", sb.toString());
            return this;
        }

        /**
         * curl -XPOST 'http://localhost:9200/_aliases' -d ' { "actions" : [ { "add" : { "index" : "dm_v1", "alias" : "dm_alias" } } ] }'
         */
        public Curl4ESOperator operateAlias() {
            StringBuffer sb = new StringBuffer();
            sb.append("curl -XPOST ").append(spliceHttpUrl()).append("/_aliases' -d '");
            if (StringUtils.isNotBlank(queryStr)) sb.append(queryStr);
            sb.append("'");
            logStr = warpResult("aliases", sb.toString());
            return this;
        }


        /**
         * curl -XGET 'localhost:9200/_alias/dm'
         */
        public Curl4ESOperator getAlias(String alias) {
            StringBuffer sb = new StringBuffer();
            sb.append("curl -XGET ").append(spliceHttpUrl()).append("/_alias/").append(alias);
            logStr = warpResult("alias", sb.toString());
            return this;
        }


        /**
         * curl -XGET http://hadoop137:9200/upuptop/stu/_search?pretty  -H 'Content-Type: application/json' -d '{   "from": 0,   "size": 0 }'
         */
        public Curl4ESOperator exist4Index() {
            StringBuffer sb = new StringBuffer();
            sb.append("curl -XGET ").append(spliceHttpUrl()).append("/_search?pretty  -H 'Content-Type: application/json' -d '{   \"from\": 0,   \"size\": 0 }'");
            logStr = warpResult("exist", sb.toString());
            return this;
        }

        /**
         * curl -XGET 'localhost:9200/_all/_mapping?pretty'
         */
        public Curl4ESOperator getMapping() {
            StringBuffer sb = new StringBuffer();
            sb.append("curl -XGET ").append(spliceHttpUrl()).append("/_mapping").append("?pretty'");
            sb.append("'");
            logStr = warpResult("search", sb.toString());
            return this;
        }

        public Curl4ESOperator delete() {
            StringBuffer sb = new StringBuffer();
            sb.append("curl -XDELETE ").append(spliceHttpUrl());
            logStr = warpResult("delete", sb.toString());
            return this;
        }
    }


    public static class RequestConverters {

        static final XContentType REQUEST_BODY_CONTENT_TYPE = XContentType.JSON;

        private RequestConverters() {
            // Contains only status utility methods
        }

        static Request delete(DeleteRequest deleteRequest) {
            String endpoint = endpoint(deleteRequest.index(), deleteRequest.type(), deleteRequest.id());
            Request request = new Request(HttpDelete.METHOD_NAME, endpoint);

            Params parameters = new Params(request);
            parameters.withRouting(deleteRequest.routing());
            parameters.withParent(deleteRequest.parent());
            parameters.withTimeout(deleteRequest.timeout());
            parameters.withVersion(deleteRequest.version());
            parameters.withVersionType(deleteRequest.versionType());
            parameters.withRefreshPolicy(deleteRequest.getRefreshPolicy());
            parameters.withWaitForActiveShards(deleteRequest.waitForActiveShards(), ActiveShardCount.DEFAULT);
            return request;
        }

        static Request info() {
            return new Request(HttpGet.METHOD_NAME, "/");
        }

        static Request bulk(BulkRequest bulkRequest) throws IOException {
            Request request = new Request(HttpPost.METHOD_NAME, "/_bulk");

            Params parameters = new Params(request);
            parameters.withTimeout(bulkRequest.timeout());
            parameters.withRefreshPolicy(bulkRequest.getRefreshPolicy());

            // Bulk API only supports newline delimited JSON or Smile. Before executing
            // the bulk, we need to check that all requests have the same content-type
            // and this content-type is supported by the Bulk API.
            XContentType bulkContentType = null;
            for (int i = 0; i < bulkRequest.numberOfActions(); i++) {
                DocWriteRequest<?> action = bulkRequest.requests().get(i);

                DocWriteRequest.OpType opType = action.opType();
                if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                    bulkContentType = enforceSameContentType((IndexRequest) action, bulkContentType);

                } else if (opType == DocWriteRequest.OpType.UPDATE) {
                    UpdateRequest updateRequest = (UpdateRequest) action;
                    if (updateRequest.doc() != null) {
                        bulkContentType = enforceSameContentType(updateRequest.doc(), bulkContentType);
                    }
                    if (updateRequest.upsertRequest() != null) {
                        bulkContentType = enforceSameContentType(updateRequest.upsertRequest(), bulkContentType);
                    }
                }
            }

            if (bulkContentType == null) {
                bulkContentType = XContentType.JSON;
            }

            final byte separator = bulkContentType.xContent().streamSeparator();
            final ContentType requestContentType = createContentType(bulkContentType);

            ByteArrayOutputStream content = new ByteArrayOutputStream();
            for (DocWriteRequest<?> action : bulkRequest.requests()) {
                DocWriteRequest.OpType opType = action.opType();

                try (XContentBuilder metadata = XContentBuilder.builder(bulkContentType.xContent())) {
                    metadata.startObject();
                    {
                        metadata.startObject(opType.getLowercase());
                        if (Strings.hasLength(action.index())) {
                            metadata.field("_index", action.index());
                        }
                        if (Strings.hasLength(action.type())) {
                            metadata.field("_type", action.type());
                        }
                        if (Strings.hasLength(action.id())) {
                            metadata.field("_id", action.id());
                        }
                        if (Strings.hasLength(action.routing())) {
                            metadata.field("routing", action.routing());
                        }
                        if (Strings.hasLength(action.parent())) {
                            metadata.field("parent", action.parent());
                        }
                        if (action.version() != Versions.MATCH_ANY) {
                            metadata.field("version", action.version());
                        }

                        VersionType versionType = action.versionType();
                        if (versionType != VersionType.INTERNAL) {
                            if (versionType == VersionType.EXTERNAL) {
                                metadata.field("version_type", "external");
                            } else if (versionType == VersionType.EXTERNAL_GTE) {
                                metadata.field("version_type", "external_gte");
                            } else if (versionType == VersionType.FORCE) {
                                metadata.field("version_type", "force");
                            }
                        }

                        if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                            IndexRequest indexRequest = (IndexRequest) action;
                            if (Strings.hasLength(indexRequest.getPipeline())) {
                                metadata.field("pipeline", indexRequest.getPipeline());
                            }
                        } else if (opType == DocWriteRequest.OpType.UPDATE) {
                            UpdateRequest updateRequest = (UpdateRequest) action;
                            if (updateRequest.retryOnConflict() > 0) {
                                metadata.field("retry_on_conflict", updateRequest.retryOnConflict());
                            }
                            if (updateRequest.fetchSource() != null) {
                                metadata.field("_source", updateRequest.fetchSource());
                            }
                        }
                        metadata.endObject();
                    }
                    metadata.endObject();

                    BytesRef metadataSource = BytesReference.bytes(metadata).toBytesRef();
                    content.write(metadataSource.bytes, metadataSource.offset, metadataSource.length);
                    content.write(separator);
                }

                BytesRef source = null;
                if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                    IndexRequest indexRequest = (IndexRequest) action;
                    BytesReference indexSource = indexRequest.source();
                    XContentType indexXContentType = indexRequest.getContentType();

                    try (XContentParser parser = XContentHelper.createParser(
                            /*
                             * EMPTY and THROW are fine here because we just call
                             * copyCurrentStructure which doesn't touch the
                             * registry or deprecation.
                             */
                            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            indexSource, indexXContentType)) {
                        try (XContentBuilder builder = XContentBuilder.builder(bulkContentType.xContent())) {
                            builder.copyCurrentStructure(parser);
                            source = BytesReference.bytes(builder).toBytesRef();
                        }
                    }
                } else if (opType == DocWriteRequest.OpType.UPDATE) {
                    source = XContentHelper.toXContent((UpdateRequest) action, bulkContentType, false).toBytesRef();
                }

                if (source != null) {
                    content.write(source.bytes, source.offset, source.length);
                    content.write(separator);
                }
            }
            request.setEntity(new ByteArrayEntity(content.toByteArray(), 0, content.size(), requestContentType));
            return request;
        }

        static Request exists(GetRequest getRequest) {
            return getStyleRequest(HttpHead.METHOD_NAME, getRequest);
        }

        static Request get(GetRequest getRequest) {
            return getStyleRequest(HttpGet.METHOD_NAME, getRequest);
        }

        private static Request getStyleRequest(String method, GetRequest getRequest) {
            Request request = new Request(method, endpoint(getRequest.index(), getRequest.type(), getRequest.id()));

            Params parameters = new Params(request);
            parameters.withPreference(getRequest.preference());
            parameters.withRouting(getRequest.routing());
            parameters.withParent(getRequest.parent());
            parameters.withRefresh(getRequest.refresh());
            parameters.withRealtime(getRequest.realtime());
            parameters.withStoredFields(getRequest.storedFields());
            parameters.withVersion(getRequest.version());
            parameters.withVersionType(getRequest.versionType());
            parameters.withFetchSourceContext(getRequest.fetchSourceContext());

            return request;
        }

        static Request multiGet(MultiGetRequest multiGetRequest) throws IOException {
            Request request = new Request(HttpPost.METHOD_NAME, "/_mget");

            Params parameters = new Params(request);
            parameters.withPreference(multiGetRequest.preference());
            parameters.withRealtime(multiGetRequest.realtime());
            parameters.withRefresh(multiGetRequest.refresh());

            request.setEntity(createEntity(multiGetRequest, REQUEST_BODY_CONTENT_TYPE));
            return request;
        }

        static Request index(IndexRequest indexRequest) {
            String method = Strings.hasLength(indexRequest.id()) ? HttpPut.METHOD_NAME : HttpPost.METHOD_NAME;
            boolean isCreate = (indexRequest.opType() == DocWriteRequest.OpType.CREATE);
            String endpoint = endpoint(indexRequest.index(), indexRequest.type(), indexRequest.id(), isCreate ? "_create" : null);
            Request request = new Request(method, endpoint);

            Params parameters = new Params(request);
            parameters.withRouting(indexRequest.routing());
            parameters.withParent(indexRequest.parent());
            parameters.withTimeout(indexRequest.timeout());
            parameters.withVersion(indexRequest.version());
            parameters.withVersionType(indexRequest.versionType());
            parameters.withPipeline(indexRequest.getPipeline());
            parameters.withRefreshPolicy(indexRequest.getRefreshPolicy());
            parameters.withWaitForActiveShards(indexRequest.waitForActiveShards(), ActiveShardCount.DEFAULT);

            BytesRef source = indexRequest.source().toBytesRef();
            ContentType contentType = createContentType(indexRequest.getContentType());
            request.setEntity(new ByteArrayEntity(source.bytes, source.offset, source.length, contentType));
            return request;
        }

        static Request ping() {
            return new Request(HttpHead.METHOD_NAME, "/");
        }

        static Request update(UpdateRequest updateRequest) throws IOException {
            String endpoint = endpoint(updateRequest.index(), updateRequest.type(), updateRequest.id(), "_update");
            Request request = new Request(HttpPost.METHOD_NAME, endpoint);

            Params parameters = new Params(request);
            parameters.withRouting(updateRequest.routing());
            parameters.withParent(updateRequest.parent());
            parameters.withTimeout(updateRequest.timeout());
            parameters.withRefreshPolicy(updateRequest.getRefreshPolicy());
            parameters.withWaitForActiveShards(updateRequest.waitForActiveShards(), ActiveShardCount.DEFAULT);
            parameters.withDocAsUpsert(updateRequest.docAsUpsert());
            parameters.withFetchSourceContext(updateRequest.fetchSource());
            parameters.withRetryOnConflict(updateRequest.retryOnConflict());
            parameters.withVersion(updateRequest.version());
            parameters.withVersionType(updateRequest.versionType());

            // The Java API allows update requests with different content types
            // set for the partial document and the upsert document. This client
            // only accepts update requests that have the same content types set
            // for both doc and upsert.
            XContentType xContentType = null;
            if (updateRequest.doc() != null) {
                xContentType = updateRequest.doc().getContentType();
            }
            if (updateRequest.upsertRequest() != null) {
                XContentType upsertContentType = updateRequest.upsertRequest().getContentType();
                if ((xContentType != null) && (xContentType != upsertContentType)) {
                    throw new IllegalStateException("Update request cannot have different content types for doc [" + xContentType + "]" +
                            " and upsert [" + upsertContentType + "] documents");
                } else {
                    xContentType = upsertContentType;
                }
            }
            if (xContentType == null) {
                xContentType = Requests.INDEX_CONTENT_TYPE;
            }
            request.setEntity(createEntity(updateRequest, xContentType));
            return request;
        }

        public static Request search(SearchRequest searchRequest, boolean isSeven) throws IOException {
            Request request = new Request(HttpPost.METHOD_NAME, endpoint(searchRequest.indices(), searchRequest.types(), "_search"));

            Params params = new Params(request);
            addSearchRequestParams(params, searchRequest, isSeven);

            if (searchRequest.source() != null) {
                request.setEntity(createEntity(searchRequest.source(), REQUEST_BODY_CONTENT_TYPE));
            }
            return request;
        }

        public static void addSearchRequestParams(Params params, SearchRequest searchRequest, boolean isSeven) {
            params.putParam(RestSearchAction.TYPED_KEYS_PARAM, "true");
            params.withRouting(searchRequest.routing());
            params.withPreference(searchRequest.preference());
            params.withIndicesOptions(searchRequest.indicesOptions());
            params.putParam("search_type", searchRequest.searchType().name().toLowerCase(Locale.ROOT));
            if (searchRequest.requestCache() != null) {
                params.putParam("request_cache", Boolean.toString(searchRequest.requestCache()));
            }
            if (searchRequest.allowPartialSearchResults() != null) {
                params.putParam("allow_partial_search_results", Boolean.toString(searchRequest.allowPartialSearchResults()));
            }
            params.putParam("batched_reduce_size", Integer.toString(searchRequest.getBatchedReduceSize()));
            if (searchRequest.scroll() != null) {
                params.putParam("scroll", searchRequest.scroll().keepAlive());
            }
            //es7则加上此参数返回total格式与es6一样，否则返回格式为{"hits":{"total":{"value":1,"relation":"eq"}}}，searchResponse.getHits().getTotal()无法获取正确total
            if (isSeven) params.putParam("rest_total_hits_as_int", "true");
        }

        static Request searchScroll(SearchScrollRequest searchScrollRequest) throws IOException {
            Request request = new Request(HttpPost.METHOD_NAME, "/_search/scroll");
            request.setEntity(createEntity(searchScrollRequest, REQUEST_BODY_CONTENT_TYPE));
            return request;
        }

        static Request clearScroll(ClearScrollRequest clearScrollRequest) throws IOException {
            Request request = new Request(HttpDelete.METHOD_NAME, "/_search/scroll");
            request.setEntity(createEntity(clearScrollRequest, REQUEST_BODY_CONTENT_TYPE));
            return request;
        }

        static Request multiSearch(MultiSearchRequest multiSearchRequest) throws IOException {
            Request request = new Request(HttpPost.METHOD_NAME, "/_msearch");

            Params params = new Params(request);
            params.putParam(RestSearchAction.TYPED_KEYS_PARAM, "true");
            if (multiSearchRequest.maxConcurrentSearchRequests() != MultiSearchRequest.MAX_CONCURRENT_SEARCH_REQUESTS_DEFAULT) {
                params.putParam("max_concurrent_searches", Integer.toString(multiSearchRequest.maxConcurrentSearchRequests()));
            }

            XContent xContent = REQUEST_BODY_CONTENT_TYPE.xContent();
            byte[] source = MultiSearchRequest.writeMultiLineFormat(multiSearchRequest, xContent);
            request.setEntity(new ByteArrayEntity(source, createContentType(xContent.type())));
            return request;
        }

        static Request searchTemplate(SearchTemplateRequest searchTemplateRequest, boolean isSeven) throws IOException {
            Request request;

            if (searchTemplateRequest.isSimulate()) {
                request = new Request(HttpGet.METHOD_NAME, "_render/template");
            } else {
                SearchRequest searchRequest = searchTemplateRequest.getRequest();
                String endpoint = endpoint(searchRequest.indices(), searchRequest.types(), "_search/template");
                request = new Request(HttpGet.METHOD_NAME, endpoint);

                Params params = new Params(request);
                addSearchRequestParams(params, searchRequest, isSeven);
            }

            request.setEntity(createEntity(searchTemplateRequest, REQUEST_BODY_CONTENT_TYPE));
            return request;
        }

        static Request multiSearchTemplate(MultiSearchTemplateRequest multiSearchTemplateRequest) throws IOException {
            Request request = new Request(HttpPost.METHOD_NAME, "/_msearch/template");

            Params params = new Params(request);
            params.putParam(RestSearchAction.TYPED_KEYS_PARAM, "true");
            if (multiSearchTemplateRequest.maxConcurrentSearchRequests() != MultiSearchRequest.MAX_CONCURRENT_SEARCH_REQUESTS_DEFAULT) {
                params.putParam("max_concurrent_searches", Integer.toString(multiSearchTemplateRequest.maxConcurrentSearchRequests()));
            }

            XContent xContent = REQUEST_BODY_CONTENT_TYPE.xContent();
            byte[] source = MultiSearchTemplateRequest.writeMultiLineFormat(multiSearchTemplateRequest, xContent);
            request.setEntity(new ByteArrayEntity(source, createContentType(xContent.type())));
            return request;
        }

        static Request explain(ExplainRequest explainRequest) throws IOException {
            Request request = new Request(HttpGet.METHOD_NAME,
                    endpoint(explainRequest.index(), explainRequest.type(), explainRequest.id(), "_explain"));

            Params params = new Params(request);
            params.withStoredFields(explainRequest.storedFields());
            params.withFetchSourceContext(explainRequest.fetchSourceContext());
            params.withRouting(explainRequest.routing());
            params.withPreference(explainRequest.preference());
            request.setEntity(createEntity(explainRequest, REQUEST_BODY_CONTENT_TYPE));
            return request;
        }

        static Request fieldCaps(FieldCapabilitiesRequest fieldCapabilitiesRequest) {
            String[] indices = fieldCapabilitiesRequest.indices();
            Request request = new Request(HttpGet.METHOD_NAME, endpoint(indices, "_field_caps"));

            Params params = new Params(request);
            params.withFields(fieldCapabilitiesRequest.fields());
            params.withIndicesOptions(fieldCapabilitiesRequest.indicesOptions());

            return request;
        }

        static Request rankEval(RankEvalRequest rankEvalRequest) throws IOException {
            Request request = new Request(HttpGet.METHOD_NAME, endpoint(rankEvalRequest.indices(), Strings.EMPTY_ARRAY, "_rank_eval"));

            Params params = new Params(request);
            params.withIndicesOptions(rankEvalRequest.indicesOptions());

            request.setEntity(createEntity(rankEvalRequest.getRankEvalSpec(), REQUEST_BODY_CONTENT_TYPE));
            return request;
        }

        static Request reindex(ReindexRequest reindexRequest) throws IOException {
            String endpoint = new EndpointBuilder().addPathPart("_reindex").build();
            Request request = new Request(HttpPost.METHOD_NAME, endpoint);
            Params params = new Params(request)
                    .withRefresh(reindexRequest.isRefresh())
                    .withTimeout(reindexRequest.getTimeout())
                    .withWaitForActiveShards(reindexRequest.getWaitForActiveShards(), ActiveShardCount.DEFAULT)
                    .withRequestsPerSecond(reindexRequest.getRequestsPerSecond());

            if (reindexRequest.getScrollTime() != null) {
                params.putParam("scroll", reindexRequest.getScrollTime());
            }
            request.setEntity(createEntity(reindexRequest, REQUEST_BODY_CONTENT_TYPE));
            return request;
        }

        static Request updateByQuery(UpdateByQueryRequest updateByQueryRequest) throws IOException {
            String endpoint =
                    endpoint(updateByQueryRequest.indices(), updateByQueryRequest.getDocTypes(), "_update_by_query");
            Request request = new Request(HttpPost.METHOD_NAME, endpoint);
            Params params = new Params(request)
                    .withRouting(updateByQueryRequest.getRouting())
                    .withPipeline(updateByQueryRequest.getPipeline())
                    .withRefresh(updateByQueryRequest.isRefresh())
                    .withTimeout(updateByQueryRequest.getTimeout())
                    .withWaitForActiveShards(updateByQueryRequest.getWaitForActiveShards(), ActiveShardCount.DEFAULT)
                    .withRequestsPerSecond(updateByQueryRequest.getRequestsPerSecond())
                    .withIndicesOptions(updateByQueryRequest.indicesOptions());
            if (updateByQueryRequest.isAbortOnVersionConflict() == false) {
                params.putParam("conflicts", "proceed");
            }
            if (updateByQueryRequest.getBatchSize() != AbstractBulkByScrollRequest.DEFAULT_SCROLL_SIZE) {
                params.putParam("scroll_size", Integer.toString(updateByQueryRequest.getBatchSize()));
            }
            if (updateByQueryRequest.getScrollTime() != AbstractBulkByScrollRequest.DEFAULT_SCROLL_TIMEOUT) {
                params.putParam("scroll", updateByQueryRequest.getScrollTime());
            }
            if (updateByQueryRequest.getSize() > 0) {
                params.putParam("size", Integer.toString(updateByQueryRequest.getSize()));
            }
            request.setEntity(createEntity(updateByQueryRequest, REQUEST_BODY_CONTENT_TYPE));
            return request;
        }

        public static Request deleteByQuery(DeleteByQueryRequest deleteByQueryRequest) throws IOException {
            String endpoint =
                    endpoint(deleteByQueryRequest.indices(), deleteByQueryRequest.getDocTypes(), "_delete_by_query");
            Request request = new Request(HttpPost.METHOD_NAME, endpoint);
            Params params = new Params(request)
                    .withRouting(deleteByQueryRequest.getRouting())
                    .withRefresh(deleteByQueryRequest.isRefresh())
                    .withTimeout(deleteByQueryRequest.getTimeout())
                    .withWaitForActiveShards(deleteByQueryRequest.getWaitForActiveShards(), ActiveShardCount.DEFAULT)
                    .withRequestsPerSecond(deleteByQueryRequest.getRequestsPerSecond())
                    .withIndicesOptions(deleteByQueryRequest.indicesOptions());
            if (deleteByQueryRequest.isAbortOnVersionConflict() == false) {
                params.putParam("conflicts", "proceed");
            }
            if (deleteByQueryRequest.getBatchSize() != AbstractBulkByScrollRequest.DEFAULT_SCROLL_SIZE) {
                params.putParam("scroll_size", Integer.toString(deleteByQueryRequest.getBatchSize()));
            }
            if (deleteByQueryRequest.getScrollTime() != AbstractBulkByScrollRequest.DEFAULT_SCROLL_TIMEOUT) {
                params.putParam("scroll", deleteByQueryRequest.getScrollTime());
            }
            if (deleteByQueryRequest.getSize() > 0) {
                params.putParam("size", Integer.toString(deleteByQueryRequest.getSize()));
            }
            request.setEntity(createEntity(deleteByQueryRequest, REQUEST_BODY_CONTENT_TYPE));
            return request;
        }

        static Request rethrottleReindex(RethrottleRequest rethrottleRequest) {
            return rethrottle(rethrottleRequest, "_reindex");
        }

        static Request rethrottleUpdateByQuery(RethrottleRequest rethrottleRequest) {
            return rethrottle(rethrottleRequest, "_update_by_query");
        }

        static Request rethrottleDeleteByQuery(RethrottleRequest rethrottleRequest) {
            return rethrottle(rethrottleRequest, "_delete_by_query");
        }

        private static Request rethrottle(RethrottleRequest rethrottleRequest, String firstPathPart) {
            String endpoint = new EndpointBuilder().addPathPart(firstPathPart).addPathPart(rethrottleRequest.getTaskId().toString())
                    .addPathPart("_rethrottle").build();
            Request request = new Request(HttpPost.METHOD_NAME, endpoint);
            Params params = new Params(request)
                    .withRequestsPerSecond(rethrottleRequest.getRequestsPerSecond());
            // we set "group_by" to "none" because this is the response format we can parse back
            params.putParam("group_by", "none");
            return request;
        }

        static Request putScript(PutStoredScriptRequest putStoredScriptRequest) throws IOException {
            String endpoint = new EndpointBuilder().addPathPartAsIs("_scripts").addPathPart(putStoredScriptRequest.id()).build();
            Request request = new Request(HttpPost.METHOD_NAME, endpoint);
            Params params = new Params(request);
            params.withTimeout(putStoredScriptRequest.timeout());
            params.withMasterTimeout(putStoredScriptRequest.masterNodeTimeout());
            if (Strings.hasText(putStoredScriptRequest.context())) {
                params.putParam("context", putStoredScriptRequest.context());
            }
            request.setEntity(createEntity(putStoredScriptRequest, REQUEST_BODY_CONTENT_TYPE));
            return request;
        }

        static Request analyze(AnalyzeRequest request) throws IOException {
            EndpointBuilder builder = new EndpointBuilder();
            String index = request.index();
            if (index != null) {
                builder.addPathPart(index);
            }
            builder.addPathPartAsIs("_analyze");
            Request req = new Request(HttpGet.METHOD_NAME, builder.build());
            req.setEntity(createEntity(request, REQUEST_BODY_CONTENT_TYPE));
            return req;
        }

        static Request getScript(GetStoredScriptRequest getStoredScriptRequest) {
            String endpoint = new EndpointBuilder().addPathPartAsIs("_scripts").addPathPart(getStoredScriptRequest.id()).build();
            Request request = new Request(HttpGet.METHOD_NAME, endpoint);
            Params params = new Params(request);
            params.withMasterTimeout(getStoredScriptRequest.masterNodeTimeout());
            return request;
        }

        static Request deleteScript(DeleteStoredScriptRequest deleteStoredScriptRequest) {
            String endpoint = new EndpointBuilder().addPathPartAsIs("_scripts").addPathPart(deleteStoredScriptRequest.id()).build();
            Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
            Params params = new Params(request);
            params.withTimeout(deleteStoredScriptRequest.timeout());
            params.withMasterTimeout(deleteStoredScriptRequest.masterNodeTimeout());
            return request;
        }

        static Request xPackWatcherPutWatch(PutWatchRequest putWatchRequest) {
            String endpoint = new EndpointBuilder()
                    .addPathPartAsIs("_xpack")
                    .addPathPartAsIs("watcher")
                    .addPathPartAsIs("watch")
                    .addPathPart(putWatchRequest.getId())
                    .build();

            Request request = new Request(HttpPut.METHOD_NAME, endpoint);
            Params params = new Params(request).withVersion(putWatchRequest.getVersion());
            if (putWatchRequest.isActive() == false) {
                params.putParam("active", "false");
            }
            ContentType contentType = createContentType(putWatchRequest.xContentType());
            BytesReference source = putWatchRequest.getSource();
            request.setEntity(new ByteArrayEntity(source.toBytesRef().bytes, 0, source.length(), contentType));
            return request;
        }

        static Request xPackWatcherDeleteWatch(DeleteWatchRequest deleteWatchRequest) {
            String endpoint = new EndpointBuilder()
                    .addPathPartAsIs("_xpack")
                    .addPathPartAsIs("watcher")
                    .addPathPartAsIs("watch")
                    .addPathPart(deleteWatchRequest.getId())
                    .build();

            Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
            return request;
        }

        static HttpEntity createEntity(ToXContent toXContent, XContentType xContentType) throws IOException {
            BytesRef source = XContentHelper.toXContent(toXContent, xContentType, false).toBytesRef();
            return new ByteArrayEntity(source.bytes, source.offset, source.length, createContentType(xContentType));
        }

        static String endpoint(String index, String type, String id) {
            return new EndpointBuilder().addPathPart(index, type, id).build();
        }

        static String endpoint(String index, String type, String id, String endpoint) {
            return new EndpointBuilder().addPathPart(index, type, id).addPathPartAsIs(endpoint).build();
        }

        static String endpoint(String[] indices) {
            return new EndpointBuilder().addCommaSeparatedPathParts(indices).build();
        }

        static String endpoint(String[] indices, String endpoint) {
            return new EndpointBuilder().addCommaSeparatedPathParts(indices).addPathPartAsIs(endpoint).build();
        }

        static String endpoint(String[] indices, String[] types, String endpoint) {
            return new EndpointBuilder().addCommaSeparatedPathParts(indices).addCommaSeparatedPathParts(types)
                    .addPathPartAsIs(endpoint).build();
        }

        static String endpoint(String[] indices, String endpoint, String[] suffixes) {
            return new EndpointBuilder().addCommaSeparatedPathParts(indices).addPathPartAsIs(endpoint)
                    .addCommaSeparatedPathParts(suffixes).build();
        }

        static String endpoint(String[] indices, String endpoint, String type) {
            return new EndpointBuilder().addCommaSeparatedPathParts(indices).addPathPartAsIs(endpoint).addPathPart(type).build();
        }

        /**
         * Returns a {@link ContentType} from a given {@link XContentType}.
         *
         * @param xContentType the {@link XContentType}
         * @return the {@link ContentType}
         */
        @SuppressForbidden(reason = "Only allowed place to convert a XContentType to a ContentType")
        public static ContentType createContentType(final XContentType xContentType) {
            return ContentType.create(xContentType.mediaTypeWithoutParameters(), (Charset) null);
        }

        /**
         * Utility class to help with common parameter names and patterns. Wraps
         * a {@link Request} and adds the parameters to it directly.
         */
        static class Params {
            private final Request request;

            Params(Request request) {
                this.request = request;
            }

            Params putParam(String name, String value) {
                if (Strings.hasLength(value)) {
                    request.addParameter(name, value);
                }
                return this;
            }

            Params putParam(String key, TimeValue value) {
                if (value != null) {
                    return putParam(key, value.getStringRep());
                }
                return this;
            }

            Params withDocAsUpsert(boolean docAsUpsert) {
                if (docAsUpsert) {
                    return putParam("doc_as_upsert", Boolean.TRUE.toString());
                }
                return this;
            }

            Params withFetchSourceContext(FetchSourceContext fetchSourceContext) {
                if (fetchSourceContext != null) {
                    if (fetchSourceContext.fetchSource() == false) {
                        putParam("_source", Boolean.FALSE.toString());
                    }
                    if (fetchSourceContext.includes() != null && fetchSourceContext.includes().length > 0) {
                        putParam("_source_include", String.join(",", fetchSourceContext.includes()));
                    }
                    if (fetchSourceContext.excludes() != null && fetchSourceContext.excludes().length > 0) {
                        putParam("_source_exclude", String.join(",", fetchSourceContext.excludes()));
                    }
                }
                return this;
            }

            Params withFields(String[] fields) {
                if (fields != null && fields.length > 0) {
                    return putParam("fields", String.join(",", fields));
                }
                return this;
            }

            Params withMasterTimeout(TimeValue masterTimeout) {
                return putParam("master_timeout", masterTimeout);
            }

            Params withParent(String parent) {
                return putParam("parent", parent);
            }

            Params withPipeline(String pipeline) {
                return putParam("pipeline", pipeline);
            }

            Params withPreference(String preference) {
                return putParam("preference", preference);
            }

            Params withRealtime(boolean realtime) {
                if (realtime == false) {
                    return putParam("realtime", Boolean.FALSE.toString());
                }
                return this;
            }

            Params withRefresh(boolean refresh) {
                if (refresh) {
                    return withRefreshPolicy(RefreshPolicy.IMMEDIATE);
                }
                return this;
            }

            /**
             * @deprecated If creating a new HLRC ReST API call, use {@link RefreshPolicy}
             * instead of {@link WriteRequest.RefreshPolicy} from the server project
             */
            @Deprecated
            Params withRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
                if (refreshPolicy != WriteRequest.RefreshPolicy.NONE) {
                    return putParam("refresh", refreshPolicy.getValue());
                }
                return this;
            }

            Params withRefreshPolicy(RefreshPolicy refreshPolicy) {
                if (refreshPolicy != RefreshPolicy.NONE) {
                    return putParam("refresh", refreshPolicy.getValue());
                }
                return this;
            }

            Params withRequestsPerSecond(float requestsPerSecond) {
                // the default in AbstractBulkByScrollRequest is Float.POSITIVE_INFINITY,
                // but we don't want to add that to the URL parameters, instead we use -1
                if (Float.isFinite(requestsPerSecond)) {
                    return putParam("requests_per_second", Float.toString(requestsPerSecond));
                } else {
                    return putParam("requests_per_second", "-1");
                }
            }

            Params withRetryOnConflict(int retryOnConflict) {
                if (retryOnConflict > 0) {
                    return putParam("retry_on_conflict", String.valueOf(retryOnConflict));
                }
                return this;
            }

            Params withRouting(String routing) {
                return putParam("routing", routing);
            }

            Params withStoredFields(String[] storedFields) {
                if (storedFields != null && storedFields.length > 0) {
                    return putParam("stored_fields", String.join(",", storedFields));
                }
                return this;
            }

            Params withTimeout(TimeValue timeout) {
                return putParam("timeout", timeout);
            }

            Params withUpdateAllTypes(boolean updateAllTypes) {
                if (updateAllTypes) {
                    return putParam("update_all_types", Boolean.TRUE.toString());
                }
                return this;
            }

            Params withVersion(long version) {
                if (version != Versions.MATCH_ANY) {
                    return putParam("version", Long.toString(version));
                }
                return this;
            }

            Params withVersionType(VersionType versionType) {
                if (versionType != VersionType.INTERNAL) {
                    return putParam("version_type", versionType.name().toLowerCase(Locale.ROOT));
                }
                return this;
            }

            Params withWaitForActiveShards(ActiveShardCount currentActiveShardCount, ActiveShardCount defaultActiveShardCount) {
                if (currentActiveShardCount != null && currentActiveShardCount != defaultActiveShardCount) {
                    return putParam("wait_for_active_shards", currentActiveShardCount.toString().toLowerCase(Locale.ROOT));
                }
                return this;
            }

            Params withIndicesOptions(IndicesOptions indicesOptions) {
                withIgnoreUnavailable(indicesOptions.ignoreUnavailable());
                putParam("allow_no_indices", Boolean.toString(indicesOptions.allowNoIndices()));
                String expandWildcards;
                if (indicesOptions.expandWildcardsOpen() == false && indicesOptions.expandWildcardsClosed() == false) {
                    expandWildcards = "none";
                } else {
                    StringJoiner joiner = new StringJoiner(",");
                    if (indicesOptions.expandWildcardsOpen()) {
                        joiner.add("open");
                    }
                    if (indicesOptions.expandWildcardsClosed()) {
                        joiner.add("closed");
                    }
                    expandWildcards = joiner.toString();
                }
                putParam("expand_wildcards", expandWildcards);
                return this;
            }

            Params withIgnoreUnavailable(boolean ignoreUnavailable) {
                // Always explicitly place the ignore_unavailable value.
                putParam("ignore_unavailable", Boolean.toString(ignoreUnavailable));
                return this;
            }

            Params withHuman(boolean human) {
                if (human) {
                    putParam("human", Boolean.toString(human));
                }
                return this;
            }

            Params withLocal(boolean local) {
                if (local) {
                    putParam("local", Boolean.toString(local));
                }
                return this;
            }

            Params withIncludeDefaults(boolean includeDefaults) {
                if (includeDefaults) {
                    return putParam("include_defaults", Boolean.TRUE.toString());
                }
                return this;
            }

            Params withPreserveExisting(boolean preserveExisting) {
                if (preserveExisting) {
                    return putParam("preserve_existing", Boolean.TRUE.toString());
                }
                return this;
            }

            Params withDetailed(boolean detailed) {
                if (detailed) {
                    return putParam("detailed", Boolean.TRUE.toString());
                }
                return this;
            }

            Params withWaitForCompletion(boolean waitForCompletion) {
                if (waitForCompletion) {
                    return putParam("wait_for_completion", Boolean.TRUE.toString());
                }
                return this;
            }

            Params withNodes(String[] nodes) {
                if (nodes != null && nodes.length > 0) {
                    return putParam("nodes", String.join(",", nodes));
                }
                return this;
            }

            Params withActions(String[] actions) {
                if (actions != null && actions.length > 0) {
                    return putParam("actions", String.join(",", actions));
                }
                return this;
            }

            Params withTaskId(TaskId taskId) {
                if (taskId != null && taskId.isSet()) {
                    return putParam("task_id", taskId.toString());
                }
                return this;
            }

            Params withParentTaskId(TaskId parentTaskId) {
                if (parentTaskId != null && parentTaskId.isSet()) {
                    return putParam("parent_task_id", parentTaskId.toString());
                }
                return this;
            }

            Params withVerify(boolean verify) {
                if (verify) {
                    return putParam("verify", Boolean.TRUE.toString());
                }
                return this;
            }

            Params withWaitForStatus(ClusterHealthStatus status) {
                if (status != null) {
                    return putParam("wait_for_status", status.name().toLowerCase(Locale.ROOT));
                }
                return this;
            }

            Params withWaitForNoRelocatingShards(boolean waitNoRelocatingShards) {
                if (waitNoRelocatingShards) {
                    return putParam("wait_for_no_relocating_shards", Boolean.TRUE.toString());
                }
                return this;
            }

            Params withWaitForNoInitializingShards(boolean waitNoInitShards) {
                if (waitNoInitShards) {
                    return putParam("wait_for_no_initializing_shards", Boolean.TRUE.toString());
                }
                return this;
            }

            Params withWaitForNodes(String waitForNodes) {
                return putParam("wait_for_nodes", waitForNodes);
            }

            Params withLevel(ClusterHealthRequest.Level level) {
                return putParam("level", level.name().toLowerCase(Locale.ROOT));
            }

            Params withWaitForEvents(Priority waitForEvents) {
                if (waitForEvents != null) {
                    return putParam("wait_for_events", waitForEvents.name().toLowerCase(Locale.ROOT));
                }
                return this;
            }
        }

        /**
         * Ensure that the {@link IndexRequest}'s content type is supported by the Bulk API and that it conforms
         * to the current {@link BulkRequest}'s content type (if it's known at the time of this method get called).
         *
         * @return the {@link IndexRequest}'s content type
         */
        static XContentType enforceSameContentType(IndexRequest indexRequest, @Nullable XContentType xContentType) {
            XContentType requestContentType = indexRequest.getContentType();
            if (requestContentType != XContentType.JSON && requestContentType != XContentType.SMILE) {
                throw new IllegalArgumentException("Unsupported content-type found for request with content-type [" + requestContentType
                        + "], only JSON and SMILE are supported");
            }
            if (xContentType == null) {
                return requestContentType;
            }
            if (requestContentType != xContentType) {
                throw new IllegalArgumentException("Mismatching content-type found for request with content-type [" + requestContentType
                        + "], previous requests have content-type [" + xContentType + "]");
            }
            return xContentType;
        }

        /**
         * Utility class to build request's endpoint given its parts as strings
         */
        static class EndpointBuilder {

            private final StringJoiner joiner = new StringJoiner("/", "/", "");

            EndpointBuilder addPathPart(String... parts) {
                for (String part : parts) {
                    if (Strings.hasLength(part)) {
                        joiner.add(encodePart(part));
                    }
                }
                return this;
            }

            EndpointBuilder addCommaSeparatedPathParts(String[] parts) {
                addPathPart(String.join(",", parts));
                return this;
            }

            EndpointBuilder addPathPartAsIs(String... parts) {
                for (String part : parts) {
                    if (Strings.hasLength(part)) {
                        joiner.add(part);
                    }
                }
                return this;
            }

            String build() {
                return joiner.toString();
            }

            private static String encodePart(String pathPart) {
                try {
                    //encode each part (e.g. index, type and id) separately before merging them into the path
                    //we prepend "/" to the path part to make this path absolute, otherwise there can be issues with
                    //paths that start with `-` or contain `:`
                    URI uri = new URI(null, null, null, -1, "/" + pathPart, null, null);
                    //manually encode any slash that each part may contain
                    return uri.getRawPath().substring(1).replaceAll("/", "%2F");
                } catch (URISyntaxException e) {
                    throw new IllegalArgumentException("Path part [" + pathPart + "] couldn't be encoded", e);
                }
            }
        }
    }


    public static class NewBusinessException extends RuntimeException {
        private int code;

        public NewBusinessException(int code, String message) {
            super(message);
            this.code = code;
        }


    }
}