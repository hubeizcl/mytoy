package com.mytoy.starter.tools.elasticsearch.builder;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.mytoy.starter.tools.MyString;
import com.mytoy.starter.tools.MyArrays;
import com.mytoy.starter.tools.MyCollection;
import com.mytoy.starter.tools.MyMap;
import javafx.util.Pair;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.nio.charset.Charset;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SearchScrollRequestBuilder {

    public static class Utils {

        private static List<SearchHit> searchHits;

        public static Utils builders() {
            return new Utils();
        }

        public Utils searchHits(List<SearchHit> searchHits) {
            this.searchHits = searchHits;
            return this;
        }

        public <T> List<T> parse(BiFunction<SearchHit, Class<T>, T> function, Class<T> type) {
            if (null == searchHits || searchHits.size() <= 0) return null;
            List<T> collect = searchHits.stream().map(vo -> function.apply(vo, type)).filter(vo -> null != vo).collect(Collectors.toList());
            return collect;
        }

        public List<Map<String, Object>> parse() {
            if (null == searchHits || searchHits.size() <= 0) return null;
            List<Map<String, Object>> collect = searchHits.stream().map(SearchHit::getSourceAsMap).collect(Collectors.toList());
            return collect;
        }

        public Map<String, Object> resultMapWarp() {
            int total = 0;
            List<Map<String, Object>> parse = new ArrayList<>();
            List<Map<String, Object>> parse2 = parse();
            if (MyCollection.isNotEmpty(parse2)) {
                parse = parse2;
                total = parse.size();
            }
            return MyMap.<String, Object>builder().of("total", total).of("list", parse).build();

        }
        public static final Function<SearchResponse, Boolean> judgmentSearchResponse = vo -> {
            if (null == vo) return false;
            SearchHits hits = vo.getHits();
            if (null == hits) return false;
            SearchHit[] hits1 = hits.getHits();
            if (null == hits1) return false;
            return true;
        };
    }

    private SearchResponse searchResponse;

    private Scroll scroll;

    private Long total = 10000l;//最多查询到多少条停止

    private Integer multiple = 1;//倍数,预计是总数的多少倍数据才能过滤得到想要的数量

    private Double errorRate = 0.5;//准确度 0.0-1.0之间，越准确，值越小，但也越消耗内存

    private Function<SearchHit, Boolean> filter;//对查找的结果进行过滤

    private boolean bloom = true;

    private SearchScrollRequestBuilder() {

    }

    public static SearchScrollRequestBuilder builders() {
        return new SearchScrollRequestBuilder();
    }

    public SearchScrollRequestBuilder searchResponse(SearchResponse searchResponse) {
        this.searchResponse = searchResponse;
        return this;
    }

    public SearchScrollRequestBuilder scroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    public SearchScrollRequestBuilder total(Long total) {
        this.total = total;
        return this;
    }

    public SearchScrollRequestBuilder multiple(Integer multiple) {
        this.multiple = multiple;
        return this;
    }

    public SearchScrollRequestBuilder errorRate(Double errorRate) {
        this.errorRate = errorRate;
        return this;
    }

    public SearchScrollRequestBuilder filter(Function<SearchHit, Boolean> filterFunction) {
        this.filter = filterFunction;
        return this;
    }

    public SearchScrollRequestBuilder bloom(boolean bloom) {
        this.bloom = bloom;
        return this;
    }

    public List<SearchHit> searchScroll(BiFunction<SearchScrollRequest, RequestOptions, SearchResponse> scrollFunction, BiFunction<ClearScrollRequest, RequestOptions, ClearScrollResponse> clearFunction) {
        if (!Utils.judgmentSearchResponse.apply(searchResponse)) return new ArrayList<>();
        if (null == scroll) scroll = new Scroll(TimeValue.timeValueMinutes(10));
        String scrollId = searchResponse.getScrollId();
        List<SearchResponse> searchResponseList = new ArrayList<>();
        searchResponseList.add(searchResponse);
        int sum = searchResponse.getHits().getHits().length;
        while (true) {
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
            searchScrollRequest.scroll(scroll);
            SearchResponse searchResponse = scrollFunction.apply(searchScrollRequest, RequestOptions.DEFAULT);
            if (!Utils.judgmentSearchResponse.apply(searchResponse)) break;
            long num = searchResponse.getHits().getHits().length;
            if (0l == num) break;//没有数据了退出
            sum += num;
            if (sum > total) break;//查询深度超过阈值退出
            searchResponseList.add(searchResponse);
        }
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        clearFunction.apply(clearScrollRequest, RequestOptions.DEFAULT);
        List<SearchHit> list = new ArrayList<>();
        Optional<SearchHit[]> collect = searchResponseList.stream().map(vo -> {
            if (Utils.judgmentSearchResponse.apply(vo)) return vo.getHits().getHits();
            return null;
        }).filter(vo -> null != vo).collect(Collectors.reducing((a, b) -> MyArrays.merge(a, b, SearchHit.class)));
        if (collect.isPresent()) list = Arrays.asList(collect.get());
        return list;
    }

    public List<SearchHit> searchScan(BiFunction<SearchScrollRequest, RequestOptions, SearchResponse> scrollFunction, BiFunction<ClearScrollRequest, RequestOptions, ClearScrollResponse> clearFunction) {
        if (null == scroll) scroll = new Scroll(TimeValue.timeValueMinutes(10));
        String scrollId = searchResponse.getScrollId();
        List<SearchResponse> searchResponseList = new ArrayList<>();
        int sum = 0;
        while (true) {
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
            searchScrollRequest.scroll(scroll);
            SearchResponse searchResponse = scrollFunction.apply(searchScrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            if (!Utils.judgmentSearchResponse.apply(searchResponse)) break;
            long num = searchResponse.getHits().getHits().length;
            if (0l == num) break;//没有数据了退出
            sum += num;
            if (sum > total) break;//查询深度超过阈值退出
            searchResponseList.add(searchResponse);
        }
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        clearFunction.apply(clearScrollRequest, RequestOptions.DEFAULT);
        List<SearchHit> list = new ArrayList<>();
        Optional<SearchHit[]> collect = searchResponseList.stream().map(vo -> {
            if (Utils.judgmentSearchResponse.apply(vo)) return vo.getHits().getHits();
            return null;
        }).filter(vo -> null != vo).collect(Collectors.reducing((a, b) -> MyArrays.merge(a, b, SearchHit.class)));
        if (collect.isPresent()) list = Arrays.asList(collect.get());
        return list;
    }

    public List<Map<String, Object>> searchScrollByDynamic(BiFunction<SearchScrollRequest, RequestOptions, SearchResponse> scrollFunction, BiFunction<ClearScrollRequest, RequestOptions, ClearScrollResponse> clearFunction, Function<SearchHit, String> parseFunction) {
        //使用布隆过滤器进行预过滤，加快处理速度
        BloomFilter<String> bloomFilter = null;
        if (bloom)
            bloomFilter = BloomFilter.create((Funnel<String>) (str, sink) -> sink.putString(str, Charset.forName("UTF-8")), Math.toIntExact((long) (this.total * multiple)), errorRate);
        if (!Utils.judgmentSearchResponse.apply(searchResponse)) return new ArrayList<>();
        if (null == scroll) scroll = new Scroll(TimeValue.timeValueMinutes(10));
        String scrollId = searchResponse.getScrollId();
        List<Map<String, Object>> result = new ArrayList<>();
        long filterNum = 0l;
        Set<String> set = new HashSet<>();
        BloomFilter<String> finalBloomFilter = bloomFilter;
        Function<SearchResponse, Pair<Long, List<Map<String, Object>>>> function = searchResponse -> {
            SearchHit[] hits = searchResponse.getHits().getHits();
            long count = 0l;
            List<Map<String, Object>> list = new ArrayList<>();
            for (SearchHit hit : hits) {
                String keyWord = parseFunction.apply(hit);
                if (MyString.isNotBlank(keyWord)) {
                    if (bloom) {
                        if (!finalBloomFilter.mightContain(keyWord)) {//先用布隆过滤器过滤一遍，加快处理速度
                            finalBloomFilter.put(keyWord);
                            if (!set.contains(keyWord)) {//在用set做精确过滤
                                set.add(keyWord);
                                list.add(hit.getSourceAsMap());
                                count++;
                            }
                        }
                    } else {
                        if (!set.contains(keyWord)) {//在用set做精确过滤
                            set.add(keyWord);
                            list.add(hit.getSourceAsMap());
                            count++;
                        }
                    }

                }
            }
            return new Pair<>(count, list);
        };
        Pair<Long, List<Map<String, Object>>> apply = function.apply(searchResponse);
        filterNum += apply.getKey();
        result.addAll(apply.getValue());
        if (filterNum < total) {
            while (true) {
                SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
                searchScrollRequest.scroll(scroll);
                SearchResponse searchResponse = scrollFunction.apply(searchScrollRequest, RequestOptions.DEFAULT);
                if (!Utils.judgmentSearchResponse.apply(searchResponse)) break;
                Pair<Long, List<Map<String, Object>>> pair = function.apply(searchResponse);
                long num = searchResponse.getHits().getHits().length;
                if (0l == num) break;//没有数据了退出
                filterNum += pair.getKey();
                result.addAll(pair.getValue());
                if (filterNum > total) break;//查询深度超过阈值退出
            }
        }
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        clearFunction.apply(clearScrollRequest, RequestOptions.DEFAULT);
        List<Map<String, Object>> subList = result.subList(0, result.size() > Math.toIntExact(total) ? Math.toIntExact(total) : result.size());
        return subList;
    }
}
