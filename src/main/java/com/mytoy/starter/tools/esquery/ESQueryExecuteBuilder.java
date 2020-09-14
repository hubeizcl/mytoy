package com.mytoy.starter.tools.esquery;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.Arrays;
import java.util.List;

public class ESQueryExecuteBuilder {

    Integer connectTimeoutMillis;

    Integer socketTimeoutMillis;

    Integer connectionRequestTimeoutMillis;

    Integer maxRetryTimeout;

    String hostList;

    Boolean printStackTraceButton = true;

    Boolean switchFilterButton = false;

    Boolean esQueryCheckButton = false;

    Boolean commConvertButton = false;

    GenericObjectPool<RestHighLevelClient> genericObjectPool;

    ESQueryExecute esQueryExecute;


    public static class Builders {

        private static ESQueryExecuteBuilder poolFactory;

        public static Builders builders() {
            poolFactory = new ESQueryExecuteBuilder();
            return new Builders();
        }

        public Builders connectTimeoutMillis(int connectTimeoutMillis) {
            this.poolFactory.connectTimeoutMillis = connectTimeoutMillis;
            return this;
        }

        public Builders socketTimeoutMillis(int socketTimeoutMillis) {
            this.poolFactory.socketTimeoutMillis = socketTimeoutMillis;
            return this;
        }

        public Builders connectionRequestTimeoutMillis(int connectionRequestTimeoutMillis) {
            this.poolFactory.connectionRequestTimeoutMillis = connectionRequestTimeoutMillis;
            return this;
        }

        public Builders maxRetryTimeout(int maxRetryTimeout) {
            this.poolFactory.maxRetryTimeout = maxRetryTimeout;
            return this;
        }

        public Builders genericObjectPool(GenericObjectPool<RestHighLevelClient> genericObjectPool) {
            this.poolFactory.genericObjectPool = genericObjectPool;
            return this;
        }

        public Builders hostList(String hostList) {
            this.poolFactory.hostList = hostList;
            return this;
        }

        public Builders switchFilterButton(Boolean switchFilterButton) {
            this.poolFactory.switchFilterButton = switchFilterButton;
            return this;
        }

        public Builders esQueryCheckButton(Boolean esQueryCheckButton) {
            this.poolFactory.esQueryCheckButton = esQueryCheckButton;
            return this;
        }

        public Builders printStackTraceButton(Boolean printStackTraceButton) {
            this.poolFactory.printStackTraceButton = printStackTraceButton;
            return this;
        }

        public Builders commConvertButton(Boolean commConvertButton) {
            this.poolFactory.commConvertButton = commConvertButton;
            return this;
        }

        public ESQueryExecute builder() {
            poolFactory.init();
            return this.poolFactory.esQueryExecute;
        }
    }

    public void init() {
        if (StringUtils.isBlank(hostList)) return;
        String[] esHosts = hostList.split(",");
        if (ArrayUtils.isEmpty(esHosts)) return;
        HttpHost[] httpHosts = new HttpHost[esHosts.length];
        for (int i = 0; i < esHosts.length; i++) {
            List<String> hostUrl = Arrays.asList(esHosts[i].split(":"));
            httpHosts[i] = new HttpHost(hostUrl.get(0), Integer.parseInt(hostUrl.get(1)), "http");
        }
        if (ArrayUtils.isEmpty(esHosts)) return;
        RestClientBuilder clientBuilder = RestClient.builder(httpHosts)
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(connectTimeoutMillis).setSocketTimeout(socketTimeoutMillis).setConnectionRequestTimeout(connectionRequestTimeoutMillis))
                .setMaxRetryTimeoutMillis(maxRetryTimeout);
        esQueryExecute = ESQueryExecute.Builders.builder()
                .genericObjectPool(genericObjectPool)
                .hosts(hostList)
                .printStackTraceButton(printStackTraceButton)
                .switchFilterButton(switchFilterButton)
                .esQueryCheckButton(esQueryCheckButton)
                .commConvertButton(commConvertButton)
                .restClientBuilder(clientBuilder).build();
        boolean isSeven = esQueryExecute.isSeven();
        esQueryExecute.setSeven(isSeven);
    }
}