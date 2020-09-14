package com.mytoy.starter.config;

import com.mytoy.starter.tools.elasticsearch.pool.ElasticSearchPoolFactory;
import com.mytoy.starter.tools.esquery.ESQueryExecute;
import com.mytoy.starter.tools.esquery.ESQueryExecuteBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.DefaultResourceLoader;

import java.io.IOException;

/**
 * ElasticSearch 连接池工具类
 */
@Configuration
@ConfigurationProperties(prefix = "es.client")
@ConditionalOnProperty(prefix = "es.client", name = "pool", havingValue = "true")
@Slf4j
public class ElasticSearchPool {
    public Integer connectTimeoutMillis;
    public Integer socketTimeoutMillis;//es查询超时时间
    public Integer connectionRequestTimeoutMillis;//es请求超时时间
    public Integer maxConnPerRoute;
    public Integer maxConnTotal;
    public Integer maxRetryTimeout;//es查询最大重试时间
    private Integer connectNum;//连接池的最大连接数默认为10
    private Integer connectPerRoute;//单个主机最大连接数
    private String hostList;//es主机地址
    private Integer maxTotal;//maxTotal: 链接池中最大连接数,默认为8.
    private Integer maxIdle; //maxIdle: 链接池中最大空闲的连接数,默认为8.
    private Integer minIdle; //minIdle: 连接池中最少空闲的连接数,默认为0.
    private Long maxWait;   //maxWait: 当连接池资源耗尽时，调用者最大阻塞的时间，超时将跑出异常。单位，毫秒数;默认为-1.表示永不超时.
    private Long minEvictableIdleTimeMillis;    //minEvictableIdleTimeMillis: 连接空闲的最小时间，达到此值后空闲连接将可能会被移除。负值(-1)表示不移除。
    private Long softMinEvictableIdleTimeMillis; //softMinEvictableIdleTimeMillis: 连接空闲的最小时间，达到此值后空闲链接将会被移除，且保留“minIdle”个空闲连接数。默认为-1.
    private Long numTestsPerEvictionRun; //numTestsPerEvictionRun: 对于“空闲链接”检测线程而言，每次检测的链接资源的个数。默认为3.
    private Boolean testOnBorrow;//testOnBorrow: 向调用者输出“链接”资源时，是否检测是有有效，如果无效则从连接池中移除，并尝试获取继续获取。默认为false。建议保持默认值.
    private Boolean testOnReturn;//testOnReturn:  向连接池“归还”链接时，是否检测“链接”对象的有效性。默认为false。建议保持默认值.
    private Boolean testWhileIdle;  //testWhileIdle:  向调用者输出“链接”对象时，是否检测它的空闲超时；默认为false。如果“链接”空闲超时，将会被移除。建议保持默认值.
    private Long timeBetweenEvictionRunsMillis;  //timeBetweenEvictionRunsMillis:  “空闲链接”检测线程，检测的周期，毫秒数。如果为负值，表示不运行“检测线程”。默认为-1.
    private Integer whenExhaustedAction;  //whenExhaustedAction: 当“连接池”中active数量达到阀值时，即“链接”资源耗尽时，连接池需要采取的手段, 默认为1： 0 : 抛出异常， 1 : 阻塞，直到有可用链接资源 2 : 强制创建新的链接资源
    private Boolean kerberosSwitch;
    private String kerberosUserPrincipal;
    private String kerberosUserKeytabPrincipal;
    private Boolean rangerSwitch;
    private String userName;
    private String password;
    private String esServiceName;
    private Boolean printStackTrace;

    @Bean("esQueryExecute")
    public ESQueryExecute getClientPool() {
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(maxTotal);
        poolConfig.setMaxIdle(maxIdle);
        poolConfig.setJmxEnabled(false);
        poolConfig.setTestOnBorrow(testOnBorrow);
        poolConfig.setTestOnReturn(testOnReturn);
        poolConfig.setTestWhileIdle(testWhileIdle);
        String krb5ConfPath = "";
        if (kerberosSwitch)
            try {
                krb5ConfPath = new DefaultResourceLoader().getResource("krb5.conf").getFile().getAbsolutePath();
            } catch (IOException e) {
                log.error("获取krb5.conf配置文件失败");
            }
        ElasticSearchPoolFactory elasticSearchPoolFactory = ElasticSearchPoolFactory.Builders.builders()
                .connectTimeoutMillis(connectTimeoutMillis)
                .socketTimeoutMillis(socketTimeoutMillis)
                .connectionRequestTimeoutMillis(connectionRequestTimeoutMillis)
                .maxConnPerRoute(maxConnPerRoute)
                .maxConnTotal(maxConnTotal)
                .maxRetryTimeout(maxRetryTimeout)
                .connectNum(connectNum)
                .connectPerRoute(connectPerRoute)
                .hostList(hostList)
                .kerberosSwitch(kerberosSwitch)
                .kerberosUserPrincipal(kerberosUserPrincipal)
                .kerberosUserKeytabPrincipal(kerberosUserKeytabPrincipal)
                .krb5ConfPath(krb5ConfPath)
                .rangerSwitch(rangerSwitch)
                .userName(userName)
                .esServiceName(esServiceName)
                .passWord(password)
                .builder();
        GenericObjectPool<RestHighLevelClient> clientPool = new GenericObjectPool<>(elasticSearchPoolFactory, poolConfig);
        ESQueryExecute esQueryExecute = ESQueryExecuteBuilder.Builders.builders().connectTimeoutMillis(connectTimeoutMillis)
                .socketTimeoutMillis(socketTimeoutMillis)
                .connectionRequestTimeoutMillis(connectionRequestTimeoutMillis)
                .maxRetryTimeout(maxRetryTimeout)
                .hostList(hostList)
                .genericObjectPool(clientPool)
                .printStackTraceButton(printStackTrace)
                .builder();
        return esQueryExecute;
    }
}
