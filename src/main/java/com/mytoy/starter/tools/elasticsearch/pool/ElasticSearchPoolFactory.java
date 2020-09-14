package com.mytoy.starter.tools.elasticsearch.pool;


import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.KerberosCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.ranger.plugin.client.BaseClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.*;

/**
 * EliasticSearch连接池工厂对象
 */
@Slf4j
public class ElasticSearchPoolFactory implements PooledObjectFactory<RestHighLevelClient> {

    private int connectTimeoutMillis;

    private int socketTimeoutMillis;

    private int connectionRequestTimeoutMillis;

    private int maxConnPerRoute;

    private int maxConnTotal;

    private int maxRetryTimeout;

    private Integer connectNum;

    private Integer connectPerRoute;

    private String hostList;

    private String kerberosUserPrincipal;

    private String kerberosUserKeytabPrincipal;

    private Boolean kerberosSwitch = false;

    private String krb5ConfPath;

    private Boolean rangerSwitch = false;

    private String userName;

    private String esServiceName;

    private String passWord;

    public static class Builders {

        private static ElasticSearchPoolFactory poolFactory;

        public static Builders builders() {
            poolFactory = new ElasticSearchPoolFactory();
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

        public Builders maxConnPerRoute(int maxConnPerRoute) {
            this.poolFactory.maxConnPerRoute = maxConnPerRoute;
            return this;
        }

        public Builders maxConnTotal(int maxConnTotal) {
            this.poolFactory.maxConnTotal = maxConnTotal;
            return this;
        }

        public Builders maxRetryTimeout(int maxRetryTimeout) {
            this.poolFactory.maxRetryTimeout = maxRetryTimeout;
            return this;
        }

        public Builders connectNum(Integer connectNum) {
            this.poolFactory.connectNum = connectNum;
            return this;
        }

        public Builders connectPerRoute(Integer connectPerRoute) {
            this.poolFactory.connectPerRoute = connectPerRoute;
            return this;
        }

        public Builders hostList(String hostList) {
            this.poolFactory.hostList = hostList;
            return this;
        }

        public Builders kerberosUserPrincipal(String kerberosUserPrincipal) {
            this.poolFactory.kerberosUserPrincipal = kerberosUserPrincipal;
            return this;
        }

        public Builders kerberosUserKeytabPrincipal(String kerberosUserKeytabPrincipal) {
            this.poolFactory.kerberosUserKeytabPrincipal = kerberosUserKeytabPrincipal;
            return this;
        }

        public Builders kerberosSwitch(Boolean kerberosSwitch) {
            this.poolFactory.kerberosSwitch = kerberosSwitch;
            return this;
        }

        public Builders krb5ConfPath(String krb5ConfPath) {
            this.poolFactory.krb5ConfPath = krb5ConfPath;
            return this;
        }

        public Builders rangerSwitch(Boolean rangerSwitch) {
            this.poolFactory.rangerSwitch = rangerSwitch;
            return this;
        }

        public Builders userName(String userName) {
            this.poolFactory.userName = userName;
            return this;
        }

        public Builders passWord(String passWord) {
            this.poolFactory.passWord = passWord;
            return this;
        }

        public Builders esServiceName(String esServiceName) {
            this.poolFactory.esServiceName = esServiceName;
            return this;
        }

        public ElasticSearchPoolFactory builder() {
            return this.poolFactory;
        }
    }

    private ElasticSearchPoolFactory() {

    }

    /**
     * 功能描述：激活资源对象
     * <p>
     * 什么时候会调用此方法
     * 1：从资源池中获取资源的时候
     * 2：资源回收线程，回收资源的时候，根据配置的 testWhileIdle 参数，判断 是否执行 factory.activateObject()方法，true 执行，false 不执行
     *
     * @param arg0
     */
    @Override
    public void activateObject(PooledObject<RestHighLevelClient> arg0) {
//        log.info("获取RestHighLevelClient连接客户端");
    }

    /**
     * 销毁对象
     */
    @Override
    public void destroyObject(PooledObject<RestHighLevelClient> pooledObject) throws Exception {
        RestHighLevelClient highLevelClient = pooledObject.getObject();
        highLevelClient.close();
//        log.info("销毁RestHighLevelClient连接客户端");
    }

    /**
     * 生产对象
     */
    @Override
    public PooledObject<RestHighLevelClient> makeObject() {
        RestHighLevelClient restHighLevelClient = null;
        if (StringUtils.isBlank(hostList)) return null;
        String[] esHosts = hostList.split(",");
        if (ArrayUtils.isEmpty(esHosts)) return null;
        HttpHost[] httpHosts = new HttpHost[esHosts.length];
        for (int i = 0; i < esHosts.length; i++) {
            List<String> hostUrl = Arrays.asList(esHosts[i].split(":"));
            httpHosts[i] = new HttpHost(hostUrl.get(0), Integer.parseInt(hostUrl.get(1)), "http");
        }
        if (ArrayUtils.isEmpty(esHosts)) return null;
        if (!Objects.isNull(connectNum)) maxConnTotal = connectNum;
        if (!Objects.isNull(connectPerRoute)) maxConnPerRoute = connectPerRoute;
        RestClientBuilder builder = RestClient.builder(httpHosts).setRequestConfigCallback(requestConfigBuilder -> {
            requestConfigBuilder.setConnectTimeout(connectTimeoutMillis);
            requestConfigBuilder.setSocketTimeout(socketTimeoutMillis);
            requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeoutMillis);
            return requestConfigBuilder;
        }).setMaxRetryTimeoutMillis(maxRetryTimeout);
        try {
            if (rangerSwitch) {
                class NewBaseClient extends BaseClient {
                    public NewBaseClient(String svcName, Map<String, String> connectionProperties) {
                        super(svcName, connectionProperties);
                    }

                    @Override
                    protected Subject getLoginSubject() {
                        return super.getLoginSubject();
                    }
                }
                HashMap<String, String> configs = new HashMap<String, String>() {{
                    put("elasticsearch.url", esHosts[0]);
                    put("username", userName);
                    put("password", passWord);
                }};
//                Map<String, Object> validateConfig = ElasticsearchResourceMgr.validateConfig(esServiceName, configs);
//                log.info(JSON.toJSONString(validateConfig));
                Subject serviceSubject = new NewBaseClient(esServiceName, configs).getLoginSubject();
                builder.setHttpClientConfigCallback(clientBuilder -> {
                    byte[] passwordBytes = passWord.getBytes(Charset.forName("iso-8859-1"));
                    byte[] prefix = (userName + ":").getBytes(Charset.forName("iso-8859-1"));
                    byte[] usernamePassword = new byte[prefix.length + passwordBytes.length];
                    System.arraycopy(prefix, 0, usernamePassword, 0, prefix.length);
                    System.arraycopy(passwordBytes, 0, usernamePassword, prefix.length, passwordBytes.length);
                    Header authorizationHeader = null;
                    try {
                        authorizationHeader = new BasicHeader("Authorization", "Basic " + new String(org.apache.xmlbeans.impl.util.Base64.encode(usernamePassword), "ASCII"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    clientBuilder.setDefaultHeaders(Lists.newArrayList(authorizationHeader));
                    return clientBuilder;
                });
                restHighLevelClient = Subject.doAs(serviceSubject, (PrivilegedExceptionAction<RestHighLevelClient>) () -> new RestHighLevelClient(builder));
                log.info("成功创建ranger加密的RestHighLevelClient的客户端");
            } else if (kerberosSwitch) {
                System.setProperty("java.security.krb5.conf", krb5ConfPath);
                builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @SneakyThrows
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        LoginContext loginContext = AccessController.doPrivileged((PrivilegedExceptionAction<LoginContext>) () -> {
                            Subject subject = new Subject(false, Collections.singleton(new KerberosPrincipal(kerberosUserPrincipal)), new HashSet<>(), new HashSet<>());
                            Configuration config = new Configuration() {
                                @SuppressWarnings("serial")
                                @Override
                                public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                                    return new AppConfigurationEntry[]{new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                                            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, new HashMap<String, Object>() {
                                        {
                                            put("useTicketCache", "true");
                                            put("useKeyTab", "true");
                                            put("keyTab", kerberosUserKeytabPrincipal);
                                            put("refreshKrb5Config", "true");
                                            put("principal", kerberosUserPrincipal);
                                            put("storeKey", "true");
                                            put("doNotPrompt", "true");
                                            put("isInitiator", "true");
                                            put("debug", "true");
                                        }
                                    })};
                                }
                            };
                            LoginContext context = new LoginContext("ESClientLoginConf", subject, null, config);
                            context.login();
                            return context;
                        });
                        GSSCredential credential = Subject.doAs(loginContext.getSubject(), (PrivilegedExceptionAction<GSSCredential>) () -> GSSManager
                                .getInstance()
                                .createCredential(null, GSSCredential.DEFAULT_LIFETIME, new Oid("1.3.6.1.5.5.2"), GSSCredential.INITIATE_ONLY));
                        httpClientBuilder.setDefaultCredentialsProvider(new CredentialsProvider() {
                            private AuthScope authScope;
                            private Credentials credentials;

                            @Override
                            public void setCredentials(AuthScope authscope, Credentials credentials) {
                                this.authScope = new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM, AuthSchemes.SPNEGO);
                                if (authscope.getScheme().regionMatches(true, 0, AuthSchemes.SPNEGO, 0, AuthSchemes.SPNEGO.length()) == false)
                                    throw new IllegalArgumentException("Only " + AuthSchemes.SPNEGO + " auth scheme is supported in AuthScope");
                                this.credentials = new KerberosCredentials(credential);
                            }

                            @Override
                            public Credentials getCredentials(AuthScope authscope) {
                                assert this.authScope != null && authscope != null;
                                return authscope.match(this.authScope) > -1 ? this.credentials : null;
                            }

                            @Override
                            public void clear() {
                                this.authScope = null;
                                this.credentials = null;
                            }
                        });
                        httpClientBuilder.setDefaultAuthSchemeRegistry(RegistryBuilder.<AuthSchemeProvider>create().register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory()).build());
                        SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
                        sslContextBuilder.loadTrustMaterial(null, new TrustAllStrategy());
                        httpClientBuilder.setSSLContext(sslContextBuilder.build());
                        httpClientBuilder.setSSLStrategy(new SSLIOSessionStrategy(sslContextBuilder.build(), new NoopHostnameVerifier()));
                        return httpClientBuilder;
                    }
                });
                restHighLevelClient = new RestHighLevelClient(builder);
                log.info("成功创建ranger加密的RestHighLevelClient的客户端");
            } else {
                builder.setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                    httpAsyncClientBuilder.setMaxConnTotal(maxConnTotal);
                    httpAsyncClientBuilder.setMaxConnPerRoute(maxConnPerRoute);
                    return httpAsyncClientBuilder;
                });
                restHighLevelClient = new RestHighLevelClient(builder);
                log.info("成功创建未加密的RestHighLevelClient的客户端");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new DefaultPooledObject<>(restHighLevelClient);
    }

    /**
     * 功能描述：钝化资源对象
     * <p>
     * 什么时候会调用此方法
     * 1：将资源返还给资源池时，调用此方法。
     */
    @Override
    public void passivateObject(PooledObject<RestHighLevelClient> pooledObject) {
//        log.info("将RestHighLevelClient还给连接池");
    }

    /**
     * 功能描述：判断资源对象是否有效，有效返回 true，无效返回 false
     * <p>
     * 什么时候会调用此方法
     * 1：从资源池中获取资源的时候，参数 testOnBorrow 或者 testOnCreate 中有一个 配置 为 true 时，则调用  factory.validateObject() 方法
     * 2：将资源返还给资源池的时候，参数 testOnReturn，配置为 true 时，调用此方法
     * 3：资源回收线程，回收资源的时候，参数 testWhileIdle，配置为 true 时，调用此方法
     */
    @Override
    public boolean validateObject(PooledObject<RestHighLevelClient> pooledObject) {
        return true;
    }
}
