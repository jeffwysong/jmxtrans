package com.googlecode.jmxtrans.model.output;

import com.googlecode.jmxtrans.model.Query;
import com.googlecode.jmxtrans.model.Result;
import com.googlecode.jmxtrans.util.BaseOutputWriter;
import com.googlecode.jmxtrans.util.IoUtils2;
import com.googlecode.jmxtrans.util.LifecycleException;
import com.googlecode.jmxtrans.util.ValidationException;
import org.codehaus.jackson.Base64Variants;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <a href="https://copperegg.com//">CopperEgg Metrics</a> implementation of the {@linkplain com.googlecode.jmxtrans.util.BaseOutputWriter}.
 * <p/>
 * This implementation uses v2 of the CopperEgg API <a href="http://dev.copperegg.com/">
 * <p/>
 * Settings:
 * <ul>
 * <li>"{@code url}": CopperEgg API server URL.
 * Optional, default value: {@value #DEFAULT_COPPEREGG_API_URL}.</li>
 * <li>"{@code user}": CopperEgg user. Mandatory</li>
 * <li>"{@code token}": CopperEgg APIKEY. Mandatory</li>
 * <li>"{@code coppereggApiTimeoutInMillis}": read timeout of the calls to CopperEgg HTTP API.
 * Optional, default value: {@value #DEFAULT_COPPEREGG_API_TIMEOUT_IN_MILLIS}.</li>
 * <li>"{@code enabled}": flag to enable/disable the writer. Optional, default value: <code>true</code>.</li>
 * <li>"{@code source}": CopperEgg . Optional, default value: {@value #DEFAULT_SOURCE} (the hostname of the server).</li>
 * </ul>
 * LibratoWriter.java author:
 * @author <a href="mailto:cleclerc@cloudbees.com">Cyrille Le Clerc</a>
 *
 * jmxtrans-embedded CopperEggWriter.java was derived from LibratoWriter.java
 * @author <a href="mailto:sjohnson@copperegg.com">Scott Johnson</a>
 *
 * CopperEggWriter.java was derived from jmxtrans-embedded CopperEggWriter.java
 * @author Jeff Wysong
 */
public class CopperEggWriter extends BaseOutputWriter {

    public final static String SETTING_URL = "url";
    public final static String SETTING_USERNAME = "username";
    public final static String SETTING_TOKEN = "token";
    public final static String SETTING_PORT = "port";
    public final static String SETTING_HOST = "host";
    public final static String SETTING_PROXY_PORT = "proxyPort";
    public final static String SETTING_PROXY_HOST = "proxyHost";
    public final static String SETTING_NAME_PREFIX = "namePrefix";

    public static final String METRIC_TYPE_GAUGE = "gauge";
    public static final String METRIC_TYPE_COUNTER = "counter";
    public static final String DEFAULT_COPPEREGG_API_URL = "https://api.copperegg.com/v2/revealmetrics";
    public static final String SETTING_COPPEREGG_API_TIMEOUT_IN_MILLIS = "coppereggApiTimeoutInMillis";
    public static final int DEFAULT_COPPEREGG_API_TIMEOUT_IN_MILLIS = 20000;
    public static final String SETTING_SOURCE = "source";
    public static final String DEFAULT_SOURCE = "#hostname#";
    private final static String DEFAULT_COPPEREGG_CONFIGURATION_PATH = "classpath:copperegg_config.json";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final AtomicInteger exceptionCounter = new AtomicInteger();
    private JsonFactory jsonFactory = new JsonFactory();

    private URL url;
    private String url_str;
    private int coppereggApiTimeoutInMillis = DEFAULT_COPPEREGG_API_TIMEOUT_IN_MILLIS;
    private long myPID = 0;
    private String myhost;
    private String myPID_host;
    private String config_location;

    private static Map<String, String> dashMap = new HashMap<String, String>();
    private static Map<String, String> metricgroupMap = new HashMap<String, String>();

    private String jvm_os_groupID = null;
    private String jvm_gc_groupID = null;
    private String jvm_runtime_groupID = null;
    private String jvm_class_groupID = null;
    private String jvm_thread_groupID = null;
    private String heap_metric_groupID = null;
    private String nonheap_metric_groupID = null;
    private String tomcat_thread_pool_groupID = null;
    private String tomcat_grp_groupID = null;
    private String tomcat_manager_groupID = null;
    private String tomcat_servlet_groupID = null;
    private String tomcat_db_groupID = null;
    private String jmxtrans_metric_groupID = null;
    private String app_groupID = null;
    private String app_sales_groupID = null;
//    private String cassandra_groupID = "cassandra_metrics2";
    private String cassandra_groupID = "jeff_test2";

    private static AtomicInteger counter = new AtomicInteger(0);

    /**
     * CopperEgg API authentication username
     */
    private String user;
    /**
     * CopperEgg APIKEY
     */
    private String token;
    private String basicAuthentication;
    /**
     * Optional proxy for the http API calls
     */
    private Proxy proxy;
    /**
     * CopperEgg measurement property 'source',
     */
    private String source;

    @Override
    public void start() throws LifecycleException {
        config_location = DEFAULT_COPPEREGG_CONFIGURATION_PATH;
        String path = config_location.substring("classpath:".length());
        logger.error("Config_location: {} and classpath.length() {}", config_location, "classpath:".length());
        logger.error("Path : {}", path);

        long thisPID = getPID();
        if( myPID == thisPID) {
            logger.info("Started from two threads with the same PID, {}",thisPID);
            return;
        }
        myPID = thisPID;
        try {
            String str = String.valueOf(myPID);
            url_str = getStringSetting(SETTING_URL, DEFAULT_COPPEREGG_API_URL);
            url = new URL(url_str);
            user = getStringSetting(SETTING_USERNAME);
            token = getStringSetting(SETTING_TOKEN);
            logger.error("token is {}", token);
            user = token;
            basicAuthentication = Base64Variants.getDefaultVariant().encode((user + ":" + "U").getBytes(Charset.forName("US-ASCII")));

            if (getStringSetting(SETTING_PROXY_HOST, null) != null && !getStringSetting(SETTING_PROXY_HOST).isEmpty()) {
                proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(getStringSetting(SETTING_PROXY_HOST), getIntSetting(SETTING_PROXY_PORT)));
            }

            coppereggApiTimeoutInMillis = getIntSetting(SETTING_COPPEREGG_API_TIMEOUT_IN_MILLIS, DEFAULT_COPPEREGG_API_TIMEOUT_IN_MILLIS);

            source = getStringSetting(SETTING_SOURCE, DEFAULT_SOURCE);
            //source = getStrategy().resolveExpression(source);
            myhost = source;
            myPID_host = myhost + '.' + str;

            try{
                Enumeration<URL> e = Thread.currentThread().getContextClassLoader().getResources("");
                while (e.hasMoreElements())
                {
                    logger.error("ClassLoader Resource: " + e.nextElement());
                }
                logger.error("Class Resource: " + CopperEggWriter.class.getResource("/"));


                InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
                if(in == null) {
                    logger.warn("No file found for " + path);
                    in = Thread.currentThread().getContextClassLoader().getResourceAsStream("classpath:copperegg_config.json");
                    if (in == null) {
                        logger.warn("No file found for classpath:copperegg_config.json either.");
                    } else {
                        logger.error("FILE WAS FOUND!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!########################");
                        read_config(in);
                    }
                } else {
                    logger.error("FILE WAS FOUND!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    read_config(in);
                }
            } catch (Exception e){
                exceptionCounter.incrementAndGet();
                logger.warn("Exception in start " + e);
            }

            ensure_metric_groups();
            ensure_dashboards();


            logger.info("jvm_os_groupID : {}", jvm_os_groupID);
            logger.info("jvm_gc_groupID : {}", jvm_gc_groupID);
            logger.info("jvm_runtime_groupID : {}", jvm_runtime_groupID);
            logger.info("jvm_class_groupID : {}", jvm_class_groupID);
            logger.info("jvm_thread_groupID : {}", jvm_thread_groupID);
            logger.info("heap_metric_groupID : {}", heap_metric_groupID);
            logger.info("nonheap_metric_groupID : {}", nonheap_metric_groupID );
            logger.info("tomcat_thread_pool_groupID : {}",tomcat_thread_pool_groupID);
            logger.info("tomcat_grp_groupID : {}",tomcat_grp_groupID);
            logger.info("tomcat_servlet_groupID : {}", tomcat_servlet_groupID );
            logger.info("tomcat_manager_groupID : {}",tomcat_manager_groupID );
            logger.info("tomcat_db_groupID  : {}", tomcat_db_groupID);
            logger.info("jmxtrans_metric_groupID : {}", jmxtrans_metric_groupID);
            logger.info("app_groupID : {}", app_groupID );
            logger.info("app_sales_groupID : {}", app_sales_groupID );
            logger.info("cassandra_groupID : {}", cassandra_groupID );



            //logger.info("Started CopperEggWriter Successfully on jvm '{}', connected to '{}', proxy {}", myPID_host, url, proxy);
        } catch (MalformedURLException e) {
            exceptionCounter.incrementAndGet();
            throw new IllegalArgumentException("Invalid gateway URL passed " + url);
        }
    }

    @Override
    public void doWrite(Query query) throws Exception {

        List<Result> results = query.getResults();

        List<Result> jvm_os_counters = new ArrayList<Result>();
        List<Result> jvm_gc_counters = new ArrayList<Result>();
        List<Result> jvm_runtime_counters = new ArrayList<Result>();
        List<Result> jvm_class_counters = new ArrayList<Result>();
        List<Result> jvm_thread_counters = new ArrayList<Result>();
        List<Result> heap_counters = new ArrayList<Result>();
        List<Result> nonheap_counters = new ArrayList<Result>();
        List<Result> tomcat_thread_pool_counters = new ArrayList<Result>();
        List<Result> tomcat_grp_counters = new ArrayList<Result>();
        List<Result> tomcat_manager_counters = new ArrayList<Result>();
        List<Result> tomcat_servlet_counters = new ArrayList<Result>();
        List<Result> tomcat_db_counters = new ArrayList<Result>();
        List<Result> jmxtrans_counters = new ArrayList<Result>();
        List<Result> app_counters = new ArrayList<Result>();
        List<Result> app_sales_counters = new ArrayList<Result>();
        List<Result> cassandra_counters = new ArrayList<Result>();


        long epochInMillis = 0;
        String myname =  null;
        Object myval = null;
        long thisPID = 0;
        String tmp = null;
        String pidHost = null;

        String delims = "[.]";
        for (Result result : results) {
            epochInMillis = result.getEpoch();
            myname =  result.getTypeName();
            myval = result.getValues();
            String valstr = myval.toString();
            thisPID = getPID();
            tmp = String.valueOf(thisPID);
            pidHost = source + "." + tmp;

            logger.error("epochInMillis : {}", epochInMillis);
            logger.error("myname : {}", myname);
            logger.error("myval : {}", myval);
            logger.error("valstr : {}", valstr);
            logger.error("thisPID : {}", thisPID);
            logger.error("tmp : {}", tmp);
            logger.error("pidHost : {}", pidHost);
            logger.error("ClassNameAlias : {}", result.getClassNameAlias());






            String[] parts = myname.split(delims);
            logger.error("parts.length : {}", parts.length);
            if( parts.length > 0 ) {
                String p1 = parts[0];
                logger.warn("p1 : {}", p1);
                if( (jmxtrans_metric_groupID != null) && (p1.equals("jmxtrans")) )  {
                    Result new_result = new Result(myname);
                    new_result.setQuery(query);
                    new_result.setTypeName(pidHost);
                    new_result.addValue(myname, myval);
                    new_result.setEpoch(epochInMillis);
//                    QueryResult new_result = new QueryResult(myname, pidHost, myval, epochInMillis);
                    jmxtrans_counters.add(new_result);
                } else if( p1.equals("jvm") ) {
                    if( parts[1].equals("os")) {
                        if (parts[2].equals("OpenFileDescriptorCount")) {
                            Result new_result = new Result(myname);
                            new_result.setQuery(query);
                            new_result.setTypeName(pidHost);
                            new_result.addValue(myname, myval);
                            new_result.setEpoch(epochInMillis);
//                            QueryResult new_result = new QueryResult(myname, pidHost, myval, epochInMillis);
                            jvm_os_counters.add(new_result);
                        } else if (parts[2].equals("CommittedVirtualMemorySize")){
                            float fval = Float.parseFloat(valstr);
                            try {
                                fval = fval/(1024.0f*1024.0f);
                            } catch (Exception e) {
                                exceptionCounter.incrementAndGet();
                                logger.info("Exception doing Float: ", e);
                            }
                            Result new_result = new Result(myname);
                            new_result.setQuery(query);
                            new_result.setTypeName(pidHost);
                            new_result.addValue(myname, fval);
                            new_result.setEpoch(epochInMillis);
//                            QueryResult new_result = new QueryResult(myname, pidHost, fval, epochInMillis);
                            jvm_os_counters.add(new_result);
                        } else if (parts[2].equals("ProcessCpuTime")) {
                            float fval = Float.parseFloat(valstr);
                            try {
                                fval = fval/(1000.0f*1000.0f*1000.0f);
                            } catch (Exception e) {
                                exceptionCounter.incrementAndGet();
                                logger.warn("Exception doing Float: ", e);
                            }
                            Result new_result = new Result(myname);
                            new_result.setQuery(query);
                            new_result.setTypeName(pidHost);
                            new_result.addValue(myname, fval);
                            new_result.setEpoch(epochInMillis);
//                            QueryResult new_result = new QueryResult(myname, pidHost, fval, epochInMillis);
                            jvm_os_counters.add(new_result);
                        }
                    } else if( (parts[1].equals("runtime")) && (parts[2].equals("Uptime")) ) {
                        float fval = Float.parseFloat(valstr);
                        try {
                            fval = fval/(1000.0f*60.0f);
                        } catch (Exception e) {
                            exceptionCounter.incrementAndGet();
                            logger.warn("Exception doing Float: ", e);
                        }
                        Result new_result = new Result(myname);
                        new_result.setQuery(query);
                        new_result.setTypeName(pidHost);
                        new_result.addValue(myname, fval);
                        new_result.setEpoch(epochInMillis);
//                        QueryResult new_result = new QueryResult(myname, pidHost, fval, epochInMillis);
                        jvm_runtime_counters.add(new_result);
                    } else if( (parts[1].equals("loadedClasses")) && (parts[2].equals("LoadedClassCount")) ) {
                        // jvm.loadedClasses.LoadedClassCount 5099 1374549969
                        Result new_result = new Result(myname);
                        new_result.setQuery(query);
                        new_result.setTypeName(pidHost);
                        new_result.addValue(myname, myval);
                        new_result.setEpoch(epochInMillis);
//                        QueryResult new_result = new QueryResult(myname, pidHost, myval, epochInMillis);
                        jvm_class_counters.add(new_result);
                    } else if( (parts[1].equals("thread")) && (parts[2].equals("ThreadCount")) ){
                        // jvm.thread.ThreadCount 13 1374549940
                        Result new_result = new Result(myname);
                        new_result.setQuery(query);
                        new_result.setTypeName(pidHost);
                        new_result.addValue(myname, myval);
                        new_result.setEpoch(epochInMillis);
//                        QueryResult new_result = new QueryResult(myname, pidHost, myval, epochInMillis);
                        jvm_thread_counters.add(new_result);
                    } else if( (parts[1].equals("gc")) &&
                            ( (parts[2].equals("Copy")) ||  (parts[2].equals("MarkSweepCompact")) ) &&
                            ( (parts[3].equals("CollectionCount")) ||  (parts[3].equals("CollectionTime")) ) ) {
                        // jvm.gc.Copy.CollectionCount 68 1374549940
                        Result new_result = new Result(myname);
                        new_result.setQuery(query);
                        new_result.setTypeName(pidHost);
                        new_result.addValue(myname, myval);
                        new_result.setEpoch(epochInMillis);
//                        QueryResult new_result = new QueryResult(myname, pidHost, myval, epochInMillis);
                        jvm_gc_counters.add(new_result);
                    } else if( parts[1].equals("memorypool") ){
                        if( ( (parts[2].equals("Perm_Gen")) || (parts[2].equals("Code_Cache")) ) &&
                                ( (parts[4].equals("committed")) || (parts[4].equals("used"))  ) ) {
                            myname = "jvmNonHeapMemoryUsage";
                            String fullID = pidHost + "." + parts[2] + "." + parts[4];
                            float fval = Float.parseFloat(valstr);
                            try {
                                fval = fval/(1024.0f*1024.0f);
                            } catch (Exception e) {
                                exceptionCounter.incrementAndGet();
                                logger.warn("Exception doing Float: ", e);
                            }
                            Result new_result = new Result(myname);
                            new_result.setQuery(query);
                            new_result.setTypeName(fullID);
                            new_result.addValue(myname, fval);
                            new_result.setEpoch(epochInMillis);
//                            QueryResult new_result = new QueryResult(myname, fullID, fval, epochInMillis);
                            nonheap_counters.add(new_result);
                        } else if( ( (parts[2].equals("Eden_Space")) ||
                                (parts[2].equals("Survivor_Space")) ||
                                (parts[2].equals("Tenured_Gen"))
                        ) && (  (parts[4].equals("committed")) ||  (parts[4].equals("used"))  ) ) {
                            myname = "jvmHeapMemoryUsage";
                            String fullID = pidHost + "." + parts[2] + "." + parts[4];
                            float fval = Float.parseFloat(valstr);
                            try {
                                fval = fval/(1024.0f*1024.0f);
                            } catch (Exception e) {
                                exceptionCounter.incrementAndGet();
                                logger.warn("Exception doingFloat: ", e);
                            }
                            Result new_result = new Result(myname);
                            new_result.setQuery(query);
                            new_result.setTypeName(fullID);
                            new_result.addValue(myname, fval);
                            new_result.setEpoch(epochInMillis);
//                            QueryResult new_result = new QueryResult(myname, fullID, fval, epochInMillis);
                            heap_counters.add(new_result);
                        }
                    }
                } else if( p1.equals("tomcat") ) {
                    if( (parts[1].equals("thread-pool")) &&
                            ( (parts[3].equals("currentThreadsBusy")) || (parts[3].equals("currentThreadCount")) ) ){
                        // tomcat.thread_pool.http-bio-8080.currentThreadCount 0 1374549955
                        String connector = parts[2];
                        myname = parts[0] + "." + parts[1] + "." + parts[3];
                        String fullID = pidHost + "." + connector;
                        Result new_result = new Result(myname);
                        new_result.setQuery(query);
                        new_result.setTypeName(fullID);
                        new_result.addValue(myname, myval);
                        new_result.setEpoch(epochInMillis);
//                        QueryResult new_result = new QueryResult(myname, fullID, myval, epochInMillis);
                        tomcat_thread_pool_counters.add(new_result);
                    } else if( (parts[1].equals("global-request-processor")) ) {
                        // tomcat.global-request-processor.http-bio-8080.bytesSent
                        String connector = parts[2];
                        myname = parts[0] + "." + parts[1] + "." + parts[3];
                        String fullID = pidHost + "." + connector;
                        if( parts[3].equals("processingTime")) {
                            float fval = Float.parseFloat(valstr);
                            try {
                                fval = fval/(1024.0f);
                            } catch (Exception e) {
                                exceptionCounter.incrementAndGet();
                                logger.warn("Exception doingFloat: ", e);
                            }
                            Result new_result = new Result(myname);
                            new_result.setQuery(query);
                            new_result.setTypeName(fullID);
                            new_result.addValue(myname, fval);
                            new_result.setEpoch(epochInMillis);
//                            QueryResult new_result = new QueryResult(myname, fullID, fval, epochInMillis);
                            tomcat_grp_counters.add(new_result);
                        } else {
                            Result new_result = new Result(myname);
                            new_result.setQuery(query);
                            new_result.setTypeName(fullID);
                            new_result.addValue(myname, myval);
                            new_result.setEpoch(epochInMillis);
//                            QueryResult new_result = new QueryResult(myname, fullID, myval, epochInMillis);
                            tomcat_grp_counters.add(new_result);
                        }
                    } else if( (parts[1].equals("manager"))  && (parts[4].equals("activeSessions")) ){
                        //  tomcat.manager.localhost._docs.activeSessions 0 1374549955
                        String myhost = parts[2];
                        String mycontext = parts[3];
                        myname = parts[0] + "." + parts[1] + "." + parts[4];
                        String fullID = pidHost + "." + myhost + "." + mycontext;
                        Result new_result = new Result(myname);
                        new_result.setQuery(query);
                        new_result.setTypeName(fullID);
                        new_result.addValue(myname, myval);
                        new_result.setEpoch(epochInMillis);
//                        QueryResult new_result = new QueryResult(myname, fullID, myval, epochInMillis);
                        tomcat_manager_counters.add(new_result);
                    } else if( (parts[1].equals("servlet")) &&
                            ( (parts[4].equals("processingTime")) ||
                                    (parts[4].equals("errorCount")) ||
                                    (parts[4].equals("requestCount")) ) ){
                        // tomcat.servlet.__localhost_cocktail-app-1_0_9-SNAPSHOT.spring-mvc.processingTime
                        String myWebmodule = parts[2];
                        String myServletname = parts[3];
                        myname = parts[0] + "." + parts[1] + "." + parts[4];
                        String fullID = pidHost + "." + myWebmodule + "." + myServletname;
                        Result new_result = new Result(myname);
                        new_result.setQuery(query);
                        new_result.setTypeName(fullID);
                        new_result.addValue(myname, myval);
                        new_result.setEpoch(epochInMillis);
//                        QueryResult new_result = new QueryResult(myname, fullID, myval, epochInMillis);
                        tomcat_servlet_counters.add(new_result);
                    } else if( (tomcat_db_groupID != null) && (parts[1].equals("data-source")) ) {
                        String myhost = parts[2];
                        String mycontext = parts[3];
                        String mydbname = parts[4];
                        myname = parts[0] + "." + parts[1] + "." + parts[5];
                        String fullID = pidHost + "." + myhost + "." + mycontext + "." + mydbname;
                        Result new_result = new Result(myname);
                        new_result.setQuery(query);
                        new_result.setTypeName(fullID);
                        new_result.addValue(myname, myval);
                        new_result.setEpoch(epochInMillis);
//                        QueryResult new_result = new QueryResult(myname, fullID, myval, epochInMillis);
                        tomcat_db_counters.add(new_result);
                    }
                } else if( (app_groupID != null) && (p1.equals("cocktail")) ) {
                    if( !(parts[1].equals("CreatedCocktailCount")) &&  !(parts[1].equals("UpdatedCocktailCount")) ) {
                        Result new_result = new Result(myname);
                        new_result.setQuery(query);
                        new_result.setTypeName(pidHost);
                        new_result.addValue(myname, myval);
                        new_result.setEpoch(epochInMillis);
//                        QueryResult new_result = new QueryResult(myname, pidHost, myval, epochInMillis);
                        app_counters.add(new_result);
                    }
                } else if( ( (app_sales_groupID != null) && (p1.equals("sales")) ) &&
                        ( (parts[1].equals("ordersCounter")) ||
                                (parts[1].equals("itemsCounter")) ||
                                (parts[1].equals("revenueInCentsCounter")) ) ){
                    Result new_result = new Result(myname);
                    new_result.setQuery(query);
                    new_result.setTypeName(pidHost);
                    new_result.addValue(myname, myval);
                    new_result.setEpoch(epochInMillis);
//                    QueryResult new_result = new QueryResult(myname, pidHost, myval, epochInMillis);
                    app_sales_counters.add(new_result);
                } else {
                    logger.warn("epochInMillis : {}", epochInMillis);
                    logger.warn("myname : {}", myname);
                    logger.warn("myval : {}", myval);
                    logger.warn("valstr : {}", valstr);
                    logger.warn("thisPID : {}", thisPID);
                    logger.warn("pidHost : {}", pidHost);
                    Result new_result = new Result(myname);
                    new_result.setQuery(query);
                    new_result.setTypeName(pidHost);
                    String[] resultParts = valstr.substring(1, valstr.length()-1).split("=");
                    logger.warn("resultParts[0] : {} and resultParts[1] : {}", resultParts[0], resultParts[1]);
                    StringBuilder keyNameBuilder = new StringBuilder();
                    keyNameBuilder.append(result.getClassNameAlias());
                    keyNameBuilder.append(".");
                    keyNameBuilder.append(resultParts[0]);
                    if (resultParts[0].contains("Size")) {
                        new_result.addValue(keyNameBuilder.toString(), Long.valueOf(resultParts[1]));
//                        logger.error("################################################");
//                        logger.error("{\"type\":\"ce_gauge\", \"name\":\"" + keyNameBuilder.toString() + "\", \"unit\":\"bytes\"}");
//                        logger.error("################################################");
                    } else if (resultParts[0].equals("Load")) {
                        new_result.addValue(keyNameBuilder.toString(), Float.valueOf(resultParts[1]));
//                        logger.error("################################################");
//                        logger.error("{\"type\":\"ce_gauge\", \"name\":\"" + keyNameBuilder.toString() + "\"}");
//                        logger.error("################################################");
                    } else {
                        new_result.addValue(keyNameBuilder.toString(), Integer.valueOf(resultParts[1]));
//                        logger.error("################################################");
//                        logger.error("{\"type\":\"ce_counter\", \"name\":\"" + keyNameBuilder.toString() + "\"}");
//                        logger.error("################################################");
                    }
                    new_result.setEpoch(epochInMillis);
                    cassandra_counters.add(new_result);
                }
            } else {
                logger.warn("parts return NULL!!!");
            }
        }
        if(jvm_os_counters.size() > 0) {
            sort_n_send(jvm_os_groupID, jvm_os_counters);
        }
        if(jvm_gc_counters.size() > 0) {
            sort_n_send(jvm_gc_groupID, jvm_gc_counters);
        }
        if(jvm_runtime_counters.size() > 0) {
            sort_n_send(jvm_runtime_groupID, jvm_runtime_counters);
        }
        if(jvm_class_counters.size() > 0) {
            sort_n_send(jvm_class_groupID, jvm_class_counters);
        }
        if(jvm_thread_counters.size() > 0) {
            sort_n_send(jvm_thread_groupID, jvm_thread_counters);
        }
        if(heap_counters.size() > 0) {
            sort_n_send(heap_metric_groupID, heap_counters);
        }
        if(nonheap_counters.size() > 0) {
            sort_n_send(nonheap_metric_groupID, nonheap_counters);
        }
        if(tomcat_thread_pool_counters.size() > 0) {
            sort_n_send(tomcat_thread_pool_groupID, tomcat_thread_pool_counters);
        }
        if(tomcat_grp_counters.size() > 0) {
            sort_n_send(tomcat_grp_groupID,tomcat_grp_counters);
        }
        if(tomcat_servlet_counters.size() > 0) {
            sort_n_send(tomcat_servlet_groupID, tomcat_servlet_counters);
        }
        if(tomcat_manager_counters.size() > 0) {
            sort_n_send(tomcat_manager_groupID, tomcat_manager_counters);
        }
        if(tomcat_db_counters.size() > 0) {
            sort_n_send(tomcat_db_groupID, tomcat_db_counters);
        }
        if(jmxtrans_counters.size() > 0) {
            sort_n_send(jmxtrans_metric_groupID, jmxtrans_counters);
        }
        if(app_counters.size() > 0) {
            sort_n_send(app_groupID, app_counters);
        }
        if(app_sales_counters.size() > 0) {
            sort_n_send(app_sales_groupID, app_sales_counters);
        }
        if (cassandra_counters.size() > 0) {
            logger.warn("SENDING SMUT TO SORT N SEND!!!");
            sort_n_send(cassandra_groupID, cassandra_counters);
        }
    }

    @Override
    public void validateSetup(Query query) throws ValidationException {
        logger.error("*****  VALIDATE_SETUP  *****");
        counter.incrementAndGet();
        logger.info("counter is {}", counter.get());
        logger.info("jvm_os_groupID : {}", jvm_os_groupID);
        logger.info("jvm_gc_groupID : {}", jvm_gc_groupID);
        logger.info("jvm_runtime_groupID : {}", jvm_runtime_groupID);
        logger.info("jvm_class_groupID : {}", jvm_class_groupID);
        logger.info("jvm_thread_groupID : {}", jvm_thread_groupID);
        logger.info("heap_metric_groupID : {}", heap_metric_groupID);
        logger.info("nonheap_metric_groupID : {}", nonheap_metric_groupID );
        logger.info("tomcat_thread_pool_groupID : {}",tomcat_thread_pool_groupID);
        logger.info("tomcat_grp_groupID : {}",tomcat_grp_groupID);
        logger.info("tomcat_servlet_groupID : {}", tomcat_servlet_groupID );
        logger.info("tomcat_manager_groupID : {}",tomcat_manager_groupID );
        logger.info("tomcat_db_groupID  : {}", tomcat_db_groupID);
        logger.info("jmxtrans_metric_groupID : {}", jmxtrans_metric_groupID);
        logger.info("app_groupID : {}", app_groupID );
        logger.info("app_sales_groupID : {}", app_sales_groupID );
        logger.info("cassandra_groupID : {}", cassandra_groupID );
//        throw new UnsupportedOperationException("validateSetup() method hasn't been implemented.");

    }


    public void sort_n_send(String mg_name, List<Result> mg_counters) {
        Collections.sort(mg_counters, new Comparator<Result>() {
            public int compare(Result o1, Result o2) {
                //Sorts by 'epochInMillis' property

                Integer rslt = o1.getEpoch() < o2.getEpoch() ? -1 : o1.getEpoch() > o2.getEpoch() ? 1 : 0;
                if (rslt == 0) {
                    rslt = (o1.getTypeName()).compareTo(o2.getTypeName());
                }
                return rslt;
            }
        });
        send_metrics(mg_name, mg_counters);
    }

    private void printResults(List<Result> results) {
        for (Result result : results) {
            logger.warn("result.name : {}", result.getAttributeName());
            printMap(result.getValues());
//            logger.warn("result.name : {} and value : {}", result.getAttributeName(), result.getValues().get(result.getAttributeName()));
        }
    }

    public void send_metrics(String mg_name, List<Result> counters) {
        logger.error("******************  SEND METRICS  ********************");
        logger.warn("mg_name : {}", mg_name);
        printResults(counters);
        long timeblock = counters.get(0).getEpoch()/1000;
        String identifier = counters.get(0).getTypeName();
        int remaining = counters.size();
        List<Result> sorted_ctrs = new ArrayList<Result>();

        for (Result counter : counters) {
            remaining = remaining - 1;
            if( (timeblock != (counter.getEpoch()/1000)) || (!identifier.equals(counter.getTypeName()) ) ) {
                one_set(mg_name, sorted_ctrs);
                timeblock = counter.getEpoch()/1000;
                identifier = counter.getTypeName();
                sorted_ctrs.clear();
                sorted_ctrs.add(counter);
            } else {
                sorted_ctrs.add(counter);
            }
            if( remaining == 0 ) {
                one_set(mg_name, sorted_ctrs);
            }
        }
    }

    public void one_set(String mg_name, List<Result> counters)  {
        HttpURLConnection urlCxn = null;
        URL newurl = null;
        try {
            newurl = new URL(url_str + "/samples/" + mg_name + ".json");
            logger.error("URL is {" + newurl + "}");
            if (proxy == null) {
                urlCxn = (HttpURLConnection) newurl.openConnection();
            } else {
                urlCxn = (HttpURLConnection) newurl.openConnection(proxy);
            }
            if (urlCxn != null) {
                urlCxn.setRequestMethod("POST");
                urlCxn.setDoInput(true);
                urlCxn.setDoOutput(true);
                urlCxn.setReadTimeout(coppereggApiTimeoutInMillis);
                urlCxn.setRequestProperty("content-type", "application/json; charset=utf-8");
                urlCxn.setRequestProperty("Authorization", "Basic " + basicAuthentication);
            }
        } catch (Exception e) {
            exceptionCounter.incrementAndGet();
            //logger.warn("Exception: one_set: failed to connect to CopperEgg Service '{}' with proxy {}", newurl, proxy, e);
            return;
        }
        if( urlCxn != null ) {
            try {
                cue_serialize(counters, urlCxn.getOutputStream());
                int responseCode = urlCxn.getResponseCode();
                logger.error("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&   REsponce Code is {}", responseCode);
                if (responseCode != 200) {
                    logger.warn("one_set: Failure " + String.valueOf(responseCode) + ": " + urlCxn.getResponseMessage() + " to send result to CopperEgg service {}", newurl);
//                    logger.warn("one_set: Failure {}: {} to send result to CopperEgg service {}");
                }
                try {
                    InputStream in = urlCxn.getInputStream();
                    logger.warn("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
                    logger.warn(urlCxn.getResponseMessage());
                    IoUtils2.copy(in, IoUtils2.nullOutputStream());
                    IoUtils2.closeQuietly(in);
                    InputStream err = urlCxn.getErrorStream();
                    if (err != null) {
                        IoUtils2.copy(err, IoUtils2.nullOutputStream());
                        IoUtils2.closeQuietly(err);
                    }
                } catch (IOException e) {
                    exceptionCounter.incrementAndGet();
                    logger.warn("Execption one_set: Write-Exception flushing http connection", e);
                }

            } catch (Exception e) {
                exceptionCounter.incrementAndGet();
//                logger.warn("Execption: one_set: Failure to send result to CopperEgg Service '{}' with proxy {}", newurl, proxy, e);
                logger.warn("Execption: one_set: Failure to send result to CopperEgg Service '{}' with proxy {}", e);
            }
        }
    }


    public void cue_serialize(Iterable<Result> counters, OutputStream out) throws IOException {
        int first = 0;
        long time = 0;
        String myID = null;
        JsonGenerator g = jsonFactory.createJsonGenerator(out, JsonEncoding.UTF8);

        for (Result counter : counters) {
            if( 0 == first ) {
                time = counter.getEpoch()/1000;
                myID = counter.getTypeName();
                first = 1;
                g.writeStartObject();
//                g.writeStringField("identifier", "my_cassandra_metrics2_identifier");
                g.writeStringField("identifier", "jeff_results_from_jmxtrans");
                g.writeNumberField("timestamp", time);
                g.writeObjectFieldStart("values");
            }
            for (Map.Entry<String, Object> entry : counter.getValues().entrySet()) {
                logger.warn("Class of value is: {}", entry.getValue().getClass());
//                String key = entry.getKey().toLowerCase();
                String key = entry.getKey();
                if (entry.getValue() instanceof Integer) {
                    g.writeNumberField(key, (Integer) entry.getValue());
                } else if (entry.getValue() instanceof Long) {
                    g.writeNumberField(key, (Long) entry.getValue());
                } else if (entry.getValue() instanceof Float) {
                    g.writeNumberField(key, (Float) entry.getValue());
                } else if (entry.getValue() instanceof Double) {
                    g.writeNumberField(key, (Double) entry.getValue());
                }
            }
        }
        logger.error("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//        logger.error(g.toString());
        g.writeEndObject();
        g.writeEndObject();
        g.flush();
        g.close();

        logger.error(out.toString());
    }

    private static long getPID() {
        String processName =
                java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        return Long.parseLong(processName.split("@")[0]);
    }

    public int cue_getExceptionCounter() {
        return exceptionCounter.get();
    }

    private String getStringSetting(String name) throws IllegalArgumentException {
        if (!getSettings().containsKey(name)) {
            throw new IllegalArgumentException("No setting '" + name + "' found");
        }
        return getSettings().get(name).toString();
    }

    private int getIntSetting(String name) throws IllegalArgumentException {
        String value = getStringSetting(name);
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Setting '" + name + "=" + value + "' is not an integer on " + this.toString());
        }
    }


    private void read_config(String in) throws IOException {
        logger.error("****************************************  READ_CONFIG STRING  *************************************");
        JsonFactory f = new MappingJsonFactory();
        JsonParser jp = f.createJsonParser(in);

        read_config_helper(jp);
    }



    public void read_config(InputStream in) throws IOException {

        logger.error("****************************************  READ_CONFIG  *************************************");


        JsonFactory f = new MappingJsonFactory();
        JsonParser jp = f.createJsonParser(in);

        read_config_helper(jp);
    }

    private void read_config_helper(JsonParser jp) throws IOException{

        JsonToken current;

        current = jp.nextToken();
        if (current != JsonToken.START_OBJECT) {
            logger.warn("read_config: Error:  START_OBJECT not found : quiting.  Found {}", current);
            return;
        }
        current = jp.nextToken();
        logger.error(current.toString());
        String fieldName = jp.getCurrentName();
        current = jp.nextToken();
        logger.error(current.toString());

        if (fieldName.equals("config")) {
            if (current != JsonToken.START_OBJECT) {
                logger.warn("read_config: Error:  START_OBJECT not found after config : quiting.");
                return;
            }
            current = jp.nextToken();
            logger.error(current.toString());
            String fieldName2 = jp.getCurrentName();
            logger.error("FieldName2 : {}", fieldName2);
            if (fieldName2.equals("metric_groups")) {
                current = jp.nextToken();
                if (current != JsonToken.START_ARRAY) {
                    logger.warn("read_config: Error:  START_ARRAY not found after metric_groups : quiting.");
                    return;
                }

                current = jp.nextToken();
                while (current != JsonToken.END_ARRAY) {
                    if (current != JsonToken.START_OBJECT) {
                        logger.warn("read_config: Error:  START_OBJECT not found after metric_groups START_ARRAY : quiting.");
                        return;
                    }
                    current = jp.nextToken();
                    JsonNode node1 = jp.readValueAsTree();
                    String node1string = write_tostring(node1);
                    metricgroupMap.put(node1.get("name").getTextValue(),node1string);
                    logger.warn("MetricGroupMap added name {} and value {}", node1.get("name").getTextValue(), node1string);
                    current = jp.nextToken();
                }

                current = jp.nextToken();
                String fieldName3 = jp.getCurrentName();
                logger.error(current.toString());
                logger.error("fieldName3 : {}", fieldName3);

                if (fieldName3.equals("dashboards")) {
                    current = jp.nextToken();
                    if (current != JsonToken.START_ARRAY) {
                        logger.warn("read_config: Error:  START_ARRAY not found after dashboards : quiting.");
                        return;
                    }
                    current = jp.nextToken();
                    logger.error(current.toString());
                    while (current != JsonToken.END_ARRAY) {
                        if (current != JsonToken.START_OBJECT) {
                            logger.warn("read_config: Error:  START_OBJECT not found after dashboards START_ARRAY : quiting.");
                            return;
                        }
                        current = jp.nextToken();
                        logger.error(current.toString());
                        JsonNode node = jp.readValueAsTree();
                        String nodestring = write_tostring(node);
                        dashMap.put(node.get("name").getTextValue(),nodestring);
                        logger.error("Putting on DASHMAP : {}, {}", node.get("name").getTextValue(),nodestring);
                        current = jp.nextToken();

                    }
                    if(jp.nextToken() != JsonToken.END_OBJECT) {
                        logger.warn("read_config: Error:  END_OBJECT expected, not found (1): quiting.");
                        return;
                    }
                    if(jp.nextToken() != JsonToken.END_OBJECT) {
                        logger.warn("read_config: Error:  END_OBJECT expected, not found (2): quiting.");
                        return;
                    }
                } else {
                    logger.warn("read_config: Error:  Expected dashboards : quiting.");
                    return;
                }
            } else {
                logger.warn("read_config: Error:  Expected metric_groups : quiting.");
                return;
            }
        }
    }

    public String write_tostring(JsonNode json){
        ObjectMapper mapper = new ObjectMapper();
        StringWriter out = new StringWriter();

        try {
            JsonFactory fac = new JsonFactory();
            JsonGenerator gen = fac.createJsonGenerator(out);

            // Now write:
            mapper.writeTree(gen, json);
            gen.flush();
            gen.close();
            return out.toString();
        }
        catch(Exception e) {
            exceptionCounter.incrementAndGet();
            logger.warn("Exception in write_tostring: " + e);
        }
        return(null);
    }

    public void ensure_metric_groups() {
        logger.error("****************************************  ENSURE_METRIC_GROUPS  *************************************");

        HttpURLConnection urlConnection = null;
        OutputStreamWriter wr = null;
        URL myurl = null;

        try {
            myurl = new URL(url_str + "/metric_groups.json?show_hidden=1");
            urlConnection = (HttpURLConnection) myurl.openConnection();
            urlConnection.setRequestMethod("GET");
            urlConnection.setDoInput(true);
            urlConnection.setDoOutput(true);
            urlConnection.setReadTimeout(coppereggApiTimeoutInMillis);
            urlConnection.setRequestProperty("content-type", "application/json; charset=utf-8");
            urlConnection.setRequestProperty("Authorization", "Basic " + basicAuthentication);

            int responseCode = urlConnection.getResponseCode();
            if (responseCode != 200) {
                logger.warn("Bad responsecode " + String.valueOf(responseCode)+ " from metric_groups Index: " +  myurl);
            }
        } catch (Exception e) {
            exceptionCounter.incrementAndGet();
            logger.warn("Failure to execute metric_groups index request "+ myurl + "  "+ e);
        } finally {
            if (urlConnection != null) {
                try {
                    InputStream in = urlConnection.getInputStream();
                    String theString = convertStreamToString(in);
                    logger.warn("TheString: {}", theString);
                    for (Map.Entry<String, String> entry : metricgroupMap.entrySet()) {
                        String checkName =  entry.getKey();
                        try {
                            String Rslt = groupFind(checkName, theString, 0);
                            if(Rslt != null){
                                // Update it
                                Rslt = Send_Commmand("/metric_groups/" + Rslt + ".json?show_hidden=1", "PUT", entry.getValue(),0);
                            } else {
                                // create it
                                Rslt = Send_Commmand("/metric_groups.json", "POST", entry.getValue(),0);
                            }
                            if(Rslt != null) {
                                if (Rslt.toLowerCase().contains("cassandra")) {
                                    cassandra_groupID = Rslt;
                                    logger.error("************ SET cassandra_groupID to {}", cassandra_groupID);
                                }
/*
                                if (Rslt.toLowerCase().contains("tomcat")) {
                                    if (Rslt.toLowerCase().contains("thread_pool")) {
                                        tomcat_thread_pool_groupID = Rslt;
                                    } else if (Rslt.toLowerCase().contains("grp")) {
                                        tomcat_grp_groupID = Rslt;
                                    } else if (Rslt.toLowerCase().contains("servlet")) {
                                        tomcat_servlet_groupID = Rslt;
                                    } else if (Rslt.toLowerCase().contains("manager")) {
                                        tomcat_manager_groupID = Rslt;
                                    } else if (Rslt.toLowerCase().contains("db")) {
                                        tomcat_db_groupID = Rslt;
                                    }
                                } else if (Rslt.toLowerCase().contains("jmxtrans")) {
                                    jmxtrans_metric_groupID = Rslt;
                                } else if (Rslt.toLowerCase().contains("sales")) {
                                    app_sales_groupID = Rslt;
                                } else if (Rslt.toLowerCase().contains("cocktail")) {
                                    app_groupID = Rslt;
                                } else if (Rslt.toLowerCase().contains("jvm")) {
                                    if (Rslt.toLowerCase().contains("os")) {
                                        jvm_os_groupID = Rslt;
                                    } else if (Rslt.toLowerCase().contains("gc")) {
                                        jvm_gc_groupID = Rslt;
                                    } else if (Rslt.toLowerCase().contains("runtime")) {
                                        jvm_runtime_groupID = Rslt;
                                    } else if (Rslt.toLowerCase().contains("class")) {
                                        jvm_class_groupID = Rslt;
                                    } else if (Rslt.toLowerCase().contains("thread")) {
                                        jvm_thread_groupID = Rslt;
                                    }
                                } else if (Rslt.toLowerCase().contains("nonheap")) {
                                    nonheap_metric_groupID = Rslt;
                                } else if (Rslt.toLowerCase().contains("heap")) {
                                    heap_metric_groupID = Rslt;
                                }
*/
                            }
                        } catch (Exception e) {
                            exceptionCounter.incrementAndGet();
                            logger.warn("Exception in metric_group update or create: "+ myurl + "  "+ e);
                        }
                    }
                } catch (IOException e) {
                    exceptionCounter.incrementAndGet();
                    logger.warn("Exception flushing http connection"+ e);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void printMap(Map<String, Object> map) {
        for (String s : map.keySet()) {
            logger.warn("Key: {} and Value: {}", s, map.get(s));
        }
    }

    public String convertStreamToString(InputStream is)
            throws IOException {
        //
        // To convert the InputStream to String we use the
        // Reader.read(char[] buffer) method. We iterate until the
        // Reader return -1 which means there's no more data to
        // read. We use the StringWriter class to produce the string.
        //
        if (is != null) {
            Writer writer = new StringWriter();

            char[] buffer = new char[1024];
            try {
                Reader reader = new BufferedReader(
                        new InputStreamReader(is, "UTF-8"));
                int n;
                while ((n = reader.read(buffer)) != -1) {
                    writer.write(buffer, 0, n);
                }
            } finally {
                is.close();
            }
            return writer.toString();
        } else {
            return "";
        }
    }


    public String groupFind(String findName, String findIndex, Integer ExpectInt) throws Exception {
        JsonFactory f = new MappingJsonFactory();
        JsonParser jp = f.createJsonParser(findIndex);

        int count = 0;
        int foundit = 0;
        String Result = null;

        JsonToken current = jp.nextToken();
        if (current != JsonToken.START_ARRAY) {
            logger.warn("groupFind: Error: START_ARRAY expected, not found : quiting.");
            return(Result);
        }
        current = jp.nextToken();
        while (current != JsonToken.END_ARRAY) {
            if (current != JsonToken.START_OBJECT) {
                logger.warn("groupFind: Error: START_OBJECT expected, not found : quiting.");
                return(Result);
            }
            current = jp.nextToken();
            JsonNode node = jp.readValueAsTree();
            String tmpStr = node.get("name").getTextValue().toString();
            if(findName.equals(node.get("name").getTextValue().toString())) {
                if(ExpectInt != 0) {
                    foundit = node.get("id").getIntValue();
                    Result = String.valueOf(foundit);
                } else {
                    Result = node.get("id").getTextValue().toString();
                }
                break;
            }
            current = jp.nextToken();
            count = count + 1;
        }
        return(Result);
    }

    public String Send_Commmand(String command, String msgtype, String payload, Integer ExpectInt){
        HttpURLConnection urlConnection = null;
        URL myurl = null;
        OutputStreamWriter wr = null;
        int responseCode = 0;
        String id = null;
        int error = 0;

        try {
            myurl = new URL(url_str + command);
            urlConnection = (HttpURLConnection) myurl.openConnection();
            urlConnection.setRequestMethod(msgtype);
            urlConnection.setDoInput(true);
            urlConnection.setDoOutput(true);
            urlConnection.setReadTimeout(coppereggApiTimeoutInMillis);
            urlConnection.addRequestProperty("User-Agent", "Mozilla/4.76");
            urlConnection.setRequestProperty("content-type", "application/json; charset=utf-8");
            urlConnection.setRequestProperty("Authorization", "Basic " + basicAuthentication);

            wr = new OutputStreamWriter(urlConnection.getOutputStream(),"UTF-8");
            wr.write(payload);
            wr.flush();

            responseCode = urlConnection.getResponseCode();
            if (responseCode != 200) {
                logger.warn("Send Command: Response code " + responseCode + " url is " + myurl + " command " + msgtype);
                error = 1;
            }
        } catch (Exception e) {
            exceptionCounter.incrementAndGet();
            logger.warn("Exception in Send Command: url is " + myurl + " command " + msgtype + "; " + e);
            error = 1;
        } finally {
            if (urlConnection != null) {
                try {
                    if( error > 0 ) {
                        InputStream err = urlConnection.getErrorStream();
                        String errString = convertStreamToString(err);
                        logger.warn("Reported error : " + errString);
                        IoUtils2.closeQuietly(err);
                    } else {
                        InputStream in = urlConnection.getInputStream();
                        String theString = convertStreamToString(in);
                        id = jparse(theString, ExpectInt);
                        IoUtils2.closeQuietly(in);
                    }
                } catch (IOException e) {
                    exceptionCounter.incrementAndGet();
                    logger.warn("Exception in Send Command : flushing http connection " +  e);
                }
            }
            if(wr != null) {
                try {
                    wr.close();
                } catch (IOException e) {
                    exceptionCounter.incrementAndGet();
                    logger.warn("Exception in Send Command: closing OutputWriter " +  e);
                }
            }
        }
        return(id);
    }

    private String jparse(String jsonstr, Integer ExpectInt) {

        ObjectMapper mapper = new ObjectMapper();
        String Result = null;
        try {
            JsonNode root = mapper.readTree(jsonstr);
            if(ExpectInt != 0) {
                int myid = root.get("id").getIntValue();
                Result = String.valueOf(myid);
            } else {
                Result = root.get("id").getTextValue().toString();
            }
        } catch (JsonGenerationException e) {
            exceptionCounter.incrementAndGet();
            logger.warn("JsonGenerationException "+ e);
        } catch (JsonMappingException e) {
            exceptionCounter.incrementAndGet();
            logger.warn("JsonMappingException "+ e);
        } catch (IOException e) {
            exceptionCounter.incrementAndGet();
            logger.warn("IOException "+ e);
        }
        return(Result);
    }

    /**
     * If dashboard doesn't exist, create it
     * If it does exist, update it.
     */

    private void ensure_dashboards() {
        HttpURLConnection urlConnection = null;
        OutputStreamWriter wr = null;
        URL myurl = null;

        try {
            myurl = new URL(url_str + "/dashboards.json");
            urlConnection = (HttpURLConnection) myurl.openConnection();
            urlConnection.setRequestMethod("GET");
            urlConnection.setDoInput(true);
            urlConnection.setDoOutput(true);
            urlConnection.setReadTimeout(coppereggApiTimeoutInMillis);
            urlConnection.setRequestProperty("content-type", "application/json; charset=utf-8");
            urlConnection.setRequestProperty("Authorization", "Basic " + basicAuthentication);

            int responseCode = urlConnection.getResponseCode();
            if (responseCode != 200) {
                logger.warn("Bad responsecode " + String.valueOf(responseCode)+ " from Dahsboards Index: " +  myurl);
            }
        } catch (Exception e) {
            exceptionCounter.incrementAndGet();
            logger.warn("Exception on dashboards index request "+ myurl + "  "+ e);
        } finally {
            if (urlConnection != null) {
                try {
                    InputStream in = urlConnection.getInputStream();
                    String theString = convertStreamToString(in);
                    for (Map.Entry<String, String> entry : dashMap.entrySet()) {
                        String checkName =  entry.getKey();
                        try {
                            String Rslt = groupFind(checkName, theString, 1);
                            if(Rslt != null){
                                // Update it
                                Rslt = Send_Commmand("/dashboards/" + Rslt + ".json", "PUT", entry.getValue(),1);
                            } else {
                                // create it
                                Rslt = Send_Commmand("/dashboards.json", "POST", entry.getValue(),1);
                            }
                        } catch (Exception e) {
                            exceptionCounter.incrementAndGet();
                            logger.warn("Exception in dashboard update or create: "+ myurl + "  "+ e);
                        }
                    }
                } catch (IOException e) {
                    exceptionCounter.incrementAndGet();
                    logger.warn("Exception flushing http connection"+ e);
                }
            }
        }
    }

}
