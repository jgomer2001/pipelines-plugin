package io.jans.cedarling.opensearch;

import java.util.*;
import java.security.*;

import org.apache.logging.log4j.*;
import org.json.*;
import org.testng.annotations.*;
import org.testng.ITestContext;

import static org.testng.Assert.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public class BenchmarkTest {
    
    private Logger logger = LogManager.getLogger(getClass());
    private NetworkUtil nu = null;
    private Random ranma = new SecureRandom();
    
    private String indexName;
    private int entries;
    private String bulkEntryTemplate;
    private String query;
    
    private int queryTookMs;
    private int filteredQueryTookMs;
    
    @BeforeClass
    public void initTestSuite(ITestContext context) throws Exception {

        //See AlterSuiteListener class first please
        Map<String, String> params = context.getSuite().getXmlSuite().getParameters();
        String user = params.get("user");
        String pwd = params.get("password");
        
        byte[] bytes = Base64.getUrlEncoder().encode((user + ":" + pwd).getBytes());
        nu = new NetworkUtil(params.get("apiBase"), "Basic " + new String(bytes, UTF_8));
        
        entries = Integer.parseInt(params.get("entries"));        
        bulkEntryTemplate = params.get("bulkEntryTemplate");
        indexName = params.get("indexName");
        query = params.get("queryFile");
        
    }
    
    @Test
    public void dropIndex() throws Exception {
        
        logger.info("Deleting index {}...", indexName);
        JSONObject obj = nu.sendDelete(indexName, 200, 404);

        logger.debug("Checking result of delete operation");
        if (obj.optInt("status") != 404) {
            assertEquals(obj.getBoolean("acknowledged"), true);
        }

    }
    
    @Test(dependsOnMethods = "dropIndex")
    public void fillIndex() throws Exception {
        
        logger.info("Creating {} entries...", entries);
        String payload = "";
        
        for (int i = 0; i < entries; i++) {
            payload += String.format(bulkEntryTemplate, getAString(), 
                        getADecimal(2000, 2026), ranma.nextFloat() * 5);            
        }
        
        logger.info("Payload of {} bytes generated", payload.getBytes().length);
        JSONObject obj = nu.sendPost(indexName + "/_bulk?refresh=true", 200, payload);

        logger.debug("Checking result of bulk operation");
        assertFalse(obj.getBoolean("errors"));
        
    }
    
    @Test(dependsOnMethods = "fillIndex")
    public void regularQuery() throws Exception {
        
        logger.info("Sending regular query...", entries);
        JSONObject obj = nu.sendPost(indexName + "/_search", 200, query);
        queryTookMs = obj.optInt("took", -1);
        assertTrue(queryTookMs != -1);
        
    }
    
    @Test(dependsOnMethods = "regularQuery")
    public void cedarlingQuery() throws Exception {
        
        dropIndex();
        fillIndex();
        
        logger.info("Sending cedarling query...", entries);
        JSONObject obj = nu.sendPost(indexName + "/_search?search_pipeline=cedarling_search", 200, query);
        filteredQueryTookMs = obj.optInt("took", -1);
        assertTrue(filteredQueryTookMs != -1);
        
        logger.info("Query times (ms): regular, cedarling; ratio");
        logger.info("{}, {}; {}", queryTookMs, filteredQueryTookMs,
                    String.format("%.3f", 1.0f*filteredQueryTookMs / queryTookMs));
        
    }
    
    private String getAString() {

        //radix 36 entails characters: 0-9 plus a-z
        String path = Integer.toString(ranma.nextInt(), Math.min(36, Character.MAX_RADIX));
        //path will have at most 6 chars in practice
        return path.substring(path.charAt(0) == '-' ? 1 : 0);
        
    }
    
    private int getADecimal(int min, int max) {
        //Pick a uniformly distributed random number from the range
        return ranma.nextInt(max - min + 1) + min;
    }
    
    /*
    private float getAFloat() {        
        //returns a random float number in [0, 1) with two decimal digits
        return ranma.nextInt(100) / 100.0f;
    }*/
    
}
