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
    
    private static final int MAX_GPA = 5;
     
    private Logger logger = LogManager.getLogger(getClass());
    private NetworkUtil nu = null;
    private Random ranma = new SecureRandom();
    
    private int entries;
    private String indexName;
    private String bulkEntryTemplate;
    private String queryTemplate;
    private boolean useCedarling;
    
    @BeforeClass
    public void initTestSuite(ITestContext context) throws Exception {

        //See AlterSuiteListener class first please
        Map<String, String> params = context.getSuite().getXmlSuite().getParameters();
        String user = params.get("user");
        String pwd = params.get("password");
        
        byte[] bytes = Base64.getUrlEncoder().encode((user + ":" + pwd).getBytes());
        nu = new NetworkUtil(params.get("apiBase"), "Basic " + new String(bytes, UTF_8));
        
        entries = Integer.parseInt(params.get("entries"));
        indexName = params.get("indexName"); 
        bulkEntryTemplate = params.get("bulkEntryTemplate");
        queryTemplate = params.get("queryFile");
        useCedarling = Boolean.valueOf(params.get("useCedarling"));
        
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
    
    @Test(dependsOnMethods="dropIndex")
    public void fillIndex() throws Exception {
        
        logger.info("Creating {} entries...", entries);
        String payload = "";
        
        for (int i = 0; i < entries; i++) {
            payload += String.format(bulkEntryTemplate, getAString(), 
                        getADecimal(2000, 2027), ranma.nextFloat() * MAX_GPA);            
        }
        
        logger.info("Payload of {} bytes generated ({} {} entries)", payload.getBytes().length, indexName, entries);
        JSONObject obj = nu.sendPost(indexName + "/_bulk?refresh=true", 200, payload);

        logger.debug("Checking result of bulk operation");
        assertFalse(obj.getBoolean("errors"));
        
    }
    
    @Test(dependsOnMethods="fillIndex")
    public void runQueries() throws Exception {
        
        if (useCedarling) {
            cedarlingQueries();
        } else {
            regularQueries();
        }
        
    }
    
    public void regularQueries() throws Exception {
        
        int queryTookMs = 0;
        //Issue several different queries and compute average "took" time
        for (int i = 0; i < MAX_GPA; i++) {
            String query = String.format(queryTemplate, i, i + 1);
            logger.info("Sending regular query #{}...", i + 1);
            
            JSONObject obj = nu.sendPost(indexName + "/_search", 200, query);
            queryTookMs += obj.getInt("took");
        }
        logger.info("Average regular query time (ms): {}", String.format("%.3f", 1.0f*queryTookMs / MAX_GPA));
        
    }
    
    public void cedarlingQueries() throws Exception {
                
        int queryTookMs = 0, decisionTime = 0;
        int totalResults = 0, emptyResultSets = 0;
        //Issue several different queries and compute average "took" and decision time
        for (int i = 0; i < MAX_GPA; i++) {
            String query = String.format(queryTemplate, i, i + 1);
            logger.info("Sending regular query #{}...", i + 1);
            
            JSONObject obj = nu.sendPost(indexName + "/_search?search_pipeline=cedarling_search", 200, query);
            queryTookMs += obj.getInt("took");
            
            int adt = obj.getJSONObject("ext").getJSONObject("cedarling").getInt("average_decision_time");
            int res = obj.getJSONObject("hits").getJSONObject("total").getInt("value");
            
            if (adt == -1) {
                //No decisions performed, ie. empty result set. This may occur when entries member variable is small
                emptyResultSets++;
                assertEquals(res, 0);
            } else {
                decisionTime += adt;
                totalResults += res;
            } 
        }
        
        assertEquals(totalResults, entries);
        logger.info("Average plugin query time (ms): {}", String.format("%.3f", 1.0f*queryTookMs / MAX_GPA));
        logger.info("Average cedarling decision time (ms): {}", String.format("%.3f", 1.0f*decisionTime / (MAX_GPA - emptyResultSets)));
        
    }
    
    private String getAString() {

        //radix 36 entails characters: 0-9 plus a-z
        String path = Integer.toString(ranma.nextInt(), Math.min(36, Character.MAX_RADIX));
        //path will have at most 6 chars in practice
        return path.substring(path.charAt(0) == '-' ? 1 : 0);
        
    }
    
    private int getADecimal(int min, int max) {
        //Pick a uniformly distributed random number from the range [min, max)
        return ranma.nextInt(max - min + 1) + min;
    }    
    
}
