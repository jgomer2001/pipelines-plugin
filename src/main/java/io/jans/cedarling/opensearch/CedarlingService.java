package io.jans.cedarling.opensearch;

import io.jans.cedarling.binding.wrapper.CedarlingAdapter;

import java.util.Map;

import org.apache.logging.log4j.*;
import org.json.JSONObject;

import uniffi.cedarling_uniffi.*;

public class CedarlingService {
    
    private CedarlingAdapter cedarlingAdapter;    
    private Logger logger = LogManager.getLogger(getClass());
    
    private static CedarlingService instance = new CedarlingService();
    
    private CedarlingService() {
        cedarlingAdapter = new CedarlingAdapter();
    }

    public static CedarlingService getInstance() {
        return instance;
    }
    
    public void init(JSONObject bootstrapProperties) {
        
        try {
            cedarlingAdapter.loadFromJson(bootstrapProperties.toString());
        } catch (Exception e) {
            logger.error("Error initializing Cedarling", e);
        }

    }

    public boolean authorize(Map<String, String> tokens, String action, Map<String, Object> resource,
            JSONObject context) throws AuthorizeException, EntityException {

        AuthorizeResult res = cedarlingAdapter.authorize(tokens, action, new JSONObject(resource), context);        
        return res.getDecision();
        
    }
    
}
