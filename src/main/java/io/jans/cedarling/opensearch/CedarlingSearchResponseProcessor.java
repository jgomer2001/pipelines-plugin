package io.jans.cedarling.opensearch;

import java.util.*;

import org.apache.logging.log4j.*;
import org.json.*;
import org.opensearch.search.*;
import org.opensearch.search.pipeline.*;
import org.opensearch.action.search.*;
import org.opensearch.search.profile.*;

public class CedarlingSearchResponseProcessor extends AbstractProcessor implements SearchResponseProcessor {
    
    public static final String TYPE = "cedarling";
    
    private Logger logger = LogManager.getLogger(getClass());
    
    @Override
    public String getType() {
        return TYPE;
    }

    private CedarlingSearchResponseProcessor(String tag, String description, boolean ignoreFailure) {
        super(tag, description, ignoreFailure);
        logger.info("Instantiating CedarlingSearchResponseProcessor");
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception {
        /*
        request.source() is expected to be like:
        
        {
            "query": { ... }
            "ext" {
                "tbac": {
                    "tokens": {
                        "access_token": "...",
                        "id_token": "...",
                        "userinfo_token": "..."
                    },
                    "context": { ... }
                }
            }
        }
        */
        logger.info("At processResponse");
        
        PluginSettings pluginSettings = SettingsService.getInstance().getSettings();
        if (!pluginSettings.isEnabled()) {
            logger.debug("Cedarling processing is disabled");
            return response;
        }
        
        List<SearchExtBuilder> exts = request.source().ext();
        if (exts.isEmpty()) {
            logger.warn("No 'ext' in request");
            return response;
        }
        
        try {
            Map<String, Object> extResponse;
            SearchResponse myResponse; 
            Map empty = Collections.emptyMap();
            
            CedarlingSearchExtBuilder cseb = CedarlingSearchExtBuilder.class.cast(exts.get(0));
            SearchHits searchHits = response.getHits();
            Iterator<SearchHit> it = searchHits.iterator();

            if (it.hasNext()) {
                List<SearchHit> authorized = new ArrayList<>();
                Map<String, Object> tbac = cseb.getParams();
                String action = pluginSettings.getSearchActionName();
                                
                Map<String, String> tokens = Optional.ofNullable(
                            tbac.get("tokens")).map(Map.class::cast).orElse(empty);
                JSONObject context = new JSONObject(Optional.ofNullable(
                            tbac.get("context")).map(Map.class::cast).orElse(empty));
                
                long decisionsTook = 0;
                do {
                    SearchHit hit = it.next();
                    Map<String, Object> map = hit.getSourceAsMap();
                    
                    try {
                        appendExtraAttributes(map, pluginSettings.getSchemaPrefix(), hit.getIndex(), hit.getId());
                        long temp = System.currentTimeMillis();
                        boolean allowed = CedarlingService.getInstance().authorize(tokens, action, map, context);
                        decisionsTook += (System.currentTimeMillis() - temp);
                        
                        if (allowed) {
                            authorized.add(hit);
                        }
                    } catch (Exception e) {
                        authorized.add(hit);    //include the result when Cedarling cannot handle it
                        logger.error(e.getMessage(), e);
                    }
                } while (it.hasNext());
                
                //override the hits, the rest remains all the same
                SearchHits mySearchHits = new SearchHits(authorized.toArray(new SearchHit[0]), 
                        searchHits.getTotalHits(), searchHits.getMaxScore(), searchHits.getSortFields(),
                        searchHits.getCollapseField(), searchHits.getCollapseValues());
                
                SearchResponseSections sections = response.getInternalResponse();
                Map<String, ProfileShardResult> shardResults = sections.profile();
                SearchResponseSections mySections = new SearchResponseSections(mySearchHits,
                        sections.aggregations(), sections.suggest(), sections.timedOut(), sections.terminatedEarly(),
                        shardResults.isEmpty() ? null : new SearchProfileShardResults(shardResults),
                        sections.getNumReducePhases(), sections.getSearchExtBuilders());
                
                myResponse = new SearchResponse(mySections,
                        response.getScrollId(), response.getTotalShards(), response.getSuccessfulShards(),
                        response.getSkippedShards(), response.getTook().getMillis(), response.getPhaseTook(),
                        response.getShardFailures(), response.getClusters(), response.pointInTimeId());
                
                extResponse = Map.of(
                        "authorized_hits_count", authorized.size(),
                        "average_decision_time", (1.0 * decisionsTook) / searchHits.getHits().length
                );
            } else {
                myResponse = response;
                extResponse = Map.of(
                        "authorized_hits_count", 0,
                        "average_decision_time", 0
                );
            }
            return CedarlingSearchResponse.make(myResponse, extResponse);
            
        } catch (Exception e) {
            logger.error("Error parsing 'ext' in request", e);
            throw e;
        }

    }

    private void appendExtraAttributes(Map<String, Object> map, String prefix, String indexName, String id) {

        map.putAll(     //TODO: this needs discussion with the Cedarling team
            Map.of(
                "type", prefix + "::" + indexName,
                "entity_type", "resource",
                "id", id
            )
        );
        
    }
    
    static class Factory implements Processor.Factory<SearchResponseProcessor> {
        
        @Override
        public CedarlingSearchResponseProcessor create(
            Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext) {

            return new CedarlingSearchResponseProcessor(tag, description, ignoreFailure);
        }

    }
    
}
