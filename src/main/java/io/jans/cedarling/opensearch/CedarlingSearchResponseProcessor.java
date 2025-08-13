package io.jans.cedarling.opensearch;

import java.util.*;

import org.apache.logging.log4j.*;
import org.json.*;
import org.opensearch.search.*;
import org.opensearch.search.pipeline.*;
import org.opensearch.action.search.*;
import org.opensearch.search.profile.*;

import uniffi.cedarling_uniffi.*;

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
        long startedAt = System.currentTimeMillis();
        
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
            SearchResponseSections sections = response.getInternalResponse();
            Map empty = Collections.emptyMap();            
            int authorizedHitsCount = 0, avgDecisionTime = -1;
            
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
                        //Thread.sleep(200);
                        //boolean allowed = true;
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
                SearchHit[] noHits = new SearchHit[0];
                SearchHits mySearchHits = new SearchHits(
                        //Use skipHits = true in the plugin config to avoid big response (it's useful for testing)
                        pluginSettings.isSkipHits() ? noHits : authorized.toArray(noHits), 
                        searchHits.getTotalHits(), searchHits.getMaxScore(), searchHits.getSortFields(),
                        searchHits.getCollapseField(), searchHits.getCollapseValues());

                Map<String, ProfileShardResult> shardResults = sections.profile();
                sections = new SearchResponseSections(mySearchHits,
                        sections.aggregations(), sections.suggest(), sections.timedOut(), sections.terminatedEarly(),
                        shardResults.isEmpty() ? null : new SearchProfileShardResults(shardResults),
                        sections.getNumReducePhases(), sections.getSearchExtBuilders());
                
                authorizedHitsCount = authorized.size();
                avgDecisionTime = Math.round((1.0f * decisionsTook) / searchHits.getHits().length);
            }
            
            return new CedarlingSearchResponse(
                        Map.of(
                            "authorized_hits_count", authorizedHitsCount,
                            "average_decision_time", avgDecisionTime
                        ),
                        sections, response.getScrollId(), response.getTotalShards(),
                        response.getSuccessfulShards(), response.getSkippedShards(),
                        System.currentTimeMillis() - startedAt + response.getTook().getMillis(), response.getPhaseTook(),
                        response.getShardFailures(), response.getClusters(), response.pointInTimeId()                
                    );
            
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
