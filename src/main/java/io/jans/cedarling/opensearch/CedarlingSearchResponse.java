package io.jans.cedarling.opensearch;

import java.io.IOException;
import java.util.Map;

import org.opensearch.action.search.*;
import org.opensearch.core.xcontent.XContentBuilder;

public class CedarlingSearchResponse extends SearchResponse {
    
    private static final String EXT_SECTION_NAME = "ext";

    private Map<String, Object> params;

    private CedarlingSearchResponse(
        Map<String, Object> params,
        SearchResponseSections internalResponse,
        String scrollId,
        int totalShards,
        int successfulShards,
        int skippedShards,
        long tookInMillis,
        ShardSearchFailure[] shardFailures,
        Clusters clusters) {

        super(internalResponse, scrollId, totalShards, successfulShards, skippedShards, tookInMillis, shardFailures, clusters);
        this.params = params;
    }

    public static CedarlingSearchResponse make(SearchResponse sr, Map<String, Object> params) {
        
        return new CedarlingSearchResponse(params,
            sr.getInternalResponse(),
            sr.getScrollId(),
            sr.getTotalShards(),
            sr.getSuccessfulShards(),
            sr.getSkippedShards(),
            sr.getSuccessfulShards(),
            sr.getShardFailures(),
            sr.getClusters()
        );
                        
    }
/*
    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    public Map<String, Object> getParams() {
        return this.params;
    }
*/
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject();
        innerToXContent(builder, params);

        if (this.params != null) {
            builder.startObject(EXT_SECTION_NAME);
            builder.field(CedarlingSearchResponseProcessor.TYPE, this.params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
        
    }
    
}
