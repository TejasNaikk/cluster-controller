package io.clustercontroller.api.models.responses;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response model for index readiness check.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class IndexReadinessResponse {
    private String index;
    private boolean ready;
    private String message;
    
    public static IndexReadinessResponse ready(String indexName) {
        return IndexReadinessResponse.builder()
            .index(indexName)
            .ready(true)
            .message("Index is ready!")
            .build();
    }
    
    public static IndexReadinessResponse notReady(String indexName, String reason) {
        return IndexReadinessResponse.builder()
            .index(indexName)
            .ready(false)
            .message(reason)
            .build();
    }
}

