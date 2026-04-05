package io.clustercontroller.lpp.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A single routable node entry within a shard's routing table entry.
 * Consumed by the LPP gateway to route requests.
 */
@Data
@NoArgsConstructor
public class LppShardRoute {

    @JsonProperty("node_name")
    private String nodeName;

    @JsonProperty("host")
    private String host;

    @JsonProperty("port")
    private int port;

    @JsonProperty("group_id")
    private String groupId;

    public LppShardRoute(String nodeName, String host, int port, String groupId) {
        this.nodeName = nodeName;
        this.host = host;
        this.port = port;
        this.groupId = groupId;
    }
}
