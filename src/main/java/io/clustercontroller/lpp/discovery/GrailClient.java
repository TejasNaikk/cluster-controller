package io.clustercontroller.lpp.discovery;

import io.clustercontroller.lpp.models.LppNodeActualState;

import java.util.List;

/**
 * Abstraction over Grail (Uber's service discovery) for fetching LPP node topology.
 *
 * <p>Grail knows the full deployment topology — which nodes exist, which Odin instance
 * they belong to, their zone, role, and the Grail shard ID. This interface lets the
 * LPP controller bootstrap node discovery before LPP nodes begin self-reporting
 * actual-state to etcd.
 *
 * <p>Production implementation will call the Grail gRPC API. The in-process mock
 * ({@link InMemoryGrailClient}) is used for unit tests.
 */
public interface GrailClient {

    /**
     * Fetch the current live node topology for a given Odin deployment namespace.
     *
     * @param namespace Odin namespace / usecase prefix (e.g., "delivery-grocery")
     * @return list of node actual states derived from Grail topology
     */
    List<LppNodeActualState> fetchNodes(String namespace);
}
