package io.clustercontroller.lpp.discovery;

import io.clustercontroller.lpp.models.LppNodeActualState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory GrailClient for unit tests and local development.
 *
 * <p>Pre-populate with {@link #addNode} to simulate a Grail topology.
 */
public class InMemoryGrailClient implements GrailClient {

    private final Map<String, List<LppNodeActualState>> nodesByNamespace = new ConcurrentHashMap<>();

    public void addNode(String namespace, LppNodeActualState node) {
        nodesByNamespace.computeIfAbsent(namespace, k -> new ArrayList<>()).add(node);
    }

    public void clearNamespace(String namespace) {
        nodesByNamespace.remove(namespace);
    }

    @Override
    public List<LppNodeActualState> fetchNodes(String namespace) {
        return nodesByNamespace.getOrDefault(namespace, List.of());
    }
}
