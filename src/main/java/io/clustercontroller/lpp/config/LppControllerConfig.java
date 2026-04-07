package io.clustercontroller.lpp.config;

import io.clustercontroller.election.LeaderElection;
import io.clustercontroller.lpp.allocation.LppShardAllocator;
import io.clustercontroller.lpp.allocation.RandomAllocationStrategy;
import io.clustercontroller.lpp.discovery.GrailClient;
import io.clustercontroller.lpp.discovery.GrailHttpClient;
import io.clustercontroller.lpp.discovery.InMemoryGrailClient;
import io.clustercontroller.lpp.discovery.LppDiscovery;
import io.clustercontroller.lpp.orchestration.LppGoalStateOrchestrator;
import io.clustercontroller.lpp.orchestration.LppRoutingTableOrchestrator;
import io.clustercontroller.lpp.orchestration.LppShadowNodeSimulator;
import io.clustercontroller.lpp.orchestration.LppStateAggregator;
import io.clustercontroller.lpp.store.LppEtcdPathResolver;
import io.clustercontroller.lpp.store.LppMetadataStore;
import io.clustercontroller.lpp.tasks.LppTaskContext;
import io.etcd.jetcd.Client;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Spring configuration for the LPP controller profile.
 * Activated via --spring.profiles.active=lpp
 */
@Slf4j
@Configuration
@Profile("lpp")
public class LppControllerConfig {

    @Value("${lpp.namespace:delivery-grocery}")
    private String namespace;

    @Value("${lpp.env:staging}")
    private String lppEnv;

    @Value("${lpp.region:local}")
    private String region;

    @Value("${lpp.observeOnly:false}")
    private boolean observeOnly;

    @Value("${etcd.endpoints:http://localhost:2379}")
    private String etcdEndpoints;

    @Bean
    public Client lppEtcdClient() {
        log.info("LPP: initializing etcd client → {}", etcdEndpoints);
        String[] endpoints = etcdEndpoints.split(",");
        return Client.builder().endpoints(endpoints).build();
    }

    @Bean
    public LppEtcdPathResolver lppEtcdPathResolver() {
        return new LppEtcdPathResolver(region, lppEnv);
    }

    @Bean
    public LppMetadataStore lppMetadataStore(Client lppEtcdClient, LppEtcdPathResolver lppEtcdPathResolver) {
        log.info("LPP: initializing LppMetadataStore (env={})", region);
        return new LppMetadataStore(lppEtcdClient, lppEtcdPathResolver);
    }

    @Value("${lpp.grail.enabled:false}")
    private boolean grailEnabled;

    @Value("${lpp.grail.baseUrl:http://localhost:5436}")
    private String grailBaseUrl;

    @Value("${lpp.grail.connectTimeoutMs:3000}")
    private int grailConnectTimeoutMs;

    @Value("${lpp.grail.readTimeoutMs:10000}")
    private int grailReadTimeoutMs;

    @Value("${lpp.shadow.simulateNodeReporting:false}")
    private boolean shadowSimulateNodeReporting;

    @Value("${lpp.shadow.simulatedActivationDelayMs:0}")
    private long shadowActivationDelayMs;

    /**
     * Allocation mode: HYBRID (default) or READER_WRITER.
     * HYBRID — all groups serve both ingest and search; one allocation pass per shard.
     * READER_WRITER — dedicated ingest-only and search-only group pools; two passes per shard.
     */
    @Value("${lpp.allocation.mode:HYBRID}")
    private String allocationMode;

    /**
     * GrailClient — uses real HTTP client when {@code lpp.grail.enabled=true},
     * otherwise falls back to InMemoryGrailClient (for local dev / unit tests).
     *
     * <p>To enable for staging, set in application-lpp.yml:
     * <pre>
     * lpp:
     *   grail:
     *     enabled: true
     *     baseUrl: http://grail.phx2.uberinternal.com
     * </pre>
     */
    @Bean
    public GrailClient grailClient() {
        if (grailEnabled) {
            log.info("LPP: using GrailHttpClient → {} (region={})", grailBaseUrl, region);
            return new GrailHttpClient(grailBaseUrl, region, grailConnectTimeoutMs, grailReadTimeoutMs);
        }
        log.info("LPP: using InMemoryGrailClient (set lpp.grail.enabled=true for real Grail)");
        return new InMemoryGrailClient();
    }

    @Bean
    public LppDiscovery lppDiscovery(GrailClient grailClient, LppMetadataStore lppMetadataStore) {
        return new LppDiscovery(grailClient, lppMetadataStore, namespace);
    }

    @Bean
    public LppShardAllocator lppShardAllocator(LppMetadataStore lppMetadataStore) {
        boolean hybrid = !"READER_WRITER".equalsIgnoreCase(allocationMode);
        log.info("LPP: allocator mode={} (hybrid={})", allocationMode, hybrid);
        return new LppShardAllocator(lppMetadataStore, new RandomAllocationStrategy(), hybrid);
    }

    @Bean
    public LppGoalStateOrchestrator lppGoalStateOrchestrator(LppMetadataStore lppMetadataStore) {
        log.info("LPP: orchestrator observeOnly={}", observeOnly);
        return new LppGoalStateOrchestrator(lppMetadataStore, observeOnly);
    }

    @Bean
    public LppRoutingTableOrchestrator lppRoutingTableOrchestrator(LppMetadataStore lppMetadataStore) {
        return new LppRoutingTableOrchestrator(lppMetadataStore);
    }

    @Bean
    public LppStateAggregator lppStateAggregator(LppMetadataStore lppMetadataStore) {
        return new LppStateAggregator(lppMetadataStore);
    }

    /**
     * Leader election for LPP controller. Uses the same etcd client as metadata store.
     * The election key is shared across all LPP controller instances — only one will be leader.
     */
    @Bean
    public LeaderElection lppLeaderElection(Client lppEtcdClient) {
        String nodeId = "lpp-controller-" + namespace + "-" + java.util.UUID.randomUUID();
        String electionKey = "/lpp/" + namespace + "/leader-election";
        log.info("LPP: leader election node ID = {}, key = {}", nodeId, electionKey);
        LeaderElection election = new LeaderElection(lppEtcdClient, nodeId, electionKey);
        election.startElection();
        return election;
    }

    /**
     * Shadow node simulator — activated by {@code lpp.shadow.simulateNodeReporting=true}.
     * Returns null (no bean) when shadow mode is off.
     */
    @Bean
    public LppShadowNodeSimulator lppShadowNodeSimulator(LppMetadataStore lppMetadataStore) {
        if (shadowSimulateNodeReporting) {
            log.info("LPP: shadow simulator ENABLED (activationDelayMs={})", shadowActivationDelayMs);
            return new LppShadowNodeSimulator(lppMetadataStore, shadowActivationDelayMs);
        }
        log.info("LPP: shadow simulator disabled (set lpp.shadow.simulateNodeReporting=true to enable)");
        return null;
    }

    @Bean
    public LppTaskContext lppTaskContext(
            LppDiscovery lppDiscovery,
            LppShardAllocator lppShardAllocator,
            LppGoalStateOrchestrator lppGoalStateOrchestrator,
            LppRoutingTableOrchestrator lppRoutingTableOrchestrator,
            LppStateAggregator lppStateAggregator,
            LppMetadataStore lppMetadataStore) {
        return new LppTaskContext(
                lppDiscovery, lppShardAllocator, lppGoalStateOrchestrator,
                lppRoutingTableOrchestrator, lppStateAggregator, lppMetadataStore, namespace, region);
    }
}
