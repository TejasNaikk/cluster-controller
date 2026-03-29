package io.clustercontroller.lpp.config;

import io.clustercontroller.lpp.allocation.LppShardAllocator;
import io.clustercontroller.lpp.discovery.GrailClient;
import io.clustercontroller.lpp.discovery.InMemoryGrailClient;
import io.clustercontroller.lpp.discovery.LppDiscovery;
import io.clustercontroller.lpp.orchestration.LppGoalStateOrchestrator;
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
        return new LppEtcdPathResolver(region);
    }

    @Bean
    public LppMetadataStore lppMetadataStore(Client lppEtcdClient, LppEtcdPathResolver lppEtcdPathResolver) {
        log.info("LPP: initializing LppMetadataStore (env={})", region);
        return new LppMetadataStore(lppEtcdClient, lppEtcdPathResolver);
    }

    /**
     * GrailClient — swap for a real Grail HTTP client when available.
     * InMemoryGrailClient is suitable for local dev and unit tests.
     */
    @Bean
    public GrailClient grailClient() {
        log.info("LPP: using InMemoryGrailClient (replace with real Grail client for prod)");
        return new InMemoryGrailClient();
    }

    @Bean
    public LppDiscovery lppDiscovery(GrailClient grailClient, LppMetadataStore lppMetadataStore) {
        return new LppDiscovery(grailClient, lppMetadataStore, namespace);
    }

    @Bean
    public LppShardAllocator lppShardAllocator(LppMetadataStore lppMetadataStore) {
        return new LppShardAllocator(lppMetadataStore);
    }

    @Bean
    public LppGoalStateOrchestrator lppGoalStateOrchestrator(LppMetadataStore lppMetadataStore) {
        log.info("LPP: orchestrator observeOnly={}", observeOnly);
        return new LppGoalStateOrchestrator(lppMetadataStore, observeOnly);
    }

    @Bean
    public LppStateAggregator lppStateAggregator(LppMetadataStore lppMetadataStore) {
        return new LppStateAggregator(lppMetadataStore);
    }

    @Bean
    public LppTaskContext lppTaskContext(
            LppDiscovery lppDiscovery,
            LppShardAllocator lppShardAllocator,
            LppGoalStateOrchestrator lppGoalStateOrchestrator,
            LppStateAggregator lppStateAggregator,
            LppMetadataStore lppMetadataStore) {
        return new LppTaskContext(
                lppDiscovery, lppShardAllocator, lppGoalStateOrchestrator,
                lppStateAggregator, lppMetadataStore, namespace, region);
    }
}
