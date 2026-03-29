package io.clustercontroller.lpp.allocation;

import io.clustercontroller.lpp.models.LppGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Random allocation strategy.
 *
 * <p>Shuffles the eligible group pool and picks the first {@code numGroups} groups.
 * No load tracking, no zone affinity — purely random. This produces a statistically
 * uniform spread over many shards and avoids deterministic hot-spots without the
 * complexity of load-aware selection.
 *
 * <p>This is the default strategy. Replace with a load-aware or topology-aware
 * implementation when richer bin-packing is needed.
 */
public class RandomAllocationStrategy implements AllocationStrategy {

    @Override
    public List<LppGroup> selectGroups(List<LppGroup> eligibleGroups, int numGroups,
                                       Map<String, Integer> groupShardCount) {
        List<LppGroup> shuffled = new ArrayList<>(eligibleGroups);
        Collections.shuffle(shuffled);
        return shuffled.subList(0, Math.min(numGroups, shuffled.size()));
    }
}
