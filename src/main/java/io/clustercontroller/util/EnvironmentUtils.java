package io.clustercontroller.util;

import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple utility for accessing config from application.yml via Spring Environment.
 * Callers pass the key they need (e.g., "controller.id", "controller.runtime_env").
 */
@Component
public class EnvironmentUtils {
    
    private static Environment springEnvironment;
    
    // Test overrides - takes precedence over Spring Environment
    private static final Map<String, String> testOverrides = new ConcurrentHashMap<>();
    
    private final Environment environment;
    
    public EnvironmentUtils(Environment environment) {
        this.environment = environment;
    }
    
    @PostConstruct
    public void init() {
        springEnvironment = this.environment;
    }
    
    /**
     * Get a config value by key from application.yml.
     * 
     * @param key the property key (e.g., "controller.id", "controller.runtime_env")
     * @return the property value
     * @throws IllegalStateException if not initialized or property not set
     */
    public static String get(String key) {
        // Check test overrides first
        String override = testOverrides.get(key);
        if (override != null) {
            return override;
        }
        
        if (springEnvironment == null) {
            throw new IllegalStateException("EnvironmentUtils not initialized yet");
        }
        String value = springEnvironment.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("Property '" + key + "' is not set in application.yml");
        }
        return value.trim();
    }
    
    // ========== Test Support ==========
    
    /**
     * For testing only - override a config value.
     */
    public static void setForTesting(String key, String value) {
        testOverrides.put(key, value);
    }
    
    /**
     * For testing only - clear all overrides.
     */
    public static void clearTestOverrides() {
        testOverrides.clear();
    }
}
