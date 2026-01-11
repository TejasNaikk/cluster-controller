package io.clustercontroller.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for EnvironmentUtils
 */
@ExtendWith(MockitoExtension.class)
class EnvironmentUtilsTest {

    @Mock
    private Environment mockEnvironment;

    @BeforeEach
    void setUp() {
        EnvironmentUtils.clearTestOverrides();
    }

    @Test
    void testGetWithValidValue() throws Exception {
        // Set up mock environment
        setSpringEnvironment(mockEnvironment);
        when(mockEnvironment.getProperty("test.key")).thenReturn("test-value");
        
        String result = EnvironmentUtils.get("test.key");
        
        assertEquals("test-value", result);
        verify(mockEnvironment).getProperty("test.key");
    }

    @Test
    void testGetWithMissingValue() throws Exception {
        // Set up mock environment
        setSpringEnvironment(mockEnvironment);
        when(mockEnvironment.getProperty("missing.key")).thenReturn(null);
        
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> EnvironmentUtils.get("missing.key")
        );
        
        assertTrue(exception.getMessage().contains("missing.key"));
        assertTrue(exception.getMessage().contains("not set"));
    }

    @Test
    void testGetWithEmptyValue() throws Exception {
        // Set up mock environment
        setSpringEnvironment(mockEnvironment);
        when(mockEnvironment.getProperty("empty.key")).thenReturn("   ");
        
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> EnvironmentUtils.get("empty.key")
        );
        
        assertTrue(exception.getMessage().contains("empty.key"));
    }

    @Test
    void testGetTrimsValue() throws Exception {
        // Set up mock environment
        setSpringEnvironment(mockEnvironment);
        when(mockEnvironment.getProperty("trim.key")).thenReturn("  value-with-spaces  ");
        
        String result = EnvironmentUtils.get("trim.key");
        
        assertEquals("value-with-spaces", result);
    }

    @Test
    void testGetThrowsWhenSpringNotInitialized() throws Exception {
        // Clear Spring environment
        setSpringEnvironment(null);
        
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> EnvironmentUtils.get("any.key")
        );
        
        assertTrue(exception.getMessage().contains("not initialized"));
    }

    /**
     * Helper to set the static springEnvironment field for testing
     */
    private void setSpringEnvironment(Environment env) throws Exception {
        Field field = EnvironmentUtils.class.getDeclaredField("springEnvironment");
        field.setAccessible(true);
        field.set(null, env);
    }
}
