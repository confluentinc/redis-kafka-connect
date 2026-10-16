package com.redis.kafka.connect.common;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.lettuce.core.RedisCredentials;
import io.lettuce.core.RedisCredentialsProvider;
import io.lettuce.core.RedisURI;
import reactor.core.publisher.Mono;

class RedisConfigCredentialsProviderTest {

    private static final String AWS_KEY = RedisConfig.REDIS_IAM_ASSUME_CREDENTIALS_PROVIDER_CLASS_KEY;

    private static final String AZURE_KEY = RedisConfig.REDIS_ENTRA_ID_CREDENTIALS_PROVIDER_CLASS_KEY;

    @Test
    void usesAwsKey() {
        Map<String, String> configs = new HashMap<>();
        configs.put(AWS_KEY, "com.example.AwsProvider");
        Assertions.assertEquals("com.example.AwsProvider", RedisConfig.credentialsProviderClassName(configs));
    }

    @Test
    void usesAzureKey() {
        Map<String, String> configs = new HashMap<>();
        configs.put(AZURE_KEY, "com.example.AzureProvider");
        Assertions.assertEquals("com.example.AzureProvider", RedisConfig.credentialsProviderClassName(configs));
    }

    @Test
    void emptyKeysMeanNoProvider() {
        Map<String, String> configs = new HashMap<>();
        Assertions.assertNull(RedisConfig.credentialsProviderClassName(configs));
        configs.put(AWS_KEY, "");
        configs.put(AZURE_KEY, "");
        Assertions.assertEquals("", RedisConfig.credentialsProviderClassName(configs));
    }

    @Test
    void rejectsBothKeys() {
        Map<String, String> configs = new HashMap<>();
        configs.put(AWS_KEY, "com.example.AwsProvider");
        configs.put(AZURE_KEY, "com.example.AzureProvider");
        Assertions.assertThrows(ConfigException.class, () -> RedisConfig.credentialsProviderClassName(configs));
    }

    @Test
    void uriUsesProviderFromAzureKey() {
        Map<String, String> configs = new HashMap<>();
        configs.put(RedisConfigDef.HOST_CONFIG, "localhost");
        configs.put(AZURE_KEY, StubCredentialsProvider.class.getName());
        RedisConfig config = new RedisConfig(new RedisConfigDef() {
        }, configs) {
        };

        RedisURI uri = config.uri();

        Assertions.assertInstanceOf(StubCredentialsProvider.class, uri.getCredentialsProvider());
        Assertions.assertTrue(((StubCredentialsProvider) uri.getCredentialsProvider()).configured);
    }

    /** A provider the core can instantiate and configure reflectively. */
    public static class StubCredentialsProvider implements RedisCredentialsProvider, Configurable {

        boolean configured;

        @Override
        public void configure(Map<String, ?> configs) {
            configured = true;
        }

        @Override
        public Mono<RedisCredentials> resolveCredentials() {
            return Mono.just(RedisCredentials.just("user", "token"));
        }

    }

}
