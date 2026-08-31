package com.redis.kafka.connect;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.kafka.connect.common.ManifestVersionProvider;
import com.redis.kafka.connect.common.RedisConfigDef;
import com.redis.kafka.connect.sink.RedisSinkConfig;
import com.redis.kafka.connect.sink.RedisSinkTask;

class SinkConnectorTest {

	// Synthetic secret embedded in a redis.uri to prove the config dump masks it.
	private static final String URI_SECRET = "CANARY_S3cret";
	private static final String CANARY_URI = "rediss://user:" + URI_SECRET + "@example.invalid:6379";

	/**
	 * RedisConfig extends AbstractConfig, whose logAll() dumps the whole parsed config at INFO on
	 * construction. redis.uri is now Type.PASSWORD, so the dump still runs (keeping the non-sensitive
	 * config visible) but masks redis.uri as [hidden] instead of logging the embedded credentials.
	 * Capture the INFO output (slf4j-simple writes to System.err) and assert the credential never
	 * appears while the dump itself is present.
	 */
	@Test
	void configDumpMasksRedisUriCredentials() {
		Map<String, String> props = new HashMap<>();
		props.put(RedisConfigDef.URI_CONFIG, CANARY_URI);

		PrintStream originalErr = System.err;
		ByteArrayOutputStream captured = new ByteArrayOutputStream();
		RedisSinkConfig config;
		try {
			System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
			config = new RedisSinkConfig(props);
		} finally {
			System.setErr(originalErr);
		}

		// Positive control: the value is still parsed and usable (read via getPassword now).
		Assertions.assertEquals(CANARY_URI, config.getPassword(RedisConfigDef.URI_CONFIG).value());

		String logged = captured.toString(StandardCharsets.UTF_8);
		// The dump still runs (only the sensitive field is masked)...
		Assertions.assertTrue(logged.contains("RedisSinkConfig values"),
				"AbstractConfig.logAll() config dump should still run");
		// ...but the redis.uri credential must be masked, never logged verbatim.
		Assertions.assertFalse(logged.contains(URI_SECRET),
				"redis.uri credential must not be logged by the config dump");
		Assertions.assertTrue(logged.contains(RedisConfigDef.URI_CONFIG + " = [hidden]"),
				"redis.uri must render as [hidden] in the config dump");
	}

	@Test
	void testConfig() {
		ConfigDef config = new RedisSinkConnector().config();
		Assertions.assertNotNull(config);
	}

	@Test
	void testTask() {
		Assertions.assertEquals(RedisSinkTask.class, new RedisSinkConnector().taskClass());
	}

	@Test
	void testTaskConfigs() {
		RedisSinkConnector connector = new RedisSinkConnector();
		HashMap<String, String> props = new HashMap<>();
		props.put("field1", "value1");
		connector.start(props);
		int maxTasks = 123;
		Assertions.assertEquals(props, connector.taskConfigs(maxTasks).get(0));
		Assertions.assertEquals(maxTasks, connector.taskConfigs(maxTasks).size());
	}

	@Test
	void testVersion() {
		Assertions.assertEquals(ManifestVersionProvider.getVersion(), new RedisSinkConnector().version());
	}

}
