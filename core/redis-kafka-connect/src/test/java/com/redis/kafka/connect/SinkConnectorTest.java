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

	// Synthetic secret embedded in a redis.uri to prove the config dump does not leak it.
	private static final String URI_SECRET = "CANARY_S3cret";
	private static final String CANARY_URI = "rediss://user:" + URI_SECRET + "@example.invalid:6379";

	/**
	 * RedisConfig extends AbstractConfig; with doLog=true the base class runs logAll() on
	 * construction and dumps the whole parsed config at INFO. redis.uri is Type.STRING, so a
	 * URI carrying embedded credentials would be logged verbatim. RedisConfig now constructs
	 * the base with doLog=false, so nothing is dumped. Capture the INFO output (slf4j-simple
	 * writes to System.err) and assert the credential never appears.
	 */
	@Test
	void configDumpDoesNotLogRedisUriCredentials() {
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

		// Positive control: the value really is parsed into the config, so if the dump ran it
		// would have logged it. Absence below therefore proves suppression, not a missed path.
		Assertions.assertEquals(CANARY_URI, config.getString(RedisConfigDef.URI_CONFIG));

		String logged = captured.toString(StandardCharsets.UTF_8);
		Assertions.assertFalse(logged.contains(URI_SECRET),
				"redis.uri credential must not be logged by the config dump");
		Assertions.assertFalse(logged.contains("RedisSinkConfig values"),
				"AbstractConfig.logAll() config dump must be suppressed");
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
