package com.redis.kafka.connect;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.connect.connector.Task;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.redis.kafka.connect.common.RedisConfigDef;
import com.redis.kafka.connect.sink.RedisSinkTask;
import com.redis.kafka.connect.source.RedisKeysSourceTask;
import com.redis.kafka.connect.source.RedisStreamSourceConfigDef;
import com.redis.kafka.connect.source.RedisStreamSourceTask;

/**
 * A task whose start() fails to reach Redis must not leak the Lettuce client resources (Netty event
 * loop threads) it created. Like the Connect runtime, each attempt calls stop() after the failed
 * start(); since start() already releases its client eagerly, this also covers stop() running twice.
 */
class TaskStartFailureCleanupTest {

	private Map<String, String> props;

	@BeforeEach
	void setup() throws IOException {
		int port;
		// Bind then release an ephemeral port so connections to it are refused
		try (ServerSocket socket = new ServerSocket(0)) {
			port = socket.getLocalPort();
		}
		props = new HashMap<>();
		props.put(RedisConfigDef.URI_CONFIG, "redis://localhost:" + port);
		props.put(RedisConfigDef.TIMEOUT_CONFIG, "1");
		props.put(RedisStreamSourceConfigDef.STREAM_NAME_CONFIG, "stream");
		props.put("topics", "topic");
	}

	@Test
	void sinkTaskReleasesClientResourcesWhenStartFails() {
		assertNoLeakOnFailedStart(RedisSinkTask::new);
	}

	@Test
	void streamSourceTaskReleasesClientResourcesWhenStartFails() {
		assertNoLeakOnFailedStart(RedisStreamSourceTask::new);
	}

	@Test
	void keysSourceTaskReleasesClientResourcesWhenStartFails() {
		assertNoLeakOnFailedStart(RedisKeysSourceTask::new);
	}

	private void assertNoLeakOnFailedStart(Supplier<Task> tasks) {
		long baseline = lettuceThreadCount();
		for (int i = 0; i < 3; i++) {
			Task task = tasks.get();
			Assertions.assertThrows(RuntimeException.class, () -> task.start(props));
			Assertions.assertDoesNotThrow(task::stop);
		}
		Awaitility.await().atMost(Duration.ofSeconds(10))
				.untilAsserted(() -> Assertions.assertEquals(baseline, lettuceThreadCount()));
	}

	private static long lettuceThreadCount() {
		return Thread.getAllStackTraces().keySet().stream()
				.filter(t -> t.isAlive() && t.getName().startsWith("lettuce-"))
				.count();
	}

}
