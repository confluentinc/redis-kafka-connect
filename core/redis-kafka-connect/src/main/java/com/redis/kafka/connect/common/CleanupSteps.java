package com.redis.kafka.connect.common;

/**
 * Runs cleanup steps so that a failing step does not skip the ones after it. The first failure is
 * rethrown by {@link #rethrow()}, with any later failures attached to it as suppressed.
 */
public class CleanupSteps {

	private RuntimeException failure;

	public void run(Runnable step) {
		try {
			step.run();
		} catch (RuntimeException e) {
			if (failure == null) {
				failure = e;
			} else {
				failure.addSuppressed(e);
			}
		}
	}

	public void rethrow() {
		if (failure != null) {
			throw failure;
		}
	}

}
