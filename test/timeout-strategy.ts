import {test, mock} from 'node:test';
import assert from 'node:assert/strict';
import delay from 'delay';
import PQueue, {TimeoutError} from '../source/index.js';

// Helper to attach a no-op catch handler to prevent unhandled rejections
// when the timeout fires before the test attaches its assertion handler.
const swallow = (promise: Promise<unknown>) => {
	promise.catch(() => {});
};

test('strict timeout strategy holds concurrency slot until original settles', async () => {
	const queue = new PQueue({
		concurrency: 1,
		timeout: 100,
		timeoutStrategy: 'strict',
	});

	const events: string[] = [];

	const task1 = queue.add(async () => {
		events.push('task1-started');
		await delay(500);
		events.push('task1-finished');
		return 'task1-result';
	});
	// Attach handler immediately to prevent unhandled rejection when timeout fires
	task1.catch(() => {});

	const task2 = queue.add(async () => {
		events.push('task2-started');
		return 'task2-result';
	});

	await delay(150);
	await assert.rejects(task1, TimeoutError);

	assert.deepEqual(events, ['task1-started'], 'Task 2 should not start while task 1 is still running');
	assert.equal(queue.pending, 1, 'Task 1 still holds the slot');
	assert.equal(queue.timedOut, 1, 'Task 1 is timed-out in-flight');
	assert.equal(queue.size, 1, 'Task 2 is waiting');

	await delay(400);

	assert.deepEqual(events, ['task1-started', 'task1-finished', 'task2-started'], 'Task 2 starts only after task 1 truly ends');

	const result2 = await task2;
	assert.equal(result2, 'task2-result');

	assert.equal(queue.size, 0);
	assert.equal(queue.pending, 0);
	assert.equal(queue.timedOut, 0);
	assert.equal(queue.running, 0);
});

test('strict timeout strategy does not re-resolve late success', async () => {
	const queue = new PQueue({
		concurrency: 1,
		timeout: 50,
		timeoutStrategy: 'strict',
	});

	const task = queue.add(async () => {
		await delay(200);
		return 'late-success';
	});

	await assert.rejects(task, TimeoutError);
	await delay(200);
	await assert.rejects(task, TimeoutError, 'Late success must not overwrite the rejection');

	await queue.onIdle();
});

test('default timeout strategy releases slot immediately on timeout', async () => {
	const queue = new PQueue({
		concurrency: 1,
		timeout: 100,
	});

	const events: string[] = [];

	const task1 = queue.add(async () => {
		events.push('task1-started');
		await delay(500);
		events.push('task1-finished');
		return 'task1-result';
	});
	swallow(task1);

	const task2 = queue.add(async () => {
		events.push('task2-started');
		return 'task2-result';
	});

	await delay(200);
	await assert.rejects(task1, TimeoutError);

	assert.deepEqual(events, ['task1-started', 'task2-started'], 'Task 2 should start after task 1 times out (default strategy)');

	const result2 = await task2;
	assert.equal(result2, 'task2-result');

	await delay(400);
	assert.deepEqual(events, ['task1-started', 'task2-started', 'task1-finished']);

	assert.equal(queue.size, 0);
	assert.equal(queue.pending, 0);
	assert.equal(queue.timedOut, 0);
});

test('queue stats distinguish waiting, running, and timed-out in strict mode', async () => {
	const queue = new PQueue({
		concurrency: 2,
		timeout: 100,
		timeoutStrategy: 'strict',
	});

	swallow(queue.add(async () => delay(500)));
	swallow(queue.add(async () => delay(500)));
	queue.add(async () => delay(10));
	queue.add(async () => delay(10));

	await delay(150);

	assert.equal(queue.size, 2, 'Two tasks waiting');
	assert.equal(queue.pending, 2, 'Two tasks holding slots (timed out)');
	assert.equal(queue.timedOut, 2, 'Two tasks timed out in-flight');
	assert.equal(queue.running, 0, 'Zero tasks actually running (both timed out)');

	await delay(400);
	await queue.onIdle();

	assert.equal(queue.size, 0);
	assert.equal(queue.pending, 0);
	assert.equal(queue.timedOut, 0);
	assert.equal(queue.running, 0);
});

test('queue stats distinguish waiting, running, and timed-out in default mode', async () => {
	const queue = new PQueue({
		concurrency: 2,
		timeout: 100,
	});

	swallow(queue.add(async () => delay(500)));
	swallow(queue.add(async () => delay(500)));
	swallow(queue.add(async () => delay(200)));
	swallow(queue.add(async () => delay(200)));

	await delay(150);

	assert.equal(queue.size, 0, 'No tasks waiting');
	assert.equal(queue.pending, 2, 'Two tasks running (tasks 3 and 4)');
	assert.equal(queue.timedOut, 2, 'Two tasks timed out in background');
	assert.equal(queue.running, 2, 'Two tasks actually running');

	// Wait for all timed-out tasks to settle (default strategy does not hold
	// the slot, so onIdle resolves when pending hits zero, before background
	// functions finish). Poll until timedOut returns to zero.
	while (queue.timedOut > 0) {
		await delay(50);
	}

	assert.equal(queue.size, 0);
	assert.equal(queue.pending, 0);
	assert.equal(queue.timedOut, 0);
	assert.equal(queue.running, 0);
});

test('cancelling a queued task does not start the function', async () => {
	const queue = new PQueue({
		concurrency: 1,
		autoStart: false,
	});

	let functionStarted = false;
	const controller = new AbortController();

	const task = queue.add(async () => {
		functionStarted = true;
		return 'should-not-run';
	}, {signal: controller.signal});

	controller.abort();
	await assert.rejects(task);

	assert.equal(functionStarted, false, 'Cancelled queued task should not start the function');
	assert.equal(queue.size, 0);
	assert.equal(queue.pending, 0);
});

test('cancelling a queued task before start does not start function (strict timeout)', async () => {
	const queue = new PQueue({
		concurrency: 1,
		timeout: 100,
		timeoutStrategy: 'strict',
		autoStart: false,
	});

	let functionStarted = false;
	const controller = new AbortController();

	const task = queue.add(async () => {
		functionStarted = true;
		return 'should-not-run';
	}, {signal: controller.signal});

	controller.abort();
	await assert.rejects(task);

	assert.equal(functionStarted, false, 'Cancelled queued task should not start the function');
	assert.equal(queue.size, 0);
	assert.equal(queue.pending, 0);
	assert.equal(queue.timedOut, 0);
});

test('strict timeout strategy with concurrency 2', async () => {
	const queue = new PQueue({
		concurrency: 2,
		timeout: 100,
		timeoutStrategy: 'strict',
	});

	const events: string[] = [];

	swallow(queue.add(async () => {
		events.push('task1-started');
		await delay(300);
		events.push('task1-finished');
	}));

	swallow(queue.add(async () => {
		events.push('task2-started');
		await delay(300);
		events.push('task2-finished');
	}));

	queue.add(async () => {
		events.push('task3-started');
		await delay(10);
		events.push('task3-finished');
	});
	queue.add(async () => {
		events.push('task4-started');
		await delay(10);
		events.push('task4-finished');
	});

	await delay(150);

	assert.deepEqual(events, ['task1-started', 'task2-started']);
	assert.equal(queue.timedOut, 2);
	assert.equal(queue.size, 2);

	await delay(200);

	// With strict timeout strategy, tasks 3 and 4 only start after tasks 1
	// and 2 finish. With concurrency 2, task 3 starts as soon as either task
	// 1 or 2 finishes, and task 4 starts after both finish. The exact finish
	// order of tasks 1 and 2 is non-deterministic (they run concurrently).
	const task1FinishedIdx = events.indexOf('task1-finished');
	const task2FinishedIdx = events.indexOf('task2-finished');
	const task3StartedIdx = events.indexOf('task3-started');
	const task4StartedIdx = events.indexOf('task4-started');

	assert.ok(task1FinishedIdx !== -1, 'task1 finished');
	assert.ok(task2FinishedIdx !== -1, 'task2 finished');
	const firstFinishedIdx = Math.min(task1FinishedIdx, task2FinishedIdx);
	const lastFinishedIdx = Math.max(task1FinishedIdx, task2FinishedIdx);
	assert.ok(task3StartedIdx > firstFinishedIdx, 'task3 starts after first task finishes');
	assert.ok(task4StartedIdx > lastFinishedIdx, 'task4 starts after both tasks finish');

	await queue.onIdle();

	assert.equal(queue.size, 0);
	assert.equal(queue.pending, 0);
	assert.equal(queue.timedOut, 0);
	assert.equal(queue.running, 0);
});

test('strict timeout strategy does not forcibly terminate non-cooperative functions', async () => {
	const queue = new PQueue({
		concurrency: 1,
		timeout: 50,
		timeoutStrategy: 'strict',
	});

	let functionCompleted = false;

	const task = queue.add(async () => {
		await delay(300);
		functionCompleted = true;
		return 'done';
	});

	await assert.rejects(task, TimeoutError);
	await delay(400);

	assert.equal(functionCompleted, true, 'Non-cooperative function should not be forcibly terminated');
	await queue.onIdle();
});

test('strict timeout strategy with task that resolves before timeout', async () => {
	const queue = new PQueue({
		concurrency: 1,
		timeout: 200,
		timeoutStrategy: 'strict',
	});

	const result = await queue.add(async () => {
		await delay(50);
		return 'success';
	});

	// Wait for the run function to fully complete (settleCaller resolves the
	// caller's promise before the strict-mode slot release finishes).
	await queue.onIdle();

	assert.equal(result, 'success');
	assert.equal(queue.size, 0);
	assert.equal(queue.pending, 0);
	assert.equal(queue.timedOut, 0);
	assert.equal(queue.running, 0);
});

test('strict timeout strategy with task that rejects before timeout', async () => {
	const queue = new PQueue({
		concurrency: 1,
		timeout: 200,
		timeoutStrategy: 'strict',
	});

	const task = queue.add(async () => {
		await delay(50);
		throw new Error('task-error');
	});

	await assert.rejects(task, {message: 'task-error'});
	await queue.onIdle();

	assert.equal(queue.size, 0);
	assert.equal(queue.pending, 0);
	assert.equal(queue.timedOut, 0);
	assert.equal(queue.running, 0);
});

test('timeoutStrategy defaults to default', async () => {
	const queue = new PQueue({
		concurrency: 1,
		timeout: 100,
	});

	const events: string[] = [];

	swallow(queue.add(async () => {
		events.push('task1-started');
		await delay(500);
		events.push('task1-finished');
	}));

	queue.add(async () => {
		events.push('task2-started');
	});

	await delay(200);
	assert.deepEqual(events, ['task1-started', 'task2-started']);
	await queue.onIdle();
});

test('strict timeout strategy per-task override', async () => {
	const queue = new PQueue({
		concurrency: 1,
		timeout: 100,
		timeoutStrategy: 'strict',
	});

	const events: string[] = [];

	const task1 = queue.add(async () => {
		events.push('task1-started');
		await delay(500);
		events.push('task1-finished');
	});
	swallow(task1);

	const task2 = queue.add(async () => {
		events.push('task2-started');
	});

	await delay(150);
	await assert.rejects(task1, TimeoutError);
	assert.deepEqual(events, ['task1-started']);
	await delay(400);
	assert.deepEqual(events, ['task1-started', 'task1-finished', 'task2-started']);
	await task2;
	await queue.onIdle();
});

test('strict timeout strategy with no timeout set', async () => {
	const queue = new PQueue({
		concurrency: 1,
		timeoutStrategy: 'strict',
	});

	const result = await queue.add(async () => {
		await delay(50);
		return 'no-timeout';
	});

	assert.equal(result, 'no-timeout');
	assert.equal(queue.size, 0);
	assert.equal(queue.pending, 0);
	assert.equal(queue.timedOut, 0);
});

test('runningTasks reflects timed-out state in strict mode', async () => {
	const queue = new PQueue({
		concurrency: 1,
		timeout: 100,
		timeoutStrategy: 'strict',
	});

	swallow(queue.add(async () => delay(500), {id: 'slow-task'}));
	await delay(50);
	let running = queue.runningTasks;
	assert.equal(running.length, 1);
	assert.equal(running[0].id, 'slow-task');
	await delay(100);
	running = queue.runningTasks;
	assert.equal(running.length, 1, 'Timed-out task should still be in runningTasks in strict mode');
	assert.equal(queue.timedOut, 1);
	await delay(400);
	running = queue.runningTasks;
	assert.equal(running.length, 0);
	assert.equal(queue.timedOut, 0);
	await queue.onIdle();
});

test('virtual time: strict timeout strategy holds slot until original settles', async () => {
	mock.timers.enable({apis: ['setTimeout', 'Date']});
	try {
		const queue = new PQueue({
			concurrency: 1,
			timeout: 100,
			timeoutStrategy: 'strict',
		});

		const events: string[] = [];

		const task1 = queue.add(async () => {
			events.push('task1-started');
			await delay(500);
			events.push('task1-finished');
			return 'task1-result';
		});
		swallow(task1);

		const task2 = queue.add(async () => {
			events.push('task2-started');
			return 'task2-result';
		});

		mock.timers.tick(150);
		await new Promise(resolve => setImmediate(resolve));

		await assert.rejects(task1, TimeoutError);

		assert.deepEqual(events, ['task1-started'], 'Task 2 should not start while task 1 is still running (virtual time)');
		assert.equal(queue.timedOut, 1);
		assert.equal(queue.size, 1);

		mock.timers.tick(400);
		await new Promise(resolve => setImmediate(resolve));

		assert.deepEqual(events, ['task1-started', 'task1-finished', 'task2-started'], 'Task 2 starts only after task 1 truly ends (virtual time)');

		const result2 = await task2;
		assert.equal(result2, 'task2-result');

		await queue.onIdle();

		assert.equal(queue.size, 0);
		assert.equal(queue.pending, 0);
		assert.equal(queue.timedOut, 0);
		assert.equal(queue.running, 0);
	} finally {
		mock.timers.reset();
	}
});

test('virtual time: default timeout strategy releases slot immediately', async () => {
	mock.timers.enable({apis: ['setTimeout', 'Date']});
	try {
		const queue = new PQueue({
			concurrency: 1,
			timeout: 100,
		});

		const events: string[] = [];

		const task1 = queue.add(async () => {
			events.push('task1-started');
			await delay(500);
			events.push('task1-finished');
		});
		swallow(task1);

		const task2 = queue.add(async () => {
			events.push('task2-started');
		});

		mock.timers.tick(150);
		await new Promise(resolve => setImmediate(resolve));

		await assert.rejects(task1, TimeoutError);

		assert.deepEqual(events, ['task1-started', 'task2-started'], 'Task 2 should start after task 1 times out (default strategy, virtual time)');

		await task2;

		mock.timers.tick(400);
		await new Promise(resolve => setImmediate(resolve));

		await queue.onIdle();

		assert.equal(queue.size, 0);
		assert.equal(queue.pending, 0);
		assert.equal(queue.timedOut, 0);
	} finally {
		mock.timers.reset();
	}
});

test('dual-task example: strict timeout strategy', async () => {
	// This example demonstrates the strict timeout slot strategy with two tasks.
	// Task 1 times out at 100ms but the function keeps running until 500ms.
	// With the strict strategy, Task 2 only starts after Task 1's function truly ends.
	const queue = new PQueue({
		concurrency: 1,
		timeout: 100,
		timeoutStrategy: 'strict',
	});

	const timeline: Array<{time: number; event: string}> = [];
	const startTime = Date.now();

	const log = (event: string) => {
		timeline.push({time: Date.now() - startTime, event});
	};

	// Task 1: long-running function that times out at 100ms
	const task1 = queue.add(async () => {
		log('task1-started');
		await delay(500);
		log('task1-finished');
		return 'task1-result';
	});

	// Task 2: should only start after task1 truly finishes
	const task2 = queue.add(async () => {
		log('task2-started');
		return 'task2-result';
	});

	// Task 1 rejects with TimeoutError at ~100ms
	await assert.rejects(task1, TimeoutError);
	log('task1-timed-out');

	// Task 2 should not have started yet
	assert.equal(queue.size, 1, 'Task 2 is still waiting');
	assert.equal(queue.timedOut, 1, 'Task 1 is timed-out in-flight');

	// Wait for task1 to finish (~500ms) and task2 to run
	const result2 = await task2;
	log('task2-completed');

	assert.equal(result2, 'task2-result');

	// Verify the timeline: task1-started, task1-timed-out, task1-finished, task2-started, task2-completed
	const events = timeline.map(t => t.event);
	assert.deepEqual(events, [
		'task1-started',
		'task1-timed-out',
		'task1-finished',
		'task2-started',
		'task2-completed',
	]);

	// Task 2 started after task1 finished (not after timeout)
	const task1Finished = timeline.find(t => t.event === 'task1-finished')!;
	const task2Started = timeline.find(t => t.event === 'task2-started')!;
	assert.ok(task2Started.time >= task1Finished.time, 'Task 2 starts after task 1 finishes');

	// Wait for all microtasks to flush so stats settle to zero
	await new Promise(resolve => setImmediate(resolve));

	// All stats should be zero
	assert.equal(queue.size, 0);
	assert.equal(queue.pending, 0);
	assert.equal(queue.timedOut, 0);
	assert.equal(queue.running, 0);

	await queue.onIdle();
});
