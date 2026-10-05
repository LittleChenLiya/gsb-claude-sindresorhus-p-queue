// Run with virtual (mocked) time:
//   node --import=tsx/esm --test test/timeout-slot-policy.ts
import {test} from 'node:test';
import assert from 'node:assert/strict';
import {setImmediate} from 'node:timers';
import delay from 'delay';
import {TimeoutError} from 'p-timeout';
import PQueue from '../source/index.js';

// Flush pending microtasks/promise reactions. `setImmediate` is not mocked, so this works with virtual time.
const flushMicrotasks = async () => {
	for (let index = 0; index < 10; index++) {
		// eslint-disable-next-line no-await-in-loop
		await new Promise(resolve => {
			setImmediate(resolve);
		});
	}
};

const settle = async <T>(promise: Promise<T>): Promise<{status: 'fulfilled'; value: T} | {status: 'rejected'; reason: unknown}> => promise.then(
	value => ({status: 'fulfilled' as const, value}),
	reason => ({status: 'rejected' as const, reason}),
);

test('strict slot policy: timed-out task holds its concurrency slot until its promise actually settles', async t => {
	t.mock.timers.enable({apis: ['setTimeout'], now: 1000});

	const queue = new PQueue({concurrency: 1, timeout: 100, timeoutSlotPolicy: 'strict'});

	const events: string[] = [];

	// Task 1 runs for 300ms but times out (for the caller) after 100ms.
	const first = settle(queue.add(async () => {
		events.push('first:start');
		await delay(300);
		events.push('first:end');
		return 'late-success';
	}));

	// Task 2 waits for the single concurrency slot.
	const second = settle(queue.add(async () => {
		events.push('second:start');
		return 'second';
	}));

	await flushMicrotasks();
	assert.deepEqual(events, ['first:start']);
	assert.deepEqual(queue.stats, {waiting: 1, running: 1, timedOut: 0});
	assert.equal(queue.pending, 1);

	// t = 1100: the timeout fires. The caller sees a TimeoutError...
	t.mock.timers.tick(100);
	await flushMicrotasks();

	const firstResult = await first;
	assert.equal(firstResult.status, 'rejected');
	assert.ok(firstResult.reason instanceof TimeoutError);

	// ...but the second task has NOT started: the slot is still held by task 1.
	assert.deepEqual(events, ['first:start']);
	assert.equal(queue.pending, 1);
	assert.deepEqual(queue.stats, {waiting: 1, running: 0, timedOut: 1});
	assert.equal(queue.runningTasks.length, 1);
	assert.equal(queue.runningTasks[0]!.timedOut, true);

	// t = 1200: still nothing. The queue does not advance while task 1 is in flight.
	t.mock.timers.tick(100);
	await flushMicrotasks();
	assert.deepEqual(events, ['first:start']);
	assert.deepEqual(queue.stats, {waiting: 1, running: 0, timedOut: 1});

	// t = 1300: task 1's underlying promise truly settles. Only now does the queue advance.
	t.mock.timers.tick(100);
	await flushMicrotasks();
	assert.deepEqual(events, ['first:start', 'first:end', 'second:start']);

	const secondResult = await second;
	assert.deepEqual(secondResult, {status: 'fulfilled', value: 'second'});

	// The late success of task 1 did not override the already-rejected result.
	const firstResultAgain = await first;
	assert.equal(firstResultAgain.status, 'rejected');
	assert.ok(firstResultAgain.reason instanceof TimeoutError);

	await queue.onIdle();

	// Statistics return to zero once everything has settled.
	assert.deepEqual(queue.stats, {waiting: 0, running: 0, timedOut: 0});
	assert.equal(queue.pending, 0);
	assert.equal(queue.size, 0);
	assert.equal(queue.runningTasks.length, 0);
});

test('strict slot policy: late success does not re-emit completed nor resolve the caller', async t => {
	t.mock.timers.enable({apis: ['setTimeout'], now: 1000});

	const queue = new PQueue({concurrency: 1, timeout: 100, timeoutSlotPolicy: 'strict'});

	const completed: unknown[] = [];
	const errors: unknown[] = [];
	queue.on('completed', result => {
		completed.push(result);
	});
	queue.on('error', error => {
		errors.push(error);
	});

	const first = settle(queue.add(async () => {
		await delay(300);
		return 'late-success';
	}));

	const second = settle(queue.add(async () => 'second'));

	await flushMicrotasks();
	t.mock.timers.tick(100); // Task 1 times out
	await flushMicrotasks();
	t.mock.timers.tick(200); // Task 1 settles late, task 2 runs
	await flushMicrotasks();

	const firstResult = await first;
	assert.equal(firstResult.status, 'rejected');
	assert.ok(firstResult.reason instanceof TimeoutError);
	assert.deepEqual(await second, {status: 'fulfilled', value: 'second'});

	// 'completed' fired only for the second task; the late success was not reported.
	assert.deepEqual(completed, ['second']);
	assert.equal(errors.length, 1);
	assert.ok(errors[0] instanceof TimeoutError);

	await queue.onIdle();
	assert.deepEqual(queue.stats, {waiting: 0, running: 0, timedOut: 0});
});

test('default slot policy is unchanged: timeout releases the slot immediately', async t => {
	t.mock.timers.enable({apis: ['setTimeout'], now: 1000});

	const queue = new PQueue({concurrency: 1, timeout: 100});

	const events: string[] = [];

	const first = settle(queue.add(async () => {
		events.push('first:start');
		await delay(300);
		events.push('first:end');
		return 'late-success';
	}));

	const second = settle(queue.add(async () => {
		events.push('second:start');
		await delay(50);
		return 'second';
	}));

	await flushMicrotasks();
	assert.deepEqual(events, ['first:start']);
	assert.deepEqual(queue.stats, {waiting: 1, running: 1, timedOut: 0});

	// t = 1100: timeout fires and the slot is released right away (default behavior).
	t.mock.timers.tick(100);
	await flushMicrotasks();

	const firstResult = await first;
	assert.equal(firstResult.status, 'rejected');
	assert.ok(firstResult.reason instanceof TimeoutError);
	assert.deepEqual(events, ['first:start', 'second:start']);
	assert.deepEqual(queue.stats, {waiting: 0, running: 1, timedOut: 0});

	t.mock.timers.tick(50); // t = 1150: task 2's delay elapses; flush so its timeout is cleared
	await flushMicrotasks();
	t.mock.timers.tick(150); // t = 1300: task 1's underlying promise settles late
	await flushMicrotasks();
	assert.deepEqual(events, ['first:start', 'second:start', 'first:end']);
	assert.deepEqual(await second, {status: 'fulfilled', value: 'second'});

	await queue.onIdle();
	assert.deepEqual(queue.stats, {waiting: 0, running: 0, timedOut: 0});
});

test('per-task timeoutSlotPolicy overrides the queue default', async t => {
	t.mock.timers.enable({apis: ['setTimeout'], now: 1000});

	// Queue default is 'release', this task opts into 'strict'.
	const queue = new PQueue({concurrency: 1, timeout: 100});

	const events: string[] = [];

	const first = settle(queue.add(async () => {
		events.push('first:start');
		await delay(300);
		events.push('first:end');
	}, {timeoutSlotPolicy: 'strict'}));

	const second = settle(queue.add(async () => {
		events.push('second:start');
	}));

	await flushMicrotasks();
	t.mock.timers.tick(100);
	await flushMicrotasks();

	assert.equal((await first).status, 'rejected');
	// Slot still held: second task has not started despite the queue default being 'release'.
	assert.deepEqual(events, ['first:start']);
	assert.deepEqual(queue.stats, {waiting: 1, running: 0, timedOut: 1});

	t.mock.timers.tick(200);
	await flushMicrotasks();
	assert.deepEqual(events, ['first:start', 'first:end', 'second:start']);
	assert.equal((await second).status, 'fulfilled');

	await queue.onIdle();
	assert.deepEqual(queue.stats, {waiting: 0, running: 0, timedOut: 0});
});

test('aborting a queued task never starts its function', async t => {
	t.mock.timers.enable({apis: ['setTimeout'], now: 1000});

	const queue = new PQueue({concurrency: 1, timeout: 100, timeoutSlotPolicy: 'strict'});

	const events: string[] = [];
	const controller = new AbortController();

	// Task 1 occupies the only slot (and keeps it past its timeout).
	const first = settle(queue.add(async () => {
		events.push('first:start');
		await delay(300);
		events.push('first:end');
	}));

	// Task 2 is queued behind it and aborted before it can start.
	const second = settle(queue.add(async () => {
		events.push('second:start');
	}, {signal: controller.signal}));

	await flushMicrotasks();
	assert.deepEqual(events, ['first:start']);
	assert.equal(queue.size, 1);

	controller.abort();
	await flushMicrotasks();

	const secondResult = await second;
	assert.equal(secondResult.status, 'rejected');
	assert.deepEqual(events, ['first:start']); // The cancelled function never ran.
	assert.equal(queue.size, 0);

	// Task 1 times out, then truly finishes; the queue drains without ever running task 2.
	t.mock.timers.tick(300);
	await flushMicrotasks();
	assert.equal((await first).status, 'rejected');
	assert.deepEqual(events, ['first:start', 'first:end']);

	await queue.onIdle();
	assert.deepEqual(queue.stats, {waiting: 0, running: 0, timedOut: 0});
});

test('invalid timeoutSlotPolicy throws', async () => {
	const isTimeoutSlotPolicyTypeError = (error: unknown) => error instanceof TypeError && /timeoutSlotPolicy/.test(error.message);

	assert.throws(
		() => new PQueue({timeoutSlotPolicy: 'bogus' as never}),
		isTimeoutSlotPolicyTypeError,
	);

	const queue = new PQueue();
	await assert.rejects(
		queue.add(async () => {}, {timeoutSlotPolicy: 'bogus' as never}),
		isTimeoutSlotPolicyTypeError,
	);
});
