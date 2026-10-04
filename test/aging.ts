import {test} from 'node:test';
import assert from 'node:assert/strict';
import pDefer from 'p-defer';
import PQueue from '../source/index.js';
import PriorityQueue from '../source/priority-queue.js';

const createRun = (value: string) => async () => value;

// Virtual clock: tests advance the clock themselves, so no real timers are involved.
const createClock = (initial = 0) => {
	let now = initial;
	return {
		now: () => now,
		advance: (milliseconds: number) => {
			now += milliseconds;
		},
		set: (milliseconds: number) => {
			now = milliseconds;
		},
	};
};

test('aging - effective priority grows with waited periods', () => {
	const clock = createClock();
	const queue = new PriorityQueue({aging: {period: 10, clock: clock.now}});
	const low = createRun('low');
	const high = createRun('high');

	queue.enqueue(low, {priority: 0});
	clock.advance(30); // low has waited 3 periods -> effective priority 3
	queue.enqueue(high, {priority: 2}); // high just arrived -> effective priority 2

	assert.equal(queue.dequeue(), low);
	assert.equal(queue.dequeue(), high);
});

test('aging - equal effective priorities dequeue in enqueue order', () => {
	const clock = createClock();
	const queue = new PriorityQueue({aging: {period: 10, clock: clock.now}});
	const first = createRun('first');
	const second = createRun('second');
	const third = createRun('third');

	queue.enqueue(first, {priority: 0});
	queue.enqueue(second, {priority: 0});
	clock.advance(10); // All three reach effective priority 1
	queue.enqueue(third, {priority: 1});

	assert.equal(queue.dequeue(), first);
	assert.equal(queue.dequeue(), second);
	assert.equal(queue.dequeue(), third);
});

test('aging - setPriority updates the baseline without resetting wait time', () => {
	const clock = createClock();
	const queue = new PriorityQueue({aging: {period: 10, clock: clock.now}});
	const upgraded = createRun('upgraded');
	const fresh = createRun('fresh');

	queue.enqueue(upgraded, {priority: 0, id: 'upgraded'});
	clock.advance(20); // Waited 2 periods
	queue.setPriority('upgraded', 1); // Effective priority is 1 + 2 = 3
	queue.enqueue(fresh, {priority: 2}); // Effective priority is 2

	assert.equal(queue.dequeue(), upgraded);
	assert.equal(queue.dequeue(), fresh);
});

test('aging - clock rollback never lowers effective priority below the baseline', () => {
	const clock = createClock(100);
	const queue = new PriorityQueue({aging: {period: 10, clock: clock.now}});
	const low = createRun('low');
	const high = createRun('high');

	queue.enqueue(low, {priority: 0});
	clock.set(0); // The clock moves backwards before the next task is enqueued
	queue.enqueue(high, {priority: 2});

	// low must not get a negative effective priority from the rollback.
	assert.equal(queue.dequeue(), high);
	assert.equal(queue.dequeue(), low);
});

test('aging - remove splices items out immediately without a placeholder', () => {
	const clock = createClock();
	const queue = new PriorityQueue({aging: {period: 10, clock: clock.now}});
	const first = createRun('first');
	const second = createRun('second');

	queue.enqueue(first, {id: 'first'});
	queue.enqueue(second, {id: 'second'});
	queue.remove('first');

	assert.equal(queue.size, 1);
	assert.equal(queue.dequeue(), second);
	assert.equal(queue.size, 0);
});

test('aging - invalid period throws', () => {
	assert.throws(() => new PriorityQueue({aging: {period: 0}}), {name: 'TypeError'});
	assert.throws(() => new PriorityQueue({aging: {period: -1}}), {name: 'TypeError'});
	assert.throws(() => new PriorityQueue({aging: {period: Number.NaN}}), {name: 'TypeError'});
	assert.throws(() => new PriorityQueue({aging: {period: Number.POSITIVE_INFINITY}}), {name: 'TypeError'});
});

test('aging - invalid clock throws', () => {
	assert.throws(() => new PriorityQueue({aging: {period: 10, clock: 'now' as unknown as () => number}}), {name: 'TypeError'});
});

test('aging - low-priority task that waits gains effective priority and runs first', async () => {
	const clock = createClock();
	const queue = new PQueue({concurrency: 1, aging: {period: 10, clock: clock.now}});
	const order: string[] = [];
	const blocker = pDefer<void>();

	queue.add(() => blocker.promise, {priority: 0});
	queue.add(async () => {
		order.push('old');
	}, {priority: 0});

	clock.advance(30); // The old task reaches effective priority 3
	queue.add(async () => {
		order.push('new');
	}, {priority: 2});

	blocker.resolve();
	await queue.onIdle();

	assert.deepEqual(order, ['old', 'new']);
});

test('aging - equal effective priorities keep enqueue order', async () => {
	const clock = createClock();
	const queue = new PQueue({concurrency: 1, aging: {period: 10, clock: clock.now}});
	const order: string[] = [];
	const blocker = pDefer<void>();

	queue.add(() => blocker.promise, {priority: 0});
	queue.add(async () => {
		order.push('old');
	}, {priority: 0});

	clock.advance(10); // Both tasks reach effective priority 1
	queue.add(async () => {
		order.push('new');
	}, {priority: 1});

	blocker.resolve();
	await queue.onIdle();

	assert.deepEqual(order, ['old', 'new']);
});

test('aging - setPriority keeps the waited time', async () => {
	const clock = createClock();
	const queue = new PQueue({concurrency: 1, aging: {period: 10, clock: clock.now}});
	const order: string[] = [];
	const blocker = pDefer<void>();

	queue.add(() => blocker.promise, {priority: 0});
	queue.add(async () => {
		order.push('upgraded');
	}, {priority: 0, id: 'upgraded'});

	clock.advance(20); // Waited 2 periods
	queue.setPriority('upgraded', 1); // Effective priority is 1 + 2 = 3
	queue.add(async () => {
		order.push('fresh');
	}, {priority: 2});

	blocker.resolve();
	await queue.onIdle();

	assert.deepEqual(order, ['upgraded', 'fresh']);
});

test('aging - aborting a queued task removes it immediately without size residue', async () => {
	const clock = createClock();
	const queue = new PQueue({concurrency: 1, aging: {period: 10, clock: clock.now}});
	const blocker = pDefer<void>();
	const controller = new AbortController();

	queue.add(() => blocker.promise, {priority: 0});
	queue.add(() => 'aborted', {priority: 0, signal: controller.signal}).catch(() => {});
	queue.add(() => 'kept', {priority: 0});

	assert.equal(queue.size, 2);
	controller.abort();
	assert.equal(queue.size, 1); // No placeholder left behind

	blocker.resolve();
	await queue.onIdle();

	assert.equal(queue.size, 0);
});

test('aging - clear() keeps the aging strategy', async () => {
	const clock = createClock();
	const queue = new PQueue({concurrency: 1, aging: {period: 10, clock: clock.now}});
	const order: string[] = [];
	const blocker = pDefer<void>();

	queue.add(() => blocker.promise, {priority: 0});
	queue.add(async () => {
		order.push('old');
	}, {priority: 0});
	queue.clear();

	queue.add(async () => {
		order.push('old-again');
	}, {priority: 0});
	clock.advance(30); // old-again reaches effective priority 3
	queue.add(async () => {
		order.push('new');
	}, {priority: 2});

	blocker.resolve();
	await queue.onIdle();

	assert.deepEqual(order, ['old-again', 'new']);
});

test('aging - tasks with the same priority keep FIFO order', async () => {
	const clock = createClock();
	const queue = new PQueue({concurrency: 1, aging: {period: 10, clock: clock.now}});
	const order: string[] = [];
	const blocker = pDefer<void>();

	queue.add(() => blocker.promise, {priority: 0});
	queue.add(async () => {
		order.push('a');
	}, {priority: 0});
	queue.add(async () => {
		order.push('b');
	}, {priority: 0});
	queue.add(async () => {
		order.push('c');
	}, {priority: 0});

	blocker.resolve();
	await queue.onIdle();

	assert.deepEqual(order, ['a', 'b', 'c']);
});
