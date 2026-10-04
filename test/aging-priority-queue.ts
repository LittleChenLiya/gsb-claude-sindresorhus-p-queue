import {test} from 'node:test';
import assert from 'node:assert/strict';
import PQueue from '../source/index.js';
import AgingPriorityQueue from '../source/aging-priority-queue.js';

const createRun = (value: string) => async () => value;

// Virtual clock fixture: tests advance `clock.now` manually, so aging is deterministic.
const createVirtualClock = (start = 0) => {
	const clock = {
		now: start,
		advance(milliseconds: number) {
			clock.now += milliseconds;
		},
		read() {
			return clock.now;
		},
	};

	return clock;
};

test('AgingPriorityQueue ages waiting tasks past newer higher-priority tasks', () => {
	const clock = createVirtualClock();
	const queue = new AgingPriorityQueue({agingPeriod: 10, now: clock.read});

	const low = createRun('low');
	const high = createRun('high');

	queue.enqueue(low, {priority: 0});

	// Wait 30ms with a 10ms period: effective priority becomes 0 + 3 = 3.
	clock.advance(30);

	queue.enqueue(high, {priority: 2});

	// Effective 3 beats the freshly enqueued priority 2.
	assert.equal(queue.dequeue(), low);
	assert.equal(queue.dequeue(), high);
	assert.equal(queue.dequeue(), undefined);
});

test('AgingPriorityQueue breaks effective-priority ties by enqueue order', () => {
	const clock = createVirtualClock();
	const queue = new AgingPriorityQueue({agingPeriod: 10, now: clock.read});

	const first = createRun('first');
	const second = createRun('second');
	const third = createRun('third');

	queue.enqueue(first, {priority: 0});
	clock.advance(10);
	// Same effective priority as `first` (0 + 1 === 1 + 0) once enqueued.
	queue.enqueue(second, {priority: 1});
	queue.enqueue(third, {priority: 1});

	assert.equal(queue.dequeue(), first);
	assert.equal(queue.dequeue(), second);
	assert.equal(queue.dequeue(), third);
	assert.equal(queue.dequeue(), undefined);
});

test('AgingPriorityQueue setPriority keeps the accumulated wait time', () => {
	const clock = createVirtualClock();
	const queue = new AgingPriorityQueue({agingPeriod: 10, now: clock.read});

	const boosted = createRun('boosted');
	const other = createRun('other');

	queue.enqueue(boosted, {priority: 0, id: 'boosted'});

	clock.advance(30);

	// Enqueued after the wait, so it has no aging bonus: effective 3.
	queue.enqueue(other, {priority: 3});

	// New baseline 1, but the 30ms wait is preserved: effective 1 + 3 = 4 > 3.
	// If setPriority reset the wait time, the effective priority would be 1 and lose.
	queue.setPriority('boosted', 1);

	assert.equal(queue.dequeue(), boosted);
	assert.equal(queue.dequeue(), other);
});

test('AgingPriorityQueue removes cancelled tasks immediately without placeholders', () => {
	const clock = createVirtualClock();
	const queue = new AgingPriorityQueue({agingPeriod: 10, now: clock.read});

	const first = createRun('first');
	const cancelled = createRun('cancelled');
	const third = createRun('third');

	queue.enqueue(first, {id: 'first'});
	queue.enqueue(cancelled, {id: 'cancelled'});
	queue.enqueue(third, {id: 'third'});

	queue.remove('cancelled');
	assert.equal(queue.size, 2);

	queue.remove(first);
	assert.equal(queue.size, 1);

	assert.equal(queue.dequeue(), third);
	assert.equal(queue.dequeue(), undefined);
	assert.equal(queue.size, 0);
});

test('AgingPriorityQueue clamps negative elapsed time when the clock rolls back', () => {
	const clock = createVirtualClock(1000);
	const queue = new AgingPriorityQueue({agingPeriod: 10, now: clock.read});

	const task = createRun('task');
	queue.enqueue(task, {priority: 0});

	// Clock rollback: waiting time must not become negative or gain extra priority.
	clock.now = 500;

	const other = createRun('other');
	queue.enqueue(other, {priority: 0});

	// Both have effective priority 0, so enqueue order decides.
	assert.equal(queue.dequeue(), task);
	assert.equal(queue.dequeue(), other);
});

test('AgingPriorityQueue validates agingPeriod', () => {
	assert.throws(() => {
		// eslint-disable-next-line no-new
		new AgingPriorityQueue({agingPeriod: 0});
	}, {name: 'TypeError'});

	assert.throws(() => {
		// eslint-disable-next-line no-new
		new AgingPriorityQueue({agingPeriod: Number.POSITIVE_INFINITY});
	}, {name: 'TypeError'});
});

test('PQueue runs aged low-priority task before newer priority-2 task', async () => {
	const clock = createVirtualClock();

	class VirtualClockAgingQueue extends AgingPriorityQueue {
		constructor() {
			super({agingPeriod: 10, now: clock.read});
		}
	}

	const queue = new PQueue({concurrency: 1, autoStart: false, queueClass: VirtualClockAgingQueue});
	const order: string[] = [];

	// Do not await these before `start()`: the promises only settle once tasks run.
	const lowPromise = queue.add(async () => {
		order.push('low');
	}, {priority: 0});

	clock.advance(30);

	const highPromise = queue.add(async () => {
		order.push('high');
	}, {priority: 2});

	queue.start();
	await Promise.all([lowPromise, highPromise]);
	await queue.onIdle();

	assert.deepEqual(order, ['low', 'high']);
});

test('PQueue abort removes the queued task from an aging queue without size residue', async () => {
	class DefaultAgingQueue extends AgingPriorityQueue {}

	const queue = new PQueue({concurrency: 1, autoStart: false, queueClass: DefaultAgingQueue});
	const controller = new AbortController();

	const promise = queue.add(async () => 'never', {signal: controller.signal});
	assert.equal(queue.size, 1);

	controller.abort();
	await assert.rejects(promise);
	assert.equal(queue.size, 0);
});
