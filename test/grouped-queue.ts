import {test} from 'node:test';
import assert from 'node:assert/strict';
import delay from 'delay';
import PQueue from '../source/index.js';
import GroupedQueue from '../source/grouped-queue.js';

const createRun = (label: string, order: string[]) => async () => {
	order.push(label);
};

test('GroupedQueue - weighted round-robin gives A,A,B cycle when both groups backlogged', async () => {
	const order: string[] = [];
	const queue = new PQueue({concurrency: 1, queueClass: GroupedQueue});

	// Add a backlog of A (weight 2) and B (weight 1) tasks all at once.
	for (let index = 0; index < 6; index++) {
		queue.add(createRun(`A${index}`, order), {group: 'A', weight: 2});
	}

	for (let index = 0; index < 6; index++) {
		queue.add(createRun(`B${index}`, order), {group: 'B', weight: 1});
	}

	await queue.onIdle();

	// Each round: A starts twice, then B starts once.
	assert.deepEqual(order, [
		'A0', 'A1', 'B0',
		'A2', 'A3', 'B1',
		'A4', 'A5', 'B2',
		'B3', 'B4', 'B5',
	]);
});

test('GroupedQueue - empty group recovery does not burst credit', async () => {
	const order: string[] = [];
	const queue = new PQueue({concurrency: 1, queueClass: GroupedQueue});

	// Drain B first so it is empty while A keeps running.
	queue.add(createRun('B0', order), {group: 'B', weight: 1});
	queue.add(createRun('A0', order), {group: 'A', weight: 2});
	queue.add(createRun('A1', order), {group: 'A', weight: 2});

	await queue.onIdle();
	assert.deepEqual(order, ['B0', 'A0', 'A1']);

	// B was empty for several rounds. When it returns it must not burst: it gets at most one start per round.
	queue.add(createRun('A2', order), {group: 'A', weight: 2});
	queue.add(createRun('A3', order), {group: 'A', weight: 2});
	queue.add(createRun('B1', order), {group: 'B', weight: 1});
	queue.add(createRun('B2', order), {group: 'B', weight: 1});

	await queue.onIdle();

	// B gets at most one start per round — no burst of credit. A's two starts are split by B's single start because A3 was enqueued after A2 began.
	assert.deepEqual(order, [
		'B0', 'A0', 'A1',
		'A2', 'B1', 'A3',
		'B2',
	]);
});

test('GroupedQueue - cancellation removes the task and size has no residue', async () => {
	const queue = new PQueue({concurrency: 1, queueClass: GroupedQueue});

	const controller = new AbortController();

	queue.add(async () => delay(1000), {group: 'A', id: 'running'});
	queue.add(async () => 'a1', {group: 'A', id: 'a1'});
	queue.add(async () => 'b1', {group: 'B', id: 'b1'});
	const aborted = queue.add(async () => 'a2', {group: 'A', id: 'a2', signal: controller.signal});

	assert.equal(queue.size, 3);

	// Cancel the queued task.
	controller.abort();

	assert.equal(queue.size, 2, 'size should drop by one after cancellation');

	await assert.rejects(aborted, {name: 'AbortError'});

	await queue.onIdle();

	assert.equal(queue.size, 0, 'size should be zero after all tasks complete');
});

test('GroupedQueue - setPriority locates the original task and does not reorder within a group', async () => {
	const order: string[] = [];
	const queue = new PQueue({concurrency: 1, queueClass: GroupedQueue});

	queue.add(createRun('A0', order), {group: 'A', id: 'a0', priority: 0});
	queue.add(createRun('A1', order), {group: 'A', id: 'a1', priority: 0});
	queue.add(createRun('A2', order), {group: 'A', id: 'a2', priority: 0});

	// Raising a1's priority must not move it ahead of a0 within the FIFO group.
	queue.setPriority('a1', 100);

	await queue.onIdle();

	assert.deepEqual(order, ['A0', 'A1', 'A2']);
});

test('GroupedQueue - tasks within a group stay FIFO regardless of priority', async () => {
	const order: string[] = [];
	const queue = new PQueue({concurrency: 1, queueClass: GroupedQueue});

	queue.add(createRun('low', order), {group: 'A', priority: 0});
	queue.add(createRun('high', order), {group: 'A', priority: 100});
	queue.add(createRun('mid', order), {group: 'A', priority: 50});

	await queue.onIdle();

	// FIFO within the group, not priority-sorted.
	assert.deepEqual(order, ['low', 'high', 'mid']);
});

test('GroupedQueue - same input order gives a deterministic startup sequence', async () => {
	const run = async () => {
		const order: string[] = [];
		const queue = new PQueue({concurrency: 1, queueClass: GroupedQueue});

		for (const group of ['A', 'B', 'A', 'B', 'A', 'B']) {
			queue.add(createRun(group, order), {group, weight: group === 'A' ? 2 : 1});
		}

		await queue.onIdle();
		return order;
	};

	const first = await run();
	const second = await run();

	assert.deepEqual(first, second);
});

test('GroupedQueue - default group when group is omitted', async () => {
	const order: string[] = [];
	const queue = new PQueue({concurrency: 1, queueClass: GroupedQueue});

	queue.add(createRun('one', order));
	queue.add(createRun('two', order));

	await queue.onIdle();

	assert.deepEqual(order, ['one', 'two']);
});

test('GroupedQueue - weight must be a positive integer', async () => {
	const queue = new PQueue({concurrency: 1, queueClass: GroupedQueue});

	await assert.rejects(
		queue.add(async () => undefined, {group: 'A', weight: 0}),
		{message: /positive integer/},
	);

	await assert.rejects(
		queue.add(async () => undefined, {group: 'A', weight: 2.5}),
		{message: /positive integer/},
	);

	await assert.rejects(
		queue.add(async () => undefined, {group: 'A', weight: -1}),
		{message: /positive integer/},
	);
});

test('GroupedQueue - sizeBy filters by group and priority', async () => {
	const queue = new PQueue({concurrency: 1, queueClass: GroupedQueue, autoStart: false});

	queue.add(async () => undefined, {group: 'A', priority: 1});
	queue.add(async () => undefined, {group: 'A', priority: 0});
	queue.add(async () => undefined, {group: 'B', priority: 1});

	assert.equal(queue.sizeBy({group: 'A'}), 2);
	assert.equal(queue.sizeBy({group: 'B'}), 1);
	assert.equal(queue.sizeBy({priority: 1}), 2);
	assert.equal(queue.sizeBy({group: 'A', priority: 1}), 1);
});

test('GroupedQueue - clear resets groups', async () => {
	const queue = new PQueue({concurrency: 1, queueClass: GroupedQueue, autoStart: false});

	queue.add(async () => undefined, {group: 'A'});
	queue.add(async () => undefined, {group: 'B'});

	assert.equal(queue.size, 2);

	queue.clear();

	assert.equal(queue.size, 0);
});
