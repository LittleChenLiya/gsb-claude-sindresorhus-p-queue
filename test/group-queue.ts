import {test} from 'node:test';
import assert from 'node:assert/strict';
import PQueue from '../source/index.js';
import GroupQueue, {type GroupQueueOptions} from '../source/group-queue.js';

const createQueue = () => new PQueue<GroupQueue, GroupQueueOptions>({concurrency: 1, autoStart: false, queueClass: GroupQueue});

// Records the start order of named tasks.
const tracker = () => {
	const started: string[] = [];
	const task = (name: string) => async () => {
		started.push(name);
		return name;
	};

	return {started, task};
};

test('GroupQueue: weight 2 vs weight 1 backlog starts A,A,B in a cycle', async () => {
	const queue = createQueue();
	const {started, task} = tracker();

	// Function names carry no group information; grouping comes only from the group option.
	const adds = [
		queue.add(task('task-1'), {group: 'alpha', groupWeight: 2}),
		queue.add(task('task-2'), {group: 'beta'}),
		queue.add(task('task-3'), {group: 'alpha'}),
		queue.add(task('task-4'), {group: 'beta'}),
		queue.add(task('task-5'), {group: 'alpha'}),
		queue.add(task('task-6'), {group: 'alpha'}),
	];

	queue.start();
	await Promise.all(adds);
	await queue.onIdle();

	assert.deepEqual(started, ['task-1', 'task-3', 'task-2', 'task-5', 'task-6', 'task-4']);
});

test('GroupQueue: same input order yields the same start sequence', async () => {
	const run = async () => {
		const queue = createQueue();
		const {started, task} = tracker();

		const adds = [
			queue.add(task('task-1'), {group: 'alpha', groupWeight: 2}),
			queue.add(task('task-2'), {group: 'beta'}),
			queue.add(task('task-3'), {group: 'alpha'}),
			queue.add(task('task-4'), {group: 'beta'}),
			queue.add(task('task-5'), {group: 'alpha'}),
			queue.add(task('task-6'), {group: 'alpha'}),
		];

		queue.start();
		await Promise.all(adds);
		return started;
	};

	assert.deepEqual(await run(), await run());
});

test('GroupQueue: cancelling a queued task leaves no size residue', async () => {
	const queue = createQueue();
	const {started, task} = tracker();
	const controller = new AbortController();

	const adds = [
		queue.add(task('task-1'), {group: 'alpha', groupWeight: 2}),
		queue.add(task('task-2'), {group: 'beta', id: 'cancel-me', signal: controller.signal}),
		queue.add(task('task-3'), {group: 'alpha'}),
		queue.add(task('task-4'), {group: 'beta'}),
	];

	assert.equal(queue.size, 4);
	controller.abort();
	await assert.rejects(adds[1]!, (error: unknown) => (error as {name?: string}).name === 'AbortError');

	// The cancelled task is gone from its group: no residue in size, per-group size, or the start sequence.
	assert.equal(queue.size, 3);
	assert.equal(queue.sizeBy({group: 'beta'}), 1);
	assert.equal(queue.sizeBy({group: 'alpha'}), 2);

	queue.start();
	await Promise.all([adds[0], adds[2], adds[3]]);
	await queue.onIdle();

	assert.deepEqual(started, ['task-1', 'task-3', 'task-4']);
	assert.equal(queue.size, 0);
});

test('GroupQueue: an empty group builds no credit and its recovery causes no burst', async () => {
	const queue = createQueue();
	const {started, task} = tracker();
	const adds: Array<Promise<unknown>> = [];

	// Beta-2 refills alpha from inside the task, so the refills land deterministically
	// while alpha has been empty for several starts.
	const refill = () => {
		adds.push(
			queue.add(task('alpha-2'), {group: 'alpha'}),
			queue.add(task('alpha-3'), {group: 'alpha'}),
			queue.add(task('alpha-4'), {group: 'alpha'}),
			queue.add(task('alpha-5'), {group: 'alpha'}),
		);
	};

	adds.push(
		queue.add(task('alpha-1'), {group: 'alpha', groupWeight: 2}),
		queue.add(task('beta-1'), {group: 'beta'}),
		queue.add(async () => {
			started.push('beta-2');
			refill();
		}, {group: 'beta'}),
		queue.add(task('beta-3'), {group: 'beta'}),
		queue.add(task('beta-4'), {group: 'beta'}),
	);

	// Alpha drains after one start, then sits empty while beta runs. When alpha
	// gets new work it must not burst: it still gets exactly 2 starts per round.
	queue.start();
	await Promise.all(adds);
	await queue.onIdle();

	// Alpha never starts more than its weight (2) in a row, even right after recovering from empty.
	assert.deepEqual(started, [
		'alpha-1',
		'beta-1',
		'beta-2',
		'alpha-2', 'alpha-3',
		'beta-3',
		'alpha-4', 'alpha-5',
		'beta-4',
	]);
});

test('GroupQueue: setPriority finds the task in its group and never leaks across groups', async () => {
	const queue = createQueue();
	const {started, task} = tracker();

	const adds = [
		queue.add(task('task-1'), {group: 'alpha', id: 'a1'}),
		queue.add(task('task-2'), {group: 'beta', id: 'b1'}),
		queue.add(task('task-3'), {group: 'alpha', id: 'a2'}),
		queue.add(task('task-4'), {group: 'beta', id: 'b2'}),
	];

	// Promote b2 within group beta. It must overtake b1, but must not jump ahead of the alpha turns.
	queue.setPriority('b2', 100);

	queue.start();
	await Promise.all(adds);

	assert.deepEqual(started, ['task-1', 'task-4', 'task-3', 'task-2']);
	assert.throws(() => queue.setPriority('missing', 1), ReferenceError);
});

test('GroupQueue: priority reorders within a group while equal priorities stay FIFO', async () => {
	const queue = createQueue();
	const {started, task} = tracker();

	const adds = [
		queue.add(task('task-1'), {group: 'alpha', id: 'a1'}),
		queue.add(task('task-2'), {group: 'alpha', id: 'a2'}),
		queue.add(task('task-3'), {group: 'alpha', id: 'a3'}),
	];

	// Promote a3 inside group alpha: it goes first, and the remaining equal-priority tasks keep FIFO order.
	queue.setPriority('a3', 1);

	queue.start();
	await Promise.all(adds);

	assert.deepEqual(started, ['task-3', 'task-1', 'task-2']);
});

test('GroupQueue: rejects a non-positive-integer groupWeight', () => {
	const queue = new GroupQueue();
	const run = async () => undefined;

	assert.throws(() => queue.enqueue(run, {group: 'alpha', groupWeight: 0}), TypeError);
	assert.throws(() => queue.enqueue(run, {group: 'alpha', groupWeight: 1.5}), TypeError);
	assert.throws(() => queue.enqueue(run, {group: 'alpha', groupWeight: -2}), TypeError);
	assert.equal(queue.size, 0);

	queue.enqueue(run, {group: 'alpha', groupWeight: 3});
	assert.equal(queue.size, 1);
});

test('GroupQueue: remove locates a task by id or run across groups', async () => {
	const queue = new GroupQueue();
	const first = async () => 'first';
	const second = async () => 'second';
	const third = async () => 'third';

	queue.enqueue(first, {group: 'alpha', id: 'a1'});
	queue.enqueue(second, {group: 'beta', id: 'b1'});
	queue.enqueue(third, {group: 'alpha', id: 'a2'});

	queue.remove('b1');
	queue.remove(third);
	assert.equal(queue.size, 1);
	assert.equal(queue.dequeue(), first);
	assert.equal(queue.dequeue(), undefined);
});

test('GroupQueue: tasks without a group share a default group of weight 1', async () => {
	const queue = createQueue();
	const {started, task} = tracker();

	const adds = [
		queue.add(task('task-1'), {group: 'alpha', groupWeight: 2}),
		queue.add(task('task-2')),
		queue.add(task('task-3'), {group: 'alpha'}),
		queue.add(task('task-4')),
	];

	queue.start();
	await Promise.all(adds);

	// Alpha (weight 2) alternates with the default group (weight 1).
	assert.deepEqual(started, ['task-1', 'task-3', 'task-2', 'task-4']);
});
