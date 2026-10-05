/*
Six-task demo of the optional grouping QueueClass (`GroupQueue`).

Run it locally with Node (from the repository root):

	node --import=tsx/esm examples/group-queue.ts

Expected output:

	Start order: task-1 -> task-3 -> task-2 -> task-5 -> task-6 -> task-4

Group `alpha` has weight 2 and group `beta` has weight 1, so while both have
backlog the starts cycle A,A,B. Note that nothing in the task function names
betrays which group a task belongs to — group identity comes only from the
`group` option passed to `add()`.
*/
import PQueue, {GroupQueue, type GroupQueueOptions} from '../source/index.js';

const queue = new PQueue<GroupQueue, GroupQueueOptions>({
	concurrency: 1,
	autoStart: false,
	queueClass: GroupQueue,
});

const started: string[] = [];
const task = (name: string) => async () => {
	started.push(name);
	return name;
};

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

console.log('Start order:', started.join(' -> '));
