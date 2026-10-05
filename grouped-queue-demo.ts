import PQueue from './source/index.js';
import GroupedQueue from './source/grouped-queue.js';

// Six-task example: 4 tasks for group A (weight 2) and 2 tasks for group B (weight 1).
// With both groups backlogged, the startup sequence is A,A,B,A,A,B.
const queue = new PQueue({concurrency: 1, queueClass: GroupedQueue});

const order: string[] = [];
const run = (label: string) => async () => {
	order.push(label);
	console.log(`start: ${label}`);
};

// Add all six tasks at once. Group identity is passed explicitly via `group` — it is never guessed from the function name.
queue.add(run('A0'), {group: 'A', weight: 2});
queue.add(run('A1'), {group: 'A', weight: 2});
queue.add(run('B0'), {group: 'B', weight: 1});
queue.add(run('A2'), {group: 'A', weight: 2});
queue.add(run('A3'), {group: 'A', weight: 2});
queue.add(run('B1'), {group: 'B', weight: 1});

await queue.onIdle();

console.log('---');
console.log('startup sequence:', order.join(', '));
console.log('expected:        A0, A1, B0, A2, A3, B1');
console.log('match:', JSON.stringify(order) === JSON.stringify(['A0', 'A1', 'B0', 'A2', 'A3', 'B1']));
