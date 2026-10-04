import {expectType} from 'tsd';
import PQueue from '../source/index.js';

const queue = new PQueue();

expectType<Promise<string>>(queue.add(async () => '🦄'));

// The aging option accepts a period and an optional virtual clock.
const agingQueue = new PQueue({
	concurrency: 1,
	aging: {
		period: 10,
		clock: () => 0,
	},
});

expectType<Promise<string>>(agingQueue.add(async () => '🦄', {priority: 0}));

// The clock is optional and defaults to Date.now.
new PQueue({aging: {period: 10}});

// @ts-expect-error The aging period must be a number.
new PQueue({aging: {period: '10'}});

// @ts-expect-error The aging clock must be a function.
new PQueue({aging: {period: 10, clock: 0}});
