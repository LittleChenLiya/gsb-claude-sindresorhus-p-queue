import {expectType} from 'tsd';
import PQueue from '../source/index.js';

const queue = new PQueue();

expectType<Promise<string>>(queue.add(async () => '🦄'));

// Timeout slot policy
const strictQueue = new PQueue({concurrency: 1, timeout: 100, timeoutSlotPolicy: 'strict'});
expectType<{readonly waiting: number; readonly running: number; readonly timedOut: number}>(strictQueue.stats);
expectType<Promise<string>>(strictQueue.add(async () => '🦄', {timeoutSlotPolicy: 'release'}));
expectType<Promise<string>>(strictQueue.add(async () => '🦄', {timeoutSlotPolicy: 'strict', timeout: 1000}));
