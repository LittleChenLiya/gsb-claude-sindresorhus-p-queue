import {type Queue, type RunFunction} from './queue.js';
import lowerBound from './lower-bound.js';
import {type QueueAddOptions, type AgingOptions} from './options.js';

const compactionThreshold = 100;

export type PriorityQueueOptions = {
	priority?: number;
} & QueueAddOptions;

type AgingElement = PriorityQueueOptions & {
	run: RunFunction;
	// Clock reading at enqueue time. The wait clock keeps running even when the priority is updated with `setPriority()`.
	enqueuedAt: number;
	// Monotonic enqueue sequence, used as a stable tie-breaker so equal effective priorities dequeue in enqueue order.
	sequence: number;
};

export default class PriorityQueue implements Queue<RunFunction, PriorityQueueOptions> {
	readonly #queue: Array<PriorityQueueOptions & {run: RunFunction}> = [];

	// The queue is stored as a sorted array, but dequeued items are left before `#head` until compaction. Only items from `#head` onward are live, which keeps repeated dequeues amortized O(1).
	#head = 0;

	// Aging is opt-in. When disabled, the queue behaves exactly as a sorted priority queue.
	readonly #aging: {
		readonly period: number;
		readonly clock: () => number;
	} | undefined;

	// Live items only, kept in enqueue order. Effective priorities change as the clock advances, so they are recomputed at dequeue time instead of maintaining a sorted order.
	readonly #agingQueue: AgingElement[] = [];

	#sequence = 0;

	constructor(options?: {readonly aging?: AgingOptions}) {
		if (options?.aging === undefined) {
			return;
		}

		const {period, clock = () => Date.now()} = options.aging;

		if (!(typeof period === 'number' && Number.isFinite(period) && period > 0)) {
			throw new TypeError(`Expected \`aging.period\` to be a finite number greater than 0, got \`${period}\` (${typeof period})`);
		}

		if (typeof clock !== 'function') {
			throw new TypeError(`Expected \`aging.clock\` to be a function, got \`${typeof clock}\``);
		}

		this.#aging = {period, clock};
	}

	enqueue(run: RunFunction, options?: Partial<PriorityQueueOptions>): void {
		if (this.#aging) {
			const {
				priority = 0,
				id,
			} = options ?? {};

			this.#agingQueue.push({
				priority,
				id,
				run,
				enqueuedAt: this.#aging.clock(),
				sequence: this.#sequence++,
			});
			return;
		}

		const {
			priority = 0,
			id,
		} = options ?? {};

		const {size} = this;
		const element = {
			priority,
			id,
			run,
		};

		if (size === 0) {
			// When the queue is logically empty, discard any consumed prefix before accepting new work.
			this.#queue.length = 0;
			this.#head = 0;
			this.#queue.push(element);
			return;
		}

		if (this.#queue.at(-1)!.priority! >= priority) {
			// Same-priority and lower-priority items belong after the current tail. Appending preserves FIFO order for equal priorities.
			this.#queue.push(element);
			return;
		}

		// Binary insertion must run on the live sorted range only.
		this.#compact();
		const index = lowerBound(this.#queue, element, (a: Readonly<PriorityQueueOptions>, b: Readonly<PriorityQueueOptions>) => b.priority! - a.priority!);
		this.#queue.splice(index, 0, element);
	}

	setPriority(id: string, priority: number) {
		if (this.#aging) {
			const element = this.#agingQueue.find((element: AgingElement) => element.id === id);
			if (element === undefined) {
				throw new ReferenceError(`No promise function with the id "${id}" exists in the queue.`);
			}

			// The wait clock keeps running: updating the baseline priority must not reset `enqueuedAt`.
			element.priority = priority;
			return;
		}

		// A dequeued item with the same id is no longer part of the queue.
		const index = this.#queue.findIndex((element: Readonly<PriorityQueueOptions>, index) => index >= this.#head && element.id === id);
		if (index === -1) {
			throw new ReferenceError(`No promise function with the id "${id}" exists in the queue.`);
		}

		const [item] = this.#queue.splice(index, 1);
		this.enqueue(item!.run, {priority, id});
	}

	remove(id: string): void;
	remove(run: RunFunction): void;
	remove(idOrRun: string | RunFunction): void {
		if (this.#aging) {
			const index = this.#agingQueue.findIndex((element: AgingElement) => {
				if (typeof idOrRun === 'string') {
					return element.id === idOrRun;
				}

				return element.run === idOrRun;
			});

			// Removed items are spliced out immediately so they never linger as placeholders.
			if (index !== -1) {
				this.#agingQueue.splice(index, 1);
			}

			return;
		}

		const index = this.#queue.findIndex((element: Readonly<PriorityQueueOptions & {run: RunFunction}>, index) => {
			// The consumed prefix may still contain references that should not be removable.
			if (index < this.#head) {
				return false;
			}

			if (typeof idOrRun === 'string') {
				return element.id === idOrRun;
			}

			return element.run === idOrRun;
		});

		if (index !== -1) {
			this.#queue.splice(index, 1);
		}
	}

	dequeue(): RunFunction | undefined {
		if (this.#aging) {
			if (this.#agingQueue.length === 0) {
				return undefined;
			}

			// Aging is computed lazily at dequeue time; no polling timers are involved.
			const now = this.#aging.clock();
			let bestIndex = 0;
			let bestEffectivePriority = this.#effectivePriority(this.#agingQueue[0]!, now);

			for (let index = 1; index < this.#agingQueue.length; index++) {
				const element = this.#agingQueue[index]!;
				const effectivePriority = this.#effectivePriority(element, now);

				// Higher effective priority wins; ties keep the earlier enqueue order.
				if (
					effectivePriority > bestEffectivePriority
					|| (effectivePriority === bestEffectivePriority && element.sequence < this.#agingQueue[bestIndex]!.sequence)
				) {
					bestIndex = index;
					bestEffectivePriority = effectivePriority;
				}
			}

			const [item] = this.#agingQueue.splice(bestIndex, 1);
			return item?.run;
		}

		if (this.#head === this.#queue.length) {
			return undefined;
		}

		const item = this.#queue[this.#head];
		this.#head++;

		if (this.#head === this.#queue.length) {
			// Fully drained queues are reset immediately so the next enqueue starts from a clean array.
			this.#queue.length = 0;
			this.#head = 0;
		} else if (this.#head > compactionThreshold && this.#head > this.#queue.length / 2) {
			// Keep repeated dequeues cheap, but stop the consumed prefix from growing without bound.
			this.#compact();
		}

		return item?.run;
	}

	filter(options: Readonly<Partial<PriorityQueueOptions>>): RunFunction[] {
		if (this.#aging) {
			const result: RunFunction[] = [];

			for (const element of this.#agingQueue) {
				if (element.priority === options.priority) {
					result.push(element.run);
				}
			}

			return result;
		}

		const result: RunFunction[] = [];

		for (let index = this.#head; index < this.#queue.length; index++) {
			const element = this.#queue[index]!;
			if (element.priority === options.priority) {
				result.push(element.run);
			}
		}

		return result;
	}

	get size(): number {
		return this.#aging ? this.#agingQueue.length : this.#queue.length - this.#head;
	}

	#effectivePriority(element: AgingElement, now: number): number {
		// A clock that moves backwards never lowers the effective priority below the baseline.
		const waited = Math.max(0, now - element.enqueuedAt);
		return element.priority! + Math.floor(waited / this.#aging!.period);
	}

	#compact(): void {
		// Compaction restores the invariant that the whole array is the live sorted range.
		if (this.#head === 0) {
			return;
		}

		this.#queue.splice(0, this.#head);
		this.#head = 0;
	}
}
