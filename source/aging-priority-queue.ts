import {type Queue, type RunFunction} from './queue.js';
import {type QueueAddOptions} from './options.js';

export type AgingPriorityQueueOptions = {
	/**
	The number of milliseconds a task must wait to gain `+1` effective priority. Must be a positive finite number.

	For example, with `agingPeriod: 10`, a task that has waited 30ms gets `+3` effective priority on top of its base priority.

	@default 1000
	*/
	readonly agingPeriod?: number;

	/**
	Injectable clock returning the current time in milliseconds. Defaults to `Date.now`.

	Inject a virtual clock in tests to make aging deterministic. If the clock ever moves backwards, the elapsed wait time is clamped to zero, so a clock rollback can never decrease (or inflate) a task's effective priority.
	*/
	readonly now?: () => number;
};

type AgingQueueElement = {
	run: RunFunction;
	id?: string;
	basePriority: number;
	enqueuedAt: number;
	sequence: number;
};

/**
A priority queue with wait-time aging: long-waiting low-priority tasks gain effective priority over time so they are not starved by newer high-priority tasks.

- The `priority` passed to `enqueue` is only a baseline. The effective priority is `basePriority + floor(waitedMs / agingPeriod)`.
- Aging is computed lazily at dequeue time only. No timers or polling are started.
- Ties in effective priority are broken by original enqueue order (stable FIFO).
- `setPriority` updates the baseline without resetting the accumulated wait time.
- `remove` (used for cancellation) splices the item out immediately, so `size` never counts placeholders.
*/
export default class AgingPriorityQueue implements Queue<RunFunction, QueueAddOptions> {
	readonly #queue: AgingQueueElement[] = [];

	readonly #agingPeriod: number;

	readonly #now: () => number;

	// Monotonic counter used to break effective-priority ties by enqueue order.
	#sequence = 0;

	constructor(options: AgingPriorityQueueOptions = {}) {
		const {
			agingPeriod = 1000,
			now = Date.now,
		} = options;

		if (!(Number.isFinite(agingPeriod) && agingPeriod > 0)) {
			throw new TypeError(`Expected \`agingPeriod\` to be a positive finite number, got \`${agingPeriod}\` (${typeof agingPeriod})`);
		}

		this.#agingPeriod = agingPeriod;
		this.#now = now;
	}

	#effectivePriority(element: Readonly<AgingQueueElement>, now: number): number {
		// Clock rollback strategy: clamp negative elapsed time to zero so a
		// backwards-moving clock can never decrease a task's effective priority.
		const waited = Math.max(0, now - element.enqueuedAt);
		return element.basePriority + Math.floor(waited / this.#agingPeriod);
	}

	enqueue(run: RunFunction, options?: Readonly<Partial<QueueAddOptions>>): void {
		const {
			priority = 0,
			id,
		} = options ?? {};

		this.#queue.push({
			run,
			id,
			basePriority: priority,
			enqueuedAt: this.#now(),
			sequence: this.#sequence++,
		});
	}

	dequeue(): RunFunction | undefined {
		if (this.#queue.length === 0) {
			return undefined;
		}

		// Aging is evaluated only here, at dequeue time. The queue stays in
		// insertion order and the best candidate is picked by a single scan.
		const now = this.#now();
		let bestIndex = 0;
		let bestEffective = this.#effectivePriority(this.#queue[0]!, now);

		for (let index = 1; index < this.#queue.length; index++) {
			const effective = this.#effectivePriority(this.#queue[index]!, now);
			// Strictly-greater wins, so equal effective priorities keep the
			// earlier-enqueued (lower sequence) item: stable FIFO on ties.
			if (effective > bestEffective) {
				bestEffective = effective;
				bestIndex = index;
			}
		}

		const [item] = this.#queue.splice(bestIndex, 1);
		return item!.run;
	}

	setPriority(id: string, priority: number): void {
		const element = this.#queue.find(element => element.id === id);
		if (!element) {
			throw new ReferenceError(`No promise function with the id "${id}" exists in the queue.`);
		}

		// Only the baseline changes. `enqueuedAt` and `sequence` are preserved,
		// so the wait time accumulated so far keeps counting towards aging.
		element.basePriority = priority;
	}

	remove(id: string): void;
	remove(run: RunFunction): void;
	remove(idOrRun: string | RunFunction): void {
		const index = this.#queue.findIndex(element => typeof idOrRun === 'string' ? element.id === idOrRun : element.run === idOrRun);

		if (index !== -1) {
			// Remove immediately so cancelled tasks never linger as placeholders.
			this.#queue.splice(index, 1);
		}
	}

	filter(options: Readonly<Partial<QueueAddOptions>>): RunFunction[] {
		const result: RunFunction[] = [];

		for (const element of this.#queue) {
			if (element.basePriority === options.priority) {
				result.push(element.run);
			}
		}

		return result;
	}

	get size(): number {
		return this.#queue.length;
	}
}
