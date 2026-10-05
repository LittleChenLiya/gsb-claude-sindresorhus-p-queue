import {type Queue, type RunFunction} from './queue.js';
import {type QueueAddOptions} from './options.js';

export type GroupedQueueOptions = {
	/**
	Group the task belongs to. Tasks in the same group are executed in FIFO order, and groups are scheduled by weight in a weighted round-robin.

	@default 'default'
	*/
	group?: string;

	/**
	Number of starts the task's group gets per round. Must be a positive integer. The weight is a property of the group, established when the first task is added; later tasks in the same group use the group's weight.

	@default 1
	*/
	weight?: number;
} & QueueAddOptions;

type GroupState = {
	queue: Array<{
		run: RunFunction;
		priority: number;
		id?: string;
	}>;
	weight: number;
};

/**
A queue that groups tasks by a `group` option and schedules them with a weighted round-robin.

Each group is independently FIFO: tasks within a group always start in insertion order, regardless of priority. Groups take turns, and each group gets up to its `weight` starts per round. Empty groups are simply skipped — they never accumulate credit, so a group that recovers from empty does not burst.

The rotation order is the order in which groups are first seen, which makes the startup sequence deterministic for a given input order.
*/
export default class GroupedQueue implements Queue<RunFunction, GroupedQueueOptions> {
	readonly #groups = new Map<string, GroupState>();

	// Groups in first-seen order, which drives the deterministic rotation.
	readonly #groupOrder: string[] = [];

	// Remaining starts for each group in the current round.
	readonly #remaining = new Map<string, number>();

	// Index into #groupOrder of the group currently being served.
	#cursor = 0;

	#size = 0;

	enqueue(run: RunFunction, options?: Partial<GroupedQueueOptions>): void {
		const {
			group = 'default',
			weight = 1,
			priority = 0,
			id,
		} = options ?? {};

		if (!Number.isInteger(weight) || weight < 1) {
			throw new TypeError(`Expected \`weight\` to be a positive integer, got \`${weight}\` (${typeof weight})`);
		}

		let state = this.#groups.get(group);
		if (!state) {
			state = {queue: [], weight};
			this.#groups.set(group, state);
			this.#groupOrder.push(group);
			this.#remaining.set(group, weight);
		}

		state.queue.push({run, priority, id});
		this.#size++;
	}

	dequeue(): RunFunction | undefined {
		if (this.#size === 0) {
			return undefined;
		}

		for (let i = 0; i < this.#groupOrder.length; i++) {
			const index = (this.#cursor + i) % this.#groupOrder.length;
			const name = this.#groupOrder[index]!;
			const state = this.#groups.get(name)!;

			// Empty groups are skipped; they never accumulate credit.
			if (state.queue.length === 0) {
				continue;
			}

			const remaining = this.#remaining.get(name) ?? 0;
			if (remaining <= 0) {
				continue;
			}

			const item = state.queue.shift()!;
			this.#remaining.set(name, remaining - 1);
			this.#size--;

			// Stay on this group while it still has credit and tasks; otherwise advance to the next group.
			this.#cursor = remaining - 1 === 0 || state.queue.length === 0
				? (index + 1) % this.#groupOrder.length
				: index;

			return item.run;
		}

		// No group could serve — start a fresh round and retry.
		this.#startNewRound();
		return this.dequeue();
	}

	setPriority(id: string, priority: number): void {
		// Locate the task by id across all groups and update its priority in place. Group order is FIFO, so priority never reorders tasks within a group.
		for (const state of this.#groups.values()) {
			const item = state.queue.find(element => element.id === id);
			if (item) {
				item.priority = priority;
				return;
			}
		}

		throw new ReferenceError(`No promise function with the id "${id}" exists in the queue.`);
	}

	remove(id: string): void {
		for (const state of this.#groups.values()) {
			const index = state.queue.findIndex(element => element.id === id);
			if (index !== -1) {
				state.queue.splice(index, 1);
				this.#size--;
				return;
			}
		}
	}

	filter(options: Readonly<Partial<GroupedQueueOptions>>): RunFunction[] {
		const result: RunFunction[] = [];

		for (const [name, state] of this.#groups) {
			if (options.group !== undefined && name !== options.group) {
				continue;
			}

			for (const item of state.queue) {
				// Only constrain by priority when one was given; otherwise every task in the (optionally filtered) group matches.
				if (options.priority === undefined || item.priority === options.priority) {
					result.push(item.run);
				}
			}
		}

		return result;
	}

	get size(): number {
		return this.#size;
	}

	#startNewRound(): void {
		// Reset every group's credit to its weight. Empty groups get credit too, but they have no tasks to spend it on, so a recovering group never bursts.
		for (const [name, state] of this.#groups) {
			this.#remaining.set(name, state.weight);
		}

		this.#cursor = 0;
	}
}
