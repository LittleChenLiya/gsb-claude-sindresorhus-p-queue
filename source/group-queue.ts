import {type Queue, type RunFunction} from './queue.js';
import lowerBound from './lower-bound.js';
import {type QueueAddOptions} from './options.js';

export type GroupQueueOptions = {
	/**
	Name of the group this task belongs to. Tasks in the same group are started FIFO (equal priorities keep insertion order); groups are scheduled against each other by weighted round-robin. Tasks without a group share a default group of weight 1.
	*/
	group?: string;

	/**
	Positive integer weight of the task's group: how many times the group may start a task per round while other groups also have backlog. The last value seen for a group wins. Empty groups never stockpile credit, so a group that recovers from empty gets no burst.

	@default 1
	*/
	groupWeight?: number;
} & QueueAddOptions;

type GroupElement = {
	run: RunFunction;
	priority: number;
	id?: string;
	group?: string;
};

type GroupState = {
	weight: number;
	// Sorted by descending priority; insertion order (FIFO) is kept among equal priorities. This orders the group internally only — it never leaks into a global priority ordering across groups.
	elements: GroupElement[];
};

// Key for tasks added without a `group` option. A symbol cannot collide with any user-chosen group name.
const defaultGroupKey: unique symbol = Symbol('defaultGroup');

/**
Optional `queueClass` for `PQueue` that splits the queue into independent FIFO groups and schedules the groups by weighted round-robin.

- Each group is an independent FIFO (equal priorities keep insertion order; `priority` only reorders within its own group, never across groups).
- A group's positive integer `groupWeight` is the number of tasks it may start per round while other groups have backlog.
- Credits are handed out per turn and never accumulate, so an empty group cannot build up credit and a group recovering from empty produces no burst.
- The same input order always yields the same start order.

@example
```
import PQueue, {GroupQueue} from 'p-queue';

const queue = new PQueue<GroupQueue, GroupQueueOptions>({concurrency: 1, queueClass: GroupQueue});

queue.add(taskA, {group: 'alpha', groupWeight: 2});
queue.add(taskB, {group: 'beta'}); // Weight 1
// Start order while both groups have backlog: alpha, alpha, beta, repeat.
```
*/
export default class GroupQueue implements Queue<RunFunction, GroupQueueOptions> {
	// Map insertion order fixes the rotation order of the groups, which keeps the start sequence deterministic for a given input order.
	readonly #groups = new Map<string | typeof defaultGroupKey, GroupState>();

	// The group whose turn is in progress, and how many starts it still has in this turn.
	#currentKey?: string | typeof defaultGroupKey;
	#credits = 0;

	enqueue(run: RunFunction, options?: Partial<GroupQueueOptions>): void {
		const {
			priority = 0,
			id,
			group,
			groupWeight,
		} = options ?? {};

		if (groupWeight !== undefined && !(Number.isInteger(groupWeight) && groupWeight >= 1)) {
			throw new TypeError(`Expected \`groupWeight\` to be a positive integer, got \`${groupWeight}\` (${typeof groupWeight})`);
		}

		const key = group ?? defaultGroupKey;
		let state = this.#groups.get(key);
		if (!state) {
			state = {weight: 1, elements: []};
			this.#groups.set(key, state);
		}

		if (groupWeight !== undefined) {
			state.weight = groupWeight;
		}

		this.#insert(state, {run, priority, id, group});
	}

	dequeue(): RunFunction | undefined {
		if (this.size === 0) {
			// A fully drained queue restarts the rotation from the first group, keeping the next batch deterministic.
			this.#currentKey = undefined;
			this.#credits = 0;
			return undefined;
		}

		// Each iteration either dequeues or advances to the next group, so this terminates within one full rotation.
		for (;;) {
			const key = this.#currentKey ?? this.#groups.keys().next().value!;
			const state = this.#groups.get(key);

			if (state && state.elements.length > 0) {
				if (this.#credits === 0) {
					// A turn starts with exactly `weight` credits. Leftover credits are discarded when the turn ends, so an empty group never stockpiles credit and a refilled group gets no burst.
					this.#credits = state.weight;
				}

				const element = state.elements.shift()!;
				this.#credits--;

				if (this.#credits === 0 || state.elements.length === 0) {
					this.#advance(key);
				}

				if (this.size === 0) {
					this.#currentKey = undefined;
					this.#credits = 0;
				}

				return element.run;
			}

			// Empty group: skip it without granting any credit.
			this.#advance(key);
		}
	}

	setPriority(id: string, priority: number): void {
		for (const state of this.#groups.values()) {
			const index = state.elements.findIndex(element => element.id === id);
			if (index === -1) {
				continue;
			}

			const [element] = state.elements.splice(index, 1);
			// Re-insert into the same group: priority reorders a group internally, it never moves a task ahead of other groups' turns.
			this.#insert(state, {...element!, priority});
			return;
		}

		throw new ReferenceError(`No promise function with the id "${id}" exists in the queue.`);
	}

	remove(id: string): void;
	remove(run: RunFunction): void;
	remove(idOrRun: string | RunFunction): void {
		for (const state of this.#groups.values()) {
			const index = state.elements.findIndex(element => typeof idOrRun === 'string' ? element.id === idOrRun : element.run === idOrRun);
			if (index !== -1) {
				state.elements.splice(index, 1);
				return;
			}
		}
	}

	filter(options: Readonly<Partial<GroupQueueOptions>>): RunFunction[] {
		const result: RunFunction[] = [];

		for (const state of this.#groups.values()) {
			for (const element of state.elements) {
				if (options.priority !== undefined && element.priority !== options.priority) {
					continue;
				}

				if (options.group !== undefined && element.group !== options.group) {
					continue;
				}

				result.push(element.run);
			}
		}

		return result;
	}

	get size(): number {
		let size = 0;
		for (const state of this.#groups.values()) {
			size += state.elements.length;
		}

		return size;
	}

	#insert(state: GroupState, element: GroupElement): void {
		const {elements} = state;

		if (elements.length === 0 || elements.at(-1)!.priority >= element.priority) {
			// Appending keeps FIFO order among equal priorities.
			elements.push(element);
			return;
		}

		const index = lowerBound(elements, element, (a: GroupElement, b: GroupElement) => b.priority - a.priority);
		elements.splice(index, 0, element);
	}

	#advance(key: string | typeof defaultGroupKey): void {
		const keys = [...this.#groups.keys()];
		const index = keys.indexOf(key);
		this.#currentKey = keys[(index + 1) % keys.length];
		this.#credits = 0;
	}
}
