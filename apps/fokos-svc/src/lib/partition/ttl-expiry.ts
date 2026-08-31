import type { PartitionStore } from "./partition-store.js";

export type TtlSweepConfig = {
	/** Maximum rows deleted by one storage transaction. */
	chunkSize: number;
	/** Delay after a row or byte budget is reached. */
	sleepMs: number;
	/** Deleted rows between two sleep delays. */
	maxRowsBeforeSleep: number;
	/** Estimated deleted bytes between two sleep delays. */
	maxBytesBeforeSleep: number;
	/** Maximum rows deleted by one sweep cycle. */
	maxRowsPerCycle: number;
	/** Delay before the first sweep after a partition wakes. */
	initialDelayMs: number;
};

export type TtlExpiryOptions = {
	store: PartitionStore;
	/** False before context load, during migration, and in split_started or split_completed. */
	canSweep: () => boolean;
	logParams: () => Record<string, unknown>;
	/** Read when work starts so subclasses can override the values. */
	config: () => TtlSweepConfig;
	/** Returns the current epoch time in seconds. */
	nowSec?: () => number;
	wait?: (ms: number) => Promise<void>;
};

export type TtlSweepResult = { deletedRows: number; deletedBytes: number; more: boolean };

export class TtlExpiry {
	#timer: ReturnType<typeof setTimeout> | null = null;
	#running = false;
	readonly #store: PartitionStore;
	readonly #canSweep: () => boolean;
	readonly #logParams: () => Record<string, unknown>;
	readonly #config: () => TtlSweepConfig;
	readonly #nowSec: () => number;
	readonly #wait: (ms: number) => Promise<void>;

	constructor(deps: TtlExpiryOptions) {
		this.#store = deps.store;
		this.#canSweep = deps.canSweep;
		this.#logParams = deps.logParams;
		this.#config = deps.config;
		this.#nowSec = deps.nowSec ?? (() => Math.floor(Date.now() / 1000));
		this.#wait = deps.wait ?? (async (ms) => await scheduler.wait(ms));
	}

	get armed(): boolean {
		return this.#timer !== null;
	}

	arm(delayMs?: number): void {
		if (this.#timer !== null) return;
		const delay = delayMs ?? 500;
		this.#timer = setTimeout(() => {
			this.#timer = null;
			void this.runCycle()
				.then((result) => {
					if (result.more) this.arm();
				})
				.catch((error) => {
					let logParams: Record<string, unknown> = {};
					try {
						logParams = this.#logParams();
					} catch (logError) {
						logParams = { logParamsError: String(logError) };
					}
					console.error({
						...logParams,
						message: "fokos/partition: TTL expiry cycle failed.",
						error: String(error),
						errorProps: error,
					});
				});
		}, delay);
	}

	disarm(): void {
		if (this.#timer === null) return;
		clearTimeout(this.#timer);
		this.#timer = null;
	}

	async runCycle(): Promise<TtlSweepResult> {
		if (this.#running) return { deletedRows: 0, deletedBytes: 0, more: false };
		this.#running = true;
		try {
			const config = this.#config();
			validateConfig(config);

			let deletedRows = 0;
			let deletedBytes = 0;
			let rowsSinceSleep = 0;
			let bytesSinceSleep = 0;

			for (;;) {
				if (!this.#canSweep()) return { deletedRows, deletedBytes, more: false };
				if (deletedRows >= config.maxRowsPerCycle) return { deletedRows, deletedBytes, more: true };

				const limit = Math.min(config.chunkSize, config.maxRowsPerCycle - deletedRows);
				const result = this.#store.deleteExpiredItems(this.#nowSec(), limit);
				if (result.deletedRows === 0) return { deletedRows, deletedBytes, more: false };

				deletedRows += result.deletedRows;
				deletedBytes += result.deletedBytes;
				rowsSinceSleep += result.deletedRows;
				bytesSinceSleep += result.deletedBytes;

				if (deletedRows >= config.maxRowsPerCycle) return { deletedRows, deletedBytes, more: true };
				if (result.deletedRows < limit) return { deletedRows, deletedBytes, more: false };

				if (rowsSinceSleep >= config.maxRowsBeforeSleep || bytesSinceSleep >= config.maxBytesBeforeSleep) {
					await this.#wait(config.sleepMs);
					rowsSinceSleep = 0;
					bytesSinceSleep = 0;
				} else {
					await this.#wait(0);
				}
			}
		} finally {
			this.#running = false;
		}
	}
}

function validateConfig(config: TtlSweepConfig): void {
	for (const name of ["chunkSize", "maxRowsBeforeSleep", "maxBytesBeforeSleep", "maxRowsPerCycle"] as const) {
		if (!Number.isInteger(config[name]) || config[name] <= 0) {
			throw new Error(`fokos/ttl-expiry: ${name} must be an integer greater than zero`);
		}
	}
	for (const name of ["sleepMs", "initialDelayMs"] as const) {
		if (!Number.isInteger(config[name]) || config[name] < 0) {
			throw new Error(`fokos/ttl-expiry: ${name} must be an integer greater than or equal to zero`);
		}
	}
}
