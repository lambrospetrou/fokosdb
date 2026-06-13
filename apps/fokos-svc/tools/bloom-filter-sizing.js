#!/usr/bin/env node

const args = process.argv.slice(2);
if (args.length < 2) {
	console.error("Usage: node tools/bloom-filter-sizing.js <initialCapacity> <maxSize>");
	console.error("  initialCapacity  Number of items for the first layer (e.g. 200000 or 200K)");
	console.error("  maxSize          Maximum total size (e.g. 1048576 or 1MB)");
	process.exit(1);
}

function parseNumber(s) {
	const match = s.match(/^([\d.]+)\s*(KB|MB|GB|K|M|B)?$/i);
	if (!match) {
		const n = Number(s);
		if (isNaN(n)) {
			console.error(`Invalid number: ${s}`);
			process.exit(1);
		}
		return n;
	}
	const value = parseFloat(match[1]);
	const unit = (match[2] || "").toUpperCase();
	switch (unit) {
		case "KB":
			return value * 1024;
		case "MB":
			return value * 1024 * 1024;
		case "GB":
			return value * 1024 * 1024 * 1024;
		case "K":
			return value * 1000;
		case "M":
			return value * 1_000_000;
		case "B":
			return value;
		default:
			return value;
	}
}

const initialCapacity = Math.floor(parseNumber(args[0]));
const maxSize = Math.floor(parseNumber(args[1]));
const errorRate = 0.01;
const TIGHTENING_RATIO = 0.5;
const LAYER_GROWTH_FACTOR = 2;
const LN2SQ = Math.LN2 * Math.LN2;

function fmt(n) {
	return n.toLocaleString();
}
function fmtBytes(b) {
	if (b < 1024) return `${b} B`;
	if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`;
	return `${(b / (1024 * 1024)).toFixed(2)} MB`;
}

console.log(`Initial capacity: ${fmt(initialCapacity)} items | Max size: ${fmtBytes(maxSize)} | Error rate: ${errorRate}`);
console.log();
console.log(
	"Layer".padEnd(7) + "Capacity".padStart(12) + "Per-layer FPR".padStart(16) + "Size".padStart(12) + "Running Total".padStart(16) + "  k",
);
console.log("-".repeat(67));

let total = 0;
for (let i = 0; ; i++) {
	const fpr = errorRate * Math.pow(TIGHTENING_RATIO, i + 1);
	const capacity = initialCapacity * Math.pow(LAYER_GROWTH_FACTOR, i);
	const m = Math.ceil((-capacity * Math.log(fpr)) / LN2SQ);
	const bytes = Math.ceil(m / 8);
	const k = Math.max(1, Math.round((m / capacity) * Math.LN2));
	total += bytes;

	console.log(
		String(i).padEnd(7) +
			fmt(capacity).padStart(12) +
			`${(fpr * 100).toFixed(4)}%`.padStart(16) +
			fmtBytes(bytes).padStart(12) +
			fmtBytes(total).padStart(16) +
			String(k).padStart(4),
	);

	if (total > maxSize) break;
}
