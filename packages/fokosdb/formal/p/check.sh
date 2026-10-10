#!/usr/bin/env bash
# Compiles the P model of FokosDB and runs its test cases in Docker.
#
# A test case named tcBug<Id> must report a violation of the monitor that EXPECTED_MONITOR gives.
# Every other test case must report no bug. The script exits non-zero on any other result.
#
# Usage:
#   check.sh [test case...] [-- <p check option>...]
#       The random checker (default: every test case of the model).
#   check.sh --pex [test case...] [-- <p check option>...]
#       The exhaustive checker PEx. A test case passes only when PEx explores every state, and a
#       tcBug<Id> test case only when PEx finds the violation.
#   check.sh --replay <test case> <schedule file>
# The script gives each option after "--" to every `p check` call without a change, for example
# `-- --sch-pct 3 --seed 42`. The script refuses an option that it sets itself.
# SCHEDULES sets the number of schedules of the random checker (default 1000). PEX_TIMEOUT sets the
# time limit of PEx for each test case, in seconds (default 60). The random checker stops at the
# first bug, and the output gives the number of schedules and timelines that it explored.
# The output of a test case goes to PCheckerOutput/<test case>/, and to PCheckerOutput/pex/<test case>/
# for PEx. The random checker writes the schedule of a bug to BugFinding/FokosDB_0_0.schedule there.
# The Maven repository of PEx stays in .p-cache/ between runs.
set -euo pipefail

cd "$(dirname "$0")"

P_VERSION=3.1.0
IMAGE="fokosdb-p:${P_VERSION}"
SCHEDULES="${SCHEDULES:-1000}"
PEX_TIMEOUT="${PEX_TIMEOUT:-60}"

declare -A EXPECTED_MONITOR=(
	[tcBugV1]=VersionIncreases
)

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
	docker build --build-arg P_VERSION="$P_VERSION" -t "$IMAGE" .
fi

p() {
	docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp \
		-e MAVEN_OPTS="-Dmaven.repo.local=/workspace/.p-cache/m2" \
		-v "$PWD":/workspace "$IMAGE" p "$@"
}

mode=bugfinding
case "${1:-}" in
--replay) ;;
--pex)
	mode=pex
	shift
	;;
esac

if ! out=$(p compile --mode "$mode" 2>&1); then
	echo "$out"
	exit 1
fi

if [[ "${1:-}" == "--replay" ]]; then
	p check -tc "$2" --replay "$3" -o "PCheckerOutput/$2/replay"
	exit $?
fi

tests=()
check_options=()
while [[ $# -gt 0 ]]; do
	if [[ "$1" == "--" ]]; then
		shift
		check_options=("$@")
		break
	fi
	tests+=("$1")
	shift
done
if [[ ${#tests[@]} -eq 0 ]]; then
	mapfile -t tests < <(sed -nE 's/^test ([A-Za-z0-9_]+).*/\1/p' PTst/*.p)
fi
if [[ ${#tests[@]} -eq 0 ]]; then
	echo "No test case to run."
	exit 1
fi
for option in "${check_options[@]}"; do
	case "$option" in
	-tc | --testcase | -o | --outdir | -md | --mode | -r | --replay | -s | --schedules | -t | --timeout)
		echo "The script sets $option. Use the test case arguments, SCHEDULES, or PEX_TIMEOUT."
		exit 1
		;;
	esac
done
if [[ ${#check_options[@]} -gt 0 ]]; then
	echo "p check options: ${check_options[*]}"
fi

# The monitor that an error line names. A liveness error names the monitor. An assertion gives its
# source location, and the monitor is the spec block that holds that line.
violated_monitor() {
	local line="$1" file lineno
	if [[ "$line" =~ Monitor\ .([A-Za-z0-9_]+) ]]; then
		echo "${BASH_REMATCH[1]}"
	elif [[ "$line" =~ (PSpec/[A-Za-z0-9_]+\.p):([0-9]+) ]]; then
		file="${BASH_REMATCH[1]}"
		lineno="${BASH_REMATCH[2]}"
		awk -v n="$lineno" 'NR <= n && $1 == "spec" { name = $2 } END { print name }' "$file"
	else
		echo "(no monitor)"
	fi
}

failures=0
for tc in "${tests[@]}"; do
	if [[ "$mode" == pex ]]; then outdir="PCheckerOutput/pex/$tc"; else outdir="PCheckerOutput/$tc"; fi
	log="$outdir/check.log"
	rm -rf "$outdir"
	mkdir -p "$outdir"
	start=$(date +%s)
	if [[ "$mode" == pex ]]; then
		p check --mode pex -tc "$tc" -t "$PEX_TIMEOUT" -o "$outdir" "${check_options[@]}" >"$log" 2>&1 || true
		clean_pattern='Result: correct for any depth'
		bug_pattern='Result: found cex'
		error_line() { grep -m1 'Property violated' "$log" || true; }
		bound="time limit ${PEX_TIMEOUT}s"
	else
		p check -tc "$tc" -s "$SCHEDULES" -o "$outdir" "${check_options[@]}" >"$log" 2>&1 || true
		clean_pattern='Found 0 bugs\.'
		bug_pattern='Checker found a bug\.'
		error_line() { grep -m1 -h '^<ErrorLog>' "$outdir"/BugFinding/*_0_0.txt || true; }
		explored() { grep -m1 -oE "Explored [0-9]+ $1" "$log" | awk '{ print $2 } END { if (NR == 0) print 0 }' || true; }
		bound="$(explored schedule) of $SCHEDULES schedules, $(explored timeline) timelines"
	fi
	secs=$(($(date +%s) - start))
	ran=$(grep -c '^\.\. Test case :: ' "$log" || true)
	expected="${EXPECTED_MONITOR[$tc]:-}"
	if grep -q '^Error:' "$log"; then
		result="ERROR: $(grep -m1 '^Error:' "$log" | sed 's/^Error: //'), see $log"
	elif [[ "$ran" -ne 1 ]]; then
		result="ERROR: the name ran $ran test cases, see $log"
	elif grep -q "$clean_pattern" "$log"; then
		if [[ -z "$expected" ]]; then result="ok"; else result="FAIL: expected a violation of $expected"; fi
	elif grep -q "$bug_pattern" "$log"; then
		got=$(violated_monitor "$(error_line)")
		if [[ "$tc" != tcBug* ]]; then
			result="FAIL: violation of $got, see $outdir"
		elif [[ "$got" == "$expected" ]]; then
			result="ok (violation of $got)"
		else
			result="FAIL: expected a violation of ${expected:-a monitor in EXPECTED_MONITOR}, got $got, see $outdir"
		fi
	elif [[ "$mode" == pex ]] && grep -q 'Result:' "$log"; then
		result="INCOMPLETE:$(grep -m1 -o 'Result:[^[]*' "$log" | sed 's/^Result://')"
	else
		result="ERROR: see $log"
	fi
	printf '%-24s %-36s %5ss  %s\n' "$tc" "$bound" "$secs" "$result"
	[[ "$result" == ok* ]] || failures=$((failures + 1))
done

if [[ $failures -gt 0 ]]; then
	echo "$failures test case(s) did not give the expected result."
	exit 1
fi
