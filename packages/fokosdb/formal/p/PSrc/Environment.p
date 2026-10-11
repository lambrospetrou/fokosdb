// The number of steps in which the Environment can inject a restart. After them, every fault stops.
fun ENVIRONMENT_STEPS(): int { return 20; }

// The ticks after the end of the faults and after the last change of an alarm. A Durable Object
// that keeps a deadline for good, such as a coordinator whose participant never answers, otherwise
// keeps the run going for good. Each change of an alarm starts the count again, and the work of a
// run is finite, so each run ends.
fun TICKS_AFTER_FAULTS(): int { return 20; }

// `down` holds the Durable Objects that drop every call, also after the end of the faults.
type tEnvConfig = (restarts: int, losses: int, answersOnly: bool, down: set[machine]);
type tEnvSetup = (config: tEnvConfig, dos: seq[machine]);
event eEnvSetup: tEnvSetup;
event eEnvStep;
event eNextTick;

// A call started, and a call ended.
event eRpcOpened;
event eRpcClosed;

// The faults and the wall clock.
//
// Faults: a restart of a Durable Object between two of its handlers, and a lost request or answer of
// one call. One budget of each kind holds for the whole run.
//
// Clock: it sends each tick to every Durable Object. It ticks only while some Durable Object has an
// alarm deadline, and only while no call is open: a call takes milliseconds, and a tick is about
// staleTransactionMs. Each inbox is in send order, so a message that a Durable Object sends after it
// read tick t reaches each other Durable Object after tick t.
machine Environment {
  var config: tEnvConfig;
  var dos: seq[machine];
  var steps: int;
  var faultsEnded: bool;
  var armed: set[machine];
  var openCalls: int;
  var now: int;
  var ticking: bool;
  var ticksAfterFaults: int;

  start state Init {
    defer eMayLose, eArmed, eRpcOpened, eRpcClosed;
    on eEnvSetup do (s: tEnvSetup) {
      config = s.config;
      dos = s.dos;
      goto Running;
    }
  }

  state Running {
    entry {
      send this, eEnvStep;
    }

    on eEnvStep do {
      if (steps < ENVIRONMENT_STEPS() && (config.restarts > 0 || config.losses > 0)) {
        steps = steps + 1;
        if (config.restarts > 0 && $) {
          config.restarts = config.restarts - 1;
          send choose(dos), eRestart;
        }
        send this, eEnvStep;
        return;
      }
      faultsEnded = true;
    }

    on eMayLose do (q: tMayLose) {
      var loss: tLoss;
      loss = LOSE_NONE;
      if (q.target in config.down) {
        loss = LOSE_REQUEST;
      } else if (config.losses > 0 && $) {
        config.losses = config.losses - 1;
        if (config.answersOnly || $) {
          loss = LOSE_ANSWER;
        } else {
          loss = LOSE_REQUEST;
        }
      }
      send q.rpc, eLossDecision, loss;
    }

    on eRpcOpened do {
      openCalls = openCalls + 1;
    }

    on eRpcClosed do {
      openCalls = openCalls - 1;
      Wake();
    }

    on eArmed do (a: tArmed) {
      if (a.armed) {
        armed += (a.who);
      } else {
        armed -= (a.who);
      }
      ticksAfterFaults = 0;
      Wake();
    }

    on eNextTick do {
      var target: machine;
      ticking = false;
      if (!CanTick()) {
        return;
      }
      now = now + 1;
      if (faultsEnded) {
        ticksAfterFaults = ticksAfterFaults + 1;
      }
      foreach (target in dos) {
        send target, eTick, now;
      }
      Wake();
    }
  }

  fun CanTick(): bool {
    return openCalls == 0 && sizeof(armed) > 0 && ticksAfterFaults < TICKS_AFTER_FAULTS();
  }

  fun Wake() {
    if (!ticking && CanTick()) {
      ticking = true;
      send this, eNextTick;
    }
  }
}
