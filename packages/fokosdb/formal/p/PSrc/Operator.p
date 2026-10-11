type tOperatorSetup = (coordinator: machine, partitions: seq[machine], env: machine);
event eOperatorSetup: tOperatorSetup;

// An operator who reads the lock-age guard error of a partition, finds the outcome of the
// transaction in the completion log of the coordinator, and calls debugForceResolveTransaction with
// that outcome on every partition. A failed call is sent again, up to three times.
machine Operator {
  var coordinator: machine;
  var partitions: seq[machine];
  var env: machine;
  var repaired: set[int];
  var outcome: map[int, bool];
  // The attempts of each call, by its request id. The id is the index of the call.
  var calls: seq[(tx: int, partition: machine, attempts: int)];

  start state Init {
    defer eGuardLogged;
    on eOperatorSetup do (s: tOperatorSetup) {
      coordinator = s.coordinator;
      partitions = s.partitions;
      env = s.env;
      goto Watching;
    }
  }

  state Watching {
    on eGuardLogged do (g: tGuardLogged) {
      if (!(g.tx in repaired)) {
        repaired += (g.tx);
        send coordinator, eLogLookup, (operator = this, tx = g.tx);
      }
    }

    on eLogEntry do (e: tLogEntry) {
      var partition: machine;
      if (!e.known) {
        return;
      }
      outcome[e.tx] = e.committed;
      foreach (partition in partitions) {
        calls += (sizeof(calls), (tx = e.tx, partition = partition, attempts = 1));
        Call(sizeof(calls) - 1);
      }
    }

    on eForceResolveResp do (r: tForceResolveResp) {}

    on eRpcFailed do (f: tRpcFailed) {
      if (calls[f.reqId].attempts < 3) {
        calls[f.reqId].attempts = calls[f.reqId].attempts + 1;
        Call(f.reqId);
      }
    }
  }

  fun Call(id: int) {
    send NewRpc(this, calls[id].partition, env), eForceResolve,
      (caller = this, reqId = id, tx = calls[id].tx, commit = outcome[calls[id].tx]);
  }
}
