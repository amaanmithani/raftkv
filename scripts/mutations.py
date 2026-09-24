"""Mutation testing for the test harness itself.

Each mutation injects a classic consensus bug into a scratch copy of the repo,
then runs the linearizability harness. A mutation is "caught" if the harness
reports a violation (an invariant failure, a non-linearizable history, or a
crash). If the harness can't catch these, a green run means nothing.

Writes results/mutations.json."""
import builtins, functools, json, shutil, subprocess, sys, tempfile, datetime
print = functools.partial(builtins.print, flush=True)
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCHEDULES = int(sys.argv[1]) if len(sys.argv) > 1 else 300
# --only a,b reruns just those mutations and merges them into results/mutations.json.
ONLY = set(sys.argv[sys.argv.index("--only") + 1].split(",")) if "--only" in sys.argv else None

MUTATIONS = [
    ("vote-ignores-log", "Voters skip the up-to-date log check (§5.4.1)", "raft/raft.go",
     "granted := canVote && upToDate", "granted := canVote"),
    ("double-vote", "A node may vote for several candidates in one term", "raft/raft.go",
     "canVote := n.vote == m.From || (n.vote == None && n.leader == None)", "canVote := true"),
    ("commit-old-term", "Leader commits earlier-term entries by counting replicas (Figure 8 bug)", "raft/raft.go",
     "idx > n.commit && t == n.term", "idx > n.commit && t > 0"),
    ("follower-over-commits", "Follower commits up to the leader's index without checking its log matches that far",
     "raft/raft.go", "if c := min(m.Commit, lastNew); c > n.commit {", "if c := min(m.Commit, n.log.lastIndex()); c > n.commit {"),
    ("forget-vote-on-crash", "The vote isn't persisted, so a restarted node can vote twice in a term", "sim/sim.go",
     "n.st.hs = rd.HardState", "n.st.hs = raft.HardState{Term: rd.HardState.Term, Commit: rd.HardState.Commit}"),
    ("no-dedup", "The state machine re-executes retried requests", "kv/kv.go",
     "case c.Seq == sess.Seq:", "case false:"),
    ("stale-suffix-on-install", "Installing a snapshot keeps the old log suffix in storage (found by review)", "sim/sim.go",
     "\t\t\tn.st.entries = nil\n", "\t\t\tn.st.entries = trimTo(n.st.entries, rd.Snapshot.Index)\n"),
    ("leader-ignores-higher-term", "A leader keeps leading after an append response shows a higher term (found by review)",
     "raft/raft.go", "		case m.Type == MsgApp || m.Type == MsgSnap:\n			n.becomeFollower(m.Term, m.From)\n		default:\n			n.becomeFollower(m.Term, None)",
     "		case m.Type == MsgApp || m.Type == MsgSnap:\n			n.becomeFollower(m.Term, m.From)\n		case m.Type == MsgAppResp && n.role == Leader:\n		default:\n			n.becomeFollower(m.Term, None)"),
]

results = []
NODE_MUTATIONS = [
    # (name, description, path, [(old, new), ...]) — bugs in the real server, outside the pure core.
    ("send-before-persist", "Real server sends messages before persisting the Ready batch", "node/node.go", [
        ("""		if err := s.store.Save(rd.HardState, rd.HardStateChanged, rd.Snapshot, rd.Entries); err != nil {
			return err
		}
		for _, m := range rd.Messages {""", """		for _, m := range rd.Messages {"""),
        ("""		if rd.Snapshot != nil {
			if err := s.kv.Restore(rd.Snapshot.Data); err != nil {""", """		if err := s.store.Save(rd.HardState, rd.HardStateChanged, rd.Snapshot, rd.Entries); err != nil {
			return err
		}
		if rd.Snapshot != nil {
			if err := s.kv.Restore(rd.Snapshot.Data); err != nil {"""),
    ]),
    ("wal-drops-entries", "Real server's WAL never stores log entries", "node/storage.go", [
        ("rec := walRecord{Entries: entries}", "rec := walRecord{}"),
    ]),
    ("install-keeps-wal-suffix", "Real server keeps the stale WAL suffix when installing a snapshot (found by review)", "node/storage.go", [
        ("		if keepSuffix && e.Index > snap.Index {", "		if e.Index > snap.Index {"),
    ]),
]
for name, desc, path, old, new in MUTATIONS:
    if ONLY and name not in ONLY:
        continue
    with tempfile.TemporaryDirectory() as tmp:
        dst = Path(tmp) / "repo"
        shutil.copytree(ROOT, dst, ignore=shutil.ignore_patterns(".git", "results"))
        f = dst / path
        src = f.read_text()
        assert src.count(old) == 1, f"{name}: pattern not found exactly once"
        f.write_text(src.replace(old, new))
        (dst / "results").mkdir()
        p = subprocess.run(["go", "run", "./cmd/linearize", "-schedules", str(SCHEDULES), "-check-timeout", "3s",
                            "-out", "results/out.json"], cwd=dst, capture_output=True, text=True, timeout=3600)
        # Targeted scenario tests: timing-sensitive bugs that random schedules
        # rarely hit (Figure 8, stale-suffix commit, vote persistence).
        tp = subprocess.run(["go", "test", "-count=1", "-run", "Figure8|FollowerCommitsOnly|VotePersists|HigherTermResponse|SnapshotInstallDrops", "./raft/", "./sim/"],
                            cwd=dst, capture_output=True, text=True, timeout=600)
        out = dst / "results" / "out.json"
        summary = json.loads(out.read_text()) if out.exists() else {}
        first = (summary.get("failing_seeds") or [{}])[0]
        crashed = False
        if p.returncode != 0 and not summary:
            b = subprocess.run(["go", "build", "./..."], cwd=dst, capture_output=True, text=True)
            if b.returncode != 0:
                raise SystemExit(f"{name}: mutation broke the build\n{b.stderr[-2000:]}")
            # The harness process itself died (e.g. runaway memory): that's a
            # symptom, not a diagnosed violation, so it isn't counted as caught.
            crashed = True
        by_random = p.returncode == 1 and not crashed  # 2 = only inconclusive checks: not counted as caught
        by_scenario = tp.returncode != 0
        failed_tests = [l.split()[2] for l in tp.stdout.splitlines() if l.startswith("--- FAIL")]
        detail = first.get("invariant_violation") or ("non-linearizable history" if first else "")
        if crashed:
            detail = "harness process died (" + ((p.stderr.strip().splitlines() or ["?"])[-1])[:80] + "); not counted"
        results.append({"mutation": name, "bug": desc, "caught": by_random or by_scenario,
                        "caught_by_randomized_harness": by_random, "failing_schedules": summary.get("violations"),
                        "of": SCHEDULES, "inconclusive_checks": summary.get("inconclusive_checks"),
                        "first_failing_seed": first.get("seed"), "randomized_failure": detail or None,
                        "caught_by_scenario_tests": failed_tests})
        print(f"{name:24s} inconclusive={summary.get('inconclusive_checks')} random={by_random} ({summary.get('violations')}/{SCHEDULES}) scenario={failed_tests} {detail}"[:240])

# Real-server mutations: the randomized harness never runs node/, so only the
# node and storage tests can catch these.
for name, desc, path, pairs in NODE_MUTATIONS:
    if ONLY and name not in ONLY:
        continue
    with tempfile.TemporaryDirectory() as tmp:
        dst = Path(tmp) / "repo"
        shutil.copytree(ROOT, dst, ignore=shutil.ignore_patterns(".git", "results"))
        f = dst / path
        src = f.read_text()
        for old, new in pairs:
            assert src.count(old) == 1, f"{name}: pattern not found exactly once: {old[:60]}"
            src = src.replace(old, new)
        f.write_text(src)
        tp = subprocess.run(["go", "test", "-count=1", "./node/"], cwd=dst, capture_output=True, text=True, timeout=900)
        failed_tests = sorted({l.split()[2] for l in tp.stdout.splitlines() if l.startswith("--- FAIL")})
        panicked = "panic:" in tp.stdout
        caught = tp.returncode != 0
        results.append({"mutation": name, "bug": desc, "caught": caught, "caught_by_randomized_harness": False,
                        "failing_schedules": None, "of": None, "first_failing_seed": None, "randomized_failure": None,
                        "caught_by_scenario_tests": failed_tests or (["node tests (panic)"] if panicked and caught else []),
                        "layer": "real server"})
        print(f"{name:24s} node-tests caught={caught} {failed_tests}")

if ONLY:
    prev = json.loads((ROOT / "results" / "mutations.json").read_text())["mutations"]
    names = {r["mutation"] for r in results}
    results = [r for r in prev if r["mutation"] not in names] + results
    order = [m[0] for m in MUTATIONS] + [m[0] for m in NODE_MUTATIONS]
    results.sort(key=lambda r: order.index(r["mutation"]) if r["mutation"] in order else 99)

report = {"what": "does the harness detect injected consensus bugs?", "schedules_per_mutation": SCHEDULES,
          "caught": sum(r["caught"] for r in results), "total": len(results),
          "date": datetime.date.today().isoformat(), "mutations": results}
(ROOT / "results").mkdir(exist_ok=True)
(ROOT / "results" / "mutations.json").write_text(json.dumps(report, indent=2) + "\n")
