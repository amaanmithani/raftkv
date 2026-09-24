"""Mutation testing for the test harness itself.

Each mutation injects a classic consensus bug into a scratch copy of the repo,
then runs the linearizability harness. A mutation is "caught" if the harness
reports a violation (an invariant failure, a non-linearizable history, or a
crash). If the harness can't catch these, a green run means nothing.

Writes results/mutations.json."""
import json, shutil, subprocess, sys, tempfile, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCHEDULES = int(sys.argv[1]) if len(sys.argv) > 1 else 300

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
]

results = []
for name, desc, path, old, new in MUTATIONS:
    with tempfile.TemporaryDirectory() as tmp:
        dst = Path(tmp) / "repo"
        shutil.copytree(ROOT, dst, ignore=shutil.ignore_patterns(".git", "results"))
        f = dst / path
        src = f.read_text()
        assert src.count(old) == 1, f"{name}: pattern not found exactly once"
        f.write_text(src.replace(old, new))
        (dst / "results").mkdir()
        p = subprocess.run(["go", "run", "./cmd/linearize", "-schedules", str(SCHEDULES), "-out", "results/out.json"],
                           cwd=dst, capture_output=True, text=True, timeout=1800)
        # Targeted scenario tests: timing-sensitive bugs that random schedules
        # rarely hit (Figure 8, stale-suffix commit, vote persistence).
        tp = subprocess.run(["go", "test", "-count=1", "-run", "Figure8|FollowerCommitsOnly|VotePersists", "./raft/", "./sim/"],
                            cwd=dst, capture_output=True, text=True, timeout=600)
        out = dst / "results" / "out.json"
        summary = json.loads(out.read_text()) if out.exists() else {}
        first = (summary.get("failing_seeds") or [{}])[0]
        if p.returncode != 0 and not summary:
            raise SystemExit(f"{name}: harness did not run (mutation broke the build?)\n{p.stderr[-2000:]}")
        by_random = p.returncode != 0
        by_scenario = tp.returncode != 0
        failed_tests = [l.split()[2] for l in tp.stdout.splitlines() if l.startswith("--- FAIL")]
        detail = first.get("invariant_violation") or ("non-linearizable history" if first else "")
        results.append({"mutation": name, "bug": desc, "caught": by_random or by_scenario,
                        "caught_by_randomized_harness": by_random, "failing_schedules": summary.get("violations"),
                        "of": SCHEDULES, "first_failing_seed": first.get("seed"), "randomized_failure": detail or None,
                        "caught_by_scenario_tests": failed_tests})
        print(f"{name:24s} random={by_random} ({summary.get('violations')}/{SCHEDULES}) scenario={failed_tests} {detail}"[:240])

report = {"what": "does the harness detect injected consensus bugs?", "schedules_per_mutation": SCHEDULES,
          "caught": sum(r["caught"] for r in results), "total": len(results),
          "date": datetime.date.today().isoformat(), "mutations": results}
(ROOT / "results").mkdir(exist_ok=True)
(ROOT / "results" / "mutations.json").write_text(json.dumps(report, indent=2) + "\n")
