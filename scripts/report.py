"""Render results/*.json into README.md between the RESULTS markers."""
import json, re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
R = ROOT / "results"
lin = json.loads((R / "linearizability.json").read_text())
mut = json.loads((R / "mutations.json").read_text())
tp = json.loads((R / "throughput.json").read_text())

L = [
    "### Linearizability under faults",
    "",
    f"{lin['method']}.",
    "",
    f"**{lin['schedules']:,} schedules, {lin['violations']} violations, {lin.get('inconclusive_checks', 0)} inconclusive checks.** {lin['operations_checked']:,} client operations checked "
    f"(reads and writes, all through the log); {lin['crashes']:,} crashes, {lin['partitions']:,} partitions, "
    f"{lin['leader_terms']:,} leader terms, {lin['messages_dropped']:,} of {lin['messages_sent']:,} messages dropped. "
    f"{lin['ops_overlapping_a_fault'] / lin['operations_completed']:.0%} of operations were in flight during an active fault; "
    f"{lin['ops_completed_after_healing'] / lin['operations_completed']:.0%} only completed after the healing phase. "
    f"Wall time {lin['wall_seconds']:.0f} s on {lin['cpus']} cores.",
    "",
    "**Scope.** The randomized harness exercises the pure Raft and KV code under an idealized storage model (writes are "
    "atomic, crashes happen between steps). Client replies are never lost, so uncertainty comes only from timeouts and "
    "retries. The real server's storage and crash ordering are covered separately by storage and cluster tests (below).",
    "",
    "### Does the harness catch real bugs?",
    "",
    f"A checker that never fails proves nothing, so each bug below was injected into a copy of the code and the harness "
    f"was run against it ({mut['schedules_per_mutation']} randomized schedules per core bug with a 3 s checker limit per "
    f"history, where a timeout counts as not caught; plus the scenario, storage and "
    f"cluster tests). **{mut['caught']} of {mut['total']} caught: "
    f"{sum(m['caught_by_randomized_harness'] for m in mut['mutations'])} by randomized schedules, the rest only by tests "
    f"written for them.** Misses are listed, not hidden.",
    "",
    "| injected bug | where | randomized harness | targeted tests |",
    "|---|---|---|---|",
]
for m in mut["mutations"]:
    layer = m.get("layer", "raft/kv core")
    if m["of"] is None:
        rnd = "n/a (doesn't run the server)"
    elif m["caught_by_randomized_harness"]:
        rnd = f"{m['failing_schedules']}/{m['of']} schedules fail: {m['randomized_failure']}"
    else:
        rnd = "not caught"
    if m.get("inconclusive_checks"):
        rnd += f" ({m['inconclusive_checks']} more timed out in the checker, not counted)"
    sc = ", ".join(f"`{t}`" for t in m["caught_by_scenario_tests"]) or ("**missed**" if not m["caught"] else "—")
    L.append(f"| {m['bug']} | {layer} | {rnd} | {sc} |")
L += [
    "",
    "Some bugs need timing that random schedules almost never produce, such as the leader-change sequence in Figure 8 of the "
    "Raft paper, so they're covered by scripted scenario tests that drive exact message interleavings. Bugs that only "
    "matter when a real disk is interrupted mid-write (for example sending before persisting) are the weak spot: the "
    "simulator can't see them and the cluster test doesn't crash at the right instant.",
    "",
    "### Throughput and latency vs cluster size",
    "",
    f"{tp['method']}. Machine: {tp['machine']['cpu']}, {tp['machine']['cores']} cores.",
    "",
    "| nodes | fsync | clients | writes/s | p50 | p99 | errors |",
    "|---|---|---|---|---|---|---|",
]
for r in tp["runs"]:
    x = r["result"]
    L.append(f"| {r['nodes']} | {r['fsync']} | {x['clients']} | {x['ops_per_sec']:,.0f} | {x['p50_ms']:.1f} ms | {x['p99_ms']:.1f} ms | {x['errors']} |")
on = {(r["nodes"], r["result"]["clients"]): r["result"] for r in tp["runs"] if r["fsync"] == "on"}
L += [
    "",
    "Clients are closed-loop (each waits for its reply before sending the next write). Up to 7 nodes and the load "
    f"generator share {tp['machine']['cores']} cores, so part of the decline with cluster size is CPU contention rather than "
    "replication cost; each row is a single run.",
    "",
    f"With fsync on, latency is set by the disk flush (leader, then followers, in sequence), so throughput scales with "
    f"concurrency: 3 nodes go from {on[(3, 32)]['ops_per_sec']:,.0f} to {on[(3, 128)]['ops_per_sec']:,.0f} writes/s between 32 and 128 "
    f"clients at the same p50, because concurrent requests share one WAL record and one fsync (group commit).",
]
readme = ROOT / "README.md"
text = readme.read_text()
block = "<!-- RESULTS:START -->\n" + "\n".join(L) + "\n<!-- RESULTS:END -->"
if "<!-- RESULTS:START -->" in text:
    text = re.sub(r"<!-- RESULTS:START -->.*<!-- RESULTS:END -->", lambda _: block, text, flags=re.S)
else:
    text = text.rstrip() + "\n\n## Results\n\n" + block + "\n"
readme.write_text(text)
print("README results updated")
