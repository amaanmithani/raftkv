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
    f"**{lin['schedules']:,} schedules, {lin['violations']} violations.** {lin['operations_checked']:,} client operations checked; "
    f"{lin['crashes']:,} crashes, {lin['partitions']:,} partitions, {lin['leader_terms']:,} leader terms, "
    f"{lin['messages_dropped']:,} of {lin['messages_sent']:,} messages dropped. Wall time {lin['wall_seconds']:.0f} s on {lin['cpus']} cores.",
    "",
    "### Does the harness catch real bugs?",
    "",
    f"A checker that never fails proves nothing, so each classic consensus bug below was injected into a copy of the code "
    f"({mut['schedules_per_mutation']} schedules per bug, plus the targeted scenario tests). "
    f"**{mut['caught']} of {mut['total']} caught.**",
    "",
    "| injected bug | randomized harness | targeted scenario test |",
    "|---|---|---|",
]
for m in mut["mutations"]:
    rnd = f"{m['failing_schedules']}/{m['of']} schedules fail: {m['randomized_failure']}" if m["caught_by_randomized_harness"] else "not caught"
    sc = ", ".join(f"`{t}`" for t in m["caught_by_scenario_tests"]) or "—"
    L.append(f"| {m['bug']} | {rnd} | {sc} |")
L += [
    "",
    "Three bugs need timing that random schedules almost never produce, such as the leader-change sequence in Figure 8 of the "
    "Raft paper, so they're covered by scripted scenario tests that drive exact message interleavings.",
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
