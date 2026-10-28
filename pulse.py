#!/usr/bin/env python3
"""X1 Pulse – data generator.

Every run (about every 15 min) it:
  1. takes the slots produced since the previous run (getBlocks, 1 call),
  2. downloads a small even sample of those blocks (default 20),
  3. stores per-run counters in .state/runs.json (kept 7 days),
  4. aggregates the last 24 h, checks tokens with the SAME scoring as the bot
     (scanner.py from ~/x1-rugcheck-bot), and writes data.json,
  5. commits data.json and pushes it to GitHub Pages (single amended commit,
     so the repository never grows).

Run once:   ~/x1-rugcheck-bot/.venv/bin/python pulse.py
No push:    ... pulse.py --no-push
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

BOT_DIR = Path(os.environ.get("X1_BOT_DIR", Path.home() / "x1-rugcheck-bot"))
sys.path.insert(0, str(BOT_DIR))
from x1rpc import RpcError, X1Rpc  # noqa: E402
import scanner  # noqa: E402

RPC_URL = "https://rpc.mainnet.x1.xyz"
HERE = Path(__file__).resolve().parent
STATE = HERE / ".state"
RUNS_FILE = STATE / "runs.json"
CACHE_FILE = STATE / "cache.json"
OUT_FILE = HERE / "data.json"
TWEET_FILE = STATE / "tweet.txt"

SAMPLE_BLOCKS = int(os.environ.get("PULSE_SAMPLES", "20"))
DEFAULT_WINDOW_S = 15 * 60
MAX_WINDOW_SLOTS = 6000          # after a long pause we only look at the latest slots
KEEP_S = 7 * 86400
DAY = 86400
SCAN_TTL = 6 * 3600              # token risk re-check interval
MEMO_TTL = 3600
CALL_GAP = 0.25                  # seconds between RPC calls (be nice to the public RPC)

BOT_URL = "https://t.me/X1RugCheckerBot"
EXPLORER = "https://explorer.mainnet.x1.xyz/address/"

VOTE = "Vote111111111111111111111111111111111111111"
SKIP_PROGRAMS = {"ComputeBudget111111111111111111111111111111", VOTE}
WXNT = "So11111111111111111111111111111111111111112"
MEMO_MINT = "memoX1sJsBY6od7CfQ58XooRALwnocAZen4L7mW1ick"
MEMO_PROGRAM = "8iq6zqaEVcfaym2u8t939PAN5jmfPVc6Z333RuxKTTZX"

KNOWN = {
    "11111111111111111111111111111111": "System (XNT transfers)",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA": "SPL Token",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb": "Token-2022 (X1)",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PE7Jx6F7JCPKsKz": "Token-2022",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL": "Associated Token Account",
    "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr": "Memo",
    "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo": "Memo v1",
    MEMO_PROGRAM: "MEMO minting (memo.rip)",
    "BxUNg2yo5371BQMZPkfcxdCptFRDHkhvEXNM1QNPBRYU": "Randomness oracle (commit/reveal bot)",
    "Stake11111111111111111111111111111111111111": "Staking",
    "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s": "Metaplex metadata",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "Orca Whirlpool fork",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": "Raydium CLMM fork",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "Raydium AMM fork",
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C": "Raydium CPMM fork",
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": "Jupiter fork",
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P": "pump.fun fork",
    "AddressLookupTab1e1111111111111111111111111": "Address Lookup Table",
}


def log(msg: str) -> None:
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def load(path: Path, default):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default


def save(path: Path, data) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, separators=(",", ":")))
    tmp.replace(path)


class PoliteRpc(X1Rpc):
    """Bot's RPC client + throttling + 3 retries."""

    async def call(self, method, params=None):
        for attempt in range(3):
            await asyncio.sleep(CALL_GAP)
            try:
                return await super().call(method, params)
            except RpcError:
                if attempt == 2:
                    raise
                await asyncio.sleep(3 + attempt * 4)


def k(key):
    return key["pubkey"] if isinstance(key, dict) else key


# ------------------------------------------------------------------ sampling
def analyse_block(blk: dict, rec: dict) -> None:
    programs, mints = Counter(rec["programs"]), Counter(rec["mints"])
    memo_tx, memo_amt = Counter(rec["memo_tx"]), Counter(rec["memo_amt"])
    wallets, new_mints = set(rec["wallets"]), set(rec["new_mints"])
    for tx in blk.get("transactions") or []:
        msg = tx["transaction"]["message"]
        meta = tx.get("meta") or {}
        ixs = list(msg.get("instructions") or [])
        for inner in meta.get("innerInstructions") or []:
            ixs += inner.get("instructions") or []
        progs = {ix.get("programId") for ix in ixs if ix.get("programId")}
        if VOTE in progs:
            continue
        rec["tx"] += 1
        keys = msg.get("accountKeys") or []
        payer = k(keys[0]) if keys else None
        if payer:
            wallets.add(payer)
        for p in progs - SKIP_PROGRAMS:
            programs[p] += 1
        for ix in ixs:
            parsed = ix.get("parsed")
            if isinstance(parsed, dict) and parsed.get("type") in ("initializeMint", "initializeMint2"):
                m = (parsed.get("info") or {}).get("mint")
                if m:
                    new_mints.add(m)
        pre = meta.get("preTokenBalances") or []
        post = meta.get("postTokenBalances") or []
        for m in {b.get("mint") for b in pre + post if b.get("mint")}:
            mints[m] += 1
        if MEMO_PROGRAM in progs and payer:
            memo_tx[payer] += 1
            delta: Counter = Counter()
            for b in post:
                if b.get("mint") == MEMO_MINT and b.get("owner"):
                    delta[b["owner"]] += int(b["uiTokenAmount"]["amount"])
            for b in pre:
                if b.get("mint") == MEMO_MINT and b.get("owner"):
                    delta[b["owner"]] -= int(b["uiTokenAmount"]["amount"])
            for owner, d in delta.items():
                if d > 0:
                    memo_amt[owner] += d
    rec.update(programs=dict(programs), mints=dict(mints), memo_tx=dict(memo_tx),
               memo_amt=dict(memo_amt), wallets=sorted(wallets), new_mints=sorted(new_mints))


async def slots_per_sec(rpc: PoliteRpc, top: int, cache: dict) -> float:
    c = cache.get("sps")
    if c and time.time() - c["t"] < 3600:
        return c["v"]
    t1 = await rpc.call("getBlockTime", [top])
    t0 = await rpc.call("getBlockTime", [top - 20000])
    v = 20000 / max(1, (t1 or 0) - (t0 or 0)) if t1 and t0 else 2.7
    cache["sps"] = {"t": time.time(), "v": v}
    return v


async def sample_run(rpc: PoliteRpc, runs: list, cache: dict) -> dict:
    top = await rpc.call("getSlot", [{"commitment": "finalized"}])
    sps = await slots_per_sec(rpc, top, cache)
    start = runs[-1]["slot_to"] + 1 if runs else top - int(DEFAULT_WINDOW_S * sps)
    start = max(start, top - MAX_WINDOW_SLOTS)
    produced = await rpc.call("getBlocks", [start, top, {"commitment": "finalized"}]) or []
    rec = {"t": int(time.time()), "slot_from": start, "slot_to": top, "sps": sps,
           "produced": len(produced), "sampled": 0, "tx": 0, "wallets": [], "programs": {},
           "mints": {}, "new_mints": [], "memo_tx": {}, "memo_amt": {}}
    if not produced:
        return rec
    n = min(SAMPLE_BLOCKS, len(produced))
    picks = [produced[int(i * (len(produced) - 1) / max(1, n - 1))] for i in range(n)]
    for slot in dict.fromkeys(picks):
        try:
            blk = await rpc.call("getBlock", [slot, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0,
                                                     "transactionDetails": "full", "rewards": False,
                                                     "commitment": "finalized"}])
        except RpcError as e:
            log(f"block {slot} skipped: {e}")
            continue
        if blk:
            rec["sampled"] += 1
            analyse_block(blk, rec)
    return rec


# ------------------------------------------------------------------ aggregation
def window(runs: list, t_from: float, t_to: float) -> list:
    return [r for r in runs if t_from < r["t"] <= t_to]


def summarise(rs: list) -> dict:
    sampled = sum(r["sampled"] for r in rs)
    tx = sum(r["tx"] for r in rs)
    produced = sum(r["produced"] for r in rs)
    slots = sum(r["slot_to"] - r["slot_from"] + 1 for r in rs)
    sps = rs[-1]["sps"] if rs else 2.7
    tpb = tx / sampled if sampled else 0.0
    blocks_per_slot = produced / slots if slots else 0.0
    covered_s = slots / sps if sps else 0
    wallets = set()
    for r in rs:
        wallets.update(r["wallets"])
    return {"sampled": sampled, "tx": tx, "produced": produced, "tpb": tpb,
            "blocks_per_day": blocks_per_slot * sps * DAY, "covered_s": covered_s,
            "wallets": len(wallets), "wallet_set": wallets}


def est_per_day(s: dict) -> int:
    return round(s["tpb"] * s["blocks_per_day"])


def merge(rs: list, field: str) -> Counter:
    c: Counter = Counter()
    for r in rs:
        c.update(r[field])
    return c


def name_of(program: str) -> str:
    return KNOWN.get(program, "Unknown program")


# ------------------------------------------------------------------ tokens
async def scan_token(rpc: PoliteRpc, mint: str, cache: dict) -> dict | None:
    tc = cache.setdefault("tokens", {})
    hit = tc.get(mint)
    if hit and time.time() - hit["t"] < SCAN_TTL:
        return hit["d"]
    try:
        res = await scanner.quick_scan(rpc, mint)
    except scanner.NotATokenError:
        tc[mint] = {"t": time.time(), "d": None}
        return None
    except Exception as e:  # RPC trouble – keep old result if any
        log(f"scan {mint} failed: {e}")
        return hit["d"] if hit else None
    level = "low" if res.score >= 75 else "medium" if res.score >= 45 else "high"
    flags = [f.text for f in res.findings if f.level in ("danger", "warn")][:3]
    d = {"mint": mint, "name": res.name, "symbol": res.symbol, "score": res.score, "risk": level,
         "flags": flags, "program": res.program, "supply": res.supply_ui(),
         "mint_authority": bool(res.mint_authority), "freeze_authority": bool(res.freeze_authority)}
    tc[mint] = {"t": time.time(), "d": d}
    return d


async def memo_info(rpc: PoliteRpc, cache: dict) -> dict:
    c = cache.get("memo")
    if c and time.time() - c["t"] < MEMO_TTL:
        return c["d"]
    d = {"decimals": 9, "supply_ui": None, "holders": []}
    try:
        res = scanner.parse_mint_account(MEMO_MINT, await rpc.get_parsed_account(MEMO_MINT))
        d["decimals"], d["supply_ui"] = res.decimals, res.supply_ui()
        largest = (await rpc.get_largest_token_accounts(MEMO_MINT))[:10]
        accs = await rpc.get_multiple_parsed_accounts([a["address"] for a in largest])
        for a, acc in zip(largest, accs):
            try:
                owner = acc["data"]["parsed"]["info"]["owner"]
            except (TypeError, KeyError):
                owner = a["address"]
            amt = int(a.get("amount", "0"))
            d["holders"].append({"owner": owner, "amount": amt / 10 ** res.decimals,
                                 "pct": amt / res.supply_raw * 100 if res.supply_raw else 0})
    except Exception as e:
        log(f"memo info failed: {e}")
        if c:
            return c["d"]
    cache["memo"] = {"t": time.time(), "d": d}
    return d


# ------------------------------------------------------------------ build
async def build(rpc: PoliteRpc, runs: list, cache: dict) -> dict:
    now = time.time()
    last = runs[-1]
    cur = window(runs, now - DAY, now)
    prev = window(runs, now - 2 * DAY, now - DAY)
    s, sp = summarise(cur), summarise(prev)

    hourly = []
    for h in range(24, 0, -1):
        rs = window(runs, now - h * 3600, now - (h - 1) * 3600)
        hs = summarise(rs)
        hourly.append({"t": int(now - (h - 1) * 3600),
                       "tx": round(est_per_day(hs) / 24) if rs and hs["sampled"] else None,
                       "wallets": hs["wallets"] if rs else None})
    daily = []
    for d in range(7, 0, -1):
        rs = window(runs, now - d * DAY, now - (d - 1) * DAY)
        ds = summarise(rs)
        daily.append({"t": int(now - (d - 1) * DAY),
                      "tx": est_per_day(ds) if rs and ds["sampled"] else None,
                      "wallets": ds["wallets"] if rs else None})

    prog = merge(cur, "programs")
    total_prog_tx = s["tx"] or 1
    programs = [{"id": p, "name": name_of(p), "tx": n, "share": n / total_prog_tx * 100}
                for p, n in prog.most_common(10)]

    memo = await memo_info(rpc, cache)
    mtx, mamt = merge(cur, "memo_tx"), merge(cur, "memo_amt")
    memo_wallets = set(mtx) | set(mamt)
    minters = sorted(({"wallet": w, "tx": mtx.get(w, 0), "amount": mamt.get(w, 0) / 10 ** memo["decimals"]}
                      for w in memo_wallets), key=lambda x: (-x["tx"], -x["amount"]))[:10]

    # tokens: first-seen bookkeeping for new mints (7 days)
    seen = cache.setdefault("new_seen", {})
    for r in runs:
        for m in r["new_mints"]:
            seen.setdefault(m, r["t"])
    for m in [m for m, t in seen.items() if now - t > KEEP_S]:
        del seen[m]
    mint_counts = merge(cur, "mints")
    active = []
    for m, n in mint_counts.most_common(14):
        if m in (WXNT,):
            continue
        d = await scan_token(rpc, m, cache)
        if d:
            active.append({**d, "tx": n})
        if len(active) >= 10:
            break
    new = []
    for m, t in sorted(seen.items(), key=lambda kv: -kv[1])[:10]:
        d = await scan_token(rpc, m, cache)
        if d:
            new.append({**d, "first_seen": t, "tx": mint_counts.get(m, 0)})

    tx_day, tx_prev = est_per_day(s), est_per_day(sp)
    data = {
        "generated_at": int(now),
        "network": {"slot": last["slot_to"], "slots_per_sec": round(last["sps"], 2)},
        "sample": {"runs": len(cur), "blocks_sampled": s["sampled"], "blocks_produced": s["produced"],
                   "hours_covered": round(s["covered_s"] / 3600, 1), "tx_in_sample": s["tx"],
                   "per_run": SAMPLE_BLOCKS},
        "activity": {"tx_24h": tx_day, "tx_prev_24h": tx_prev if prev else None,
                     "tx_per_block": round(s["tpb"], 2), "wallets_24h": s["wallets"],
                     "wallets_prev_24h": sp["wallets"] if prev else None,
                     "blocks_per_day": round(s["blocks_per_day"]),
                     "hourly": hourly, "daily": daily},
        "programs": programs,
        "memo": {"mint": MEMO_MINT, "program": MEMO_PROGRAM, "site": "https://memo.rip",
                 "supply": memo["supply_ui"], "minters": minters, "holders": memo["holders"],
                 "mint_tx_24h": sum(mtx.values())},
        "tokens": {"active": active, "new": new},
        "links": {"bot": BOT_URL, "explorer": EXPLORER},
    }
    return data


def tweet(data: dict) -> str:
    a = data["activity"]
    top = ", ".join(p["name"] for p in data["programs"][:3]) or "n/a"
    trend = ""
    if a["tx_prev_24h"]:
        ch = (a["tx_24h"] - a["tx_prev_24h"]) / max(1, a["tx_prev_24h"]) * 100
        trend = f" ({ch:+.0f}% vs prev. 24h)"
    risky = len({t["mint"] for t in data["tokens"]["active"] + data["tokens"]["new"] if t["risk"] == "high"})
    return (f"X1 Pulse – last 24h (sampled)\n"
            f"• ~{a['tx_24h']:,} user transactions{trend}\n"
            f"• {a['wallets_24h']} active wallets seen\n"
            f"• Top apps: {top}\n"
            f"• MEMO mint tx in sample: {data['memo']['mint_tx_24h']}\n"
            f"• High-risk tokens flagged: {risky}\n"
            f"Check any X1 token: {BOT_URL}\n#X1 #XNT")


def publish() -> None:
    def git(*args):
        return subprocess.run(["git", "-C", str(HERE), *args], capture_output=True, text=True)

    git("add", "-A")
    if git("diff", "--cached", "--quiet").returncode == 0:
        log("nothing to publish")
        return
    has_head = git("rev-parse", "--verify", "HEAD").returncode == 0
    msg = f"X1 Pulse update {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC"
    r = git("commit", "--amend", "-q", "-m", msg) if has_head else git("commit", "-q", "-m", msg)
    if r.returncode:
        log(f"git commit failed: {r.stderr.strip()}")
        return
    r = git("push", "-q", "-f", "origin", "HEAD:main")
    log("published" if r.returncode == 0 else f"git push failed: {r.stderr.strip()}")


async def main() -> int:
    STATE.mkdir(exist_ok=True)
    runs = load(RUNS_FILE, [])
    cache = load(CACHE_FILE, {})
    rpc = PoliteRpc(RPC_URL, timeout=40)
    try:
        rec = await sample_run(rpc, runs, cache)
        runs.append(rec)
        runs = [r for r in runs if time.time() - r["t"] <= KEEP_S]
        save(RUNS_FILE, runs)
        log(f"slots {rec['slot_from']}-{rec['slot_to']}: produced {rec['produced']}, "
            f"sampled {rec['sampled']}, user tx {rec['tx']}, wallets {len(rec['wallets'])}")
        data = await build(rpc, runs, cache)
    except RpcError as e:
        log(f"RPC unavailable, skipping this run: {e}")
        return 1
    finally:
        save(CACHE_FILE, cache)
        await rpc.close()
    OUT_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=1))
    TWEET_FILE.write_text(tweet(data))
    log(f"data.json written: ~{data['activity']['tx_24h']:,} tx/24h, {data['activity']['wallets_24h']} wallets")
    if "--no-push" not in sys.argv:
        publish()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
