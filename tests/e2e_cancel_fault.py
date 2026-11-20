"""端到端验证（HTTP）：取消卡住 / 取消报错两条回收路径。

前置：修复后的 API 在 BASE_URL 运行；可选 PSQL 做服务端核对。
覆盖：
  1) 正常商品查询基线；
  2) hang 注入：慢查询 1s 超时返回 504；取消卡住期间 /pool/stats 毫秒级
     响应、另一条连接上的商品查询照常；cancel_timeout 后旧连接被关闭并
     补位，槽位与连接数不越限；服务端无残留活跃查询；
  3) error 注入：取消报错即更换连接，行为同上；
  4) 关闭注入后普通查询全部恢复。
"""
from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time

import httpx

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
PSQL = os.environ.get("PSQL")
POOL_SIZE = int(os.environ.get("POOL_SIZE", "4"))
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}"
          + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(f"{name}: {detail}")


def server_count(where: str = "") -> int | None:
    if not PSQL:
        return None
    sql = ("SELECT count(*) FROM pg_stat_activity "
           "WHERE application_name = 'ro-api' " + where)
    try:
        out = subprocess.run(["psql", PSQL, "-tAc", sql],
                             capture_output=True, text=True, timeout=10)
        if out.returncode != 0:
            return None
        return int(out.stdout.strip())
    except Exception:
        return None


async def wait_idle(client: httpx.AsyncClient, timeout: float = 20) -> dict:
    deadline = time.monotonic() + timeout
    s = (await client.get("/pool/stats")).json()
    while time.monotonic() < deadline:
        if s["idle"] == POOL_SIZE and s["cleaning"] == 0 and s["waiting"] == 0:
            return s
        await asyncio.sleep(0.1)
        s = (await client.get("/pool/stats")).json()
    return s


async def probe_loop(client: httpx.AsyncClient, stop: asyncio.Event,
                     bucket: dict) -> None:
    """取消进行期间：反复取指标 + 跑商品查询，记录最大端到端延迟。"""
    while not stop.is_set():
        for coro in (
            client.get("/pool/stats"),
            client.post("/query/normal", json={"limit": 2}),
        ):
            t0 = time.monotonic()
            try:
                r = await coro
                dt = time.monotonic() - t0
                bucket["worst"] = max(bucket["worst"], dt)
                if r.status_code != 200:
                    bucket["errors"].append(r.status_code)
                else:
                    bucket["ok"] += 1
            except Exception as exc:  # noqa: BLE001
                bucket["errors"].append(repr(exc))
        await asyncio.sleep(0.05)


async def scenario(client: httpx.AsyncClient, mode: str) -> None:
    print(f"\n===== 场景：cancel fault = {mode} =====")
    before = (await client.get("/pool/stats")).json()
    await client.post("/debug/cancel-fault",
                      json={"mode": mode, "hang_seconds": 12})

    bucket = {"worst": 0.0, "ok": 0, "errors": []}
    stop = asyncio.Event()
    probe = asyncio.create_task(probe_loop(client, stop, bucket))

    t0 = time.monotonic()
    r = await client.post("/query/slow", json={
        # 8s：短于整体核对窗口，让“取消未送达时后端自行跑完”的路径
        # 也能在测试内被观察到收尾
        "delay_seconds": 8, "acquire_timeout": 5, "query_timeout": 1})
    dt = time.monotonic() - t0
    check(f"{mode}: 慢查询按 query_timeout 返回 504",
          r.status_code == 504, f"{r.status_code} {r.text}")
    check(f"{mode}: 504 耗时≈1s（不被清理拖住）", 0.8 < dt < 2.5, f"{dt:.2f}s")

    # 让回收路径走完（hang 时取消要等 cancel_timeout=3s，再关旧补新）
    s = await wait_idle(client)
    stop.set()
    await probe

    check(f"{mode}: 取消期间 stats/商品查询全部 200",
          not bucket["errors"], str(bucket["errors"]))
    check(f"{mode}: 取消期间服务保持毫秒级响应（最大延迟<1s）",
          bucket["worst"] < 1.0, f"worst={bucket['worst']:.2f}s")
    check(f"{mode}: 探测期间成功请求数充足", bucket["ok"] >= 10, str(bucket["ok"]))

    check(f"{mode}: 池槽位守恒",
          s["idle"] + s["in_use"] + s["cleaning"] == s["size"], str(s))
    check(f"{mode}: 打开连接数不越限",
          s["opened_connections"] <= s["size"], str(s))
    c0, c1 = before["counters"], s["counters"]
    check(f"{mode}: replaced 恰好 +1（旧连接关闭并补位）",
          c1["replaced"] - c0["replaced"] == 1,
          f"{c0['replaced']} -> {c1['replaced']}")
    check(f"{mode}: recycled 不增加（取消失败绝不复用旧连接）",
          c1["recycled"] == c0["recycled"],
          f"{c0['recycled']} -> {c1['recycled']}")
    if mode == "hang":
        check(f"{mode}: server_cancel_timeouts +1",
              c1["server_cancel_timeouts"] - c0["server_cancel_timeouts"] == 1,
              str(c1))
    else:
        check(f"{mode}: server_cancel_errors +1",
              c1["server_cancel_errors"] - c0["server_cancel_errors"] == 1,
              str(c1))

    # 故障注入下取消信号没有送达（hang 超时 / error 直接失败），旧连接
    # 已在客户端侧关闭；后端要等 pg_sleep 跑完、回到读循环时才发现对端
    # 断开。因此窗口内暂时存在 active 查询是预期行为（README 已知限制），
    # 关键是：客户端连接数不越限，且后端查询自行结束后无残留。
    n = server_count()
    if n is not None:
        check(f"{mode}: 服务端连接数 <= 池上限+临时取消连接（客户端视角已关）",
              n <= POOL_SIZE + 1, f"n={n}")

    # 等后端那条 pg_sleep(8) 自行结束并被服务端回收
    deadline = time.monotonic() + 12
    active2 = server_count("AND state = 'active'")
    while active2 and time.monotonic() < deadline:
        await asyncio.sleep(0.5)
        active2 = server_count("AND state = 'active'")
    if active2 is not None:
        check(f"{mode}: 旧查询自行结束后无残留活跃查询",
              active2 == 0, f"active={active2}")
    n2 = server_count()
    if n2 is not None:
        check(f"{mode}: 收尾后服务端连接数回到池上限", n2 == POOL_SIZE, f"n={n2}")

    await client.post("/debug/cancel-fault", json={"mode": ""})
    r = await client.post("/query/normal", json={"limit": 3})
    check(f"{mode}: 关闭注入后商品查询恢复 200",
          r.status_code == 200, r.text)


async def main() -> int:
    timeout = httpx.Timeout(30.0)
    async with httpx.AsyncClient(base_url=BASE, timeout=timeout) as client:
        r = await client.post("/query/normal", json={"limit": 3})
        check("基线: 商品查询 200 且返回 3 行",
              r.status_code == 200 and len(r.json()["rows"]) == 3, r.text)

        # 普通的查询超时（无故障）：取消成功 -> 回收复用
        before = (await client.get("/pool/stats")).json()
        r = await client.post("/query/slow", json={
            "delay_seconds": 10, "acquire_timeout": 5, "query_timeout": 1})
        check("无故障: 查询超时返回 504", r.status_code == 504, r.text)
        s = await wait_idle(client)
        c0, c1 = before["counters"], s["counters"]
        check("无故障: 取消成功并回收复用（recycled +1）",
              c1["recycled"] - c0["recycled"] == 1
              and c1["server_cancels"] - c0["server_cancels"] == 1, str(c1))

        await scenario(client, "hang")
        await scenario(client, "error")

        s = await wait_idle(client)
        check("收尾: 池完全恢复 idle=size",
              s["idle"] == POOL_SIZE and s["cleaning"] == 0, str(s))
        check("收尾: opened_connections == size",
              s["opened_connections"] == POOL_SIZE, str(s))
        print("\n最终计数器:", s["counters"])

    print()
    if failures:
        print(f"{len(failures)} FAILURE(S):")
        for f in failures:
            print(" -", f)
        return 1
    print("ALL E2E CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
