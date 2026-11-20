"""复现/回归：服务端取消调用阻塞或报错时，事件循环是否被拖住。

用法：
  MODE=hang|error HANG_SECONDS=8 python3 repro_cancel_hang.py /workspace/app

脚本直接加载目标目录里的 pool.py（基线或修复后均可），给
AsyncConnection.cancel 打补丁模拟同步阻塞 / 抛错。在一个慢查询超时进入
回收的整个窗口内，持续并发轮询 /pool/stats 等价指标并反复执行商品查询，
记录它们的最大端到端延迟与事件循环心跳卡顿。
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

sys.path.insert(0, sys.argv[1])

import psycopg  # noqa: E402
from pool import AsyncPool, PoolConfig  # noqa: E402

MODE = os.environ.get("MODE", "hang")
HANG_SECONDS = float(os.environ.get("HANG_SECONDS", "8"))
WINDOW = float(os.environ.get("WINDOW", "14"))

DSN = dict(host="127.0.0.1", port=5432, dbname="demo",
           user="app", password="app")

stalls: list[float] = []


async def heartbeat() -> None:
    last = time.monotonic()
    deadline = last + WINDOW
    while time.monotonic() < deadline:
        await asyncio.sleep(0.1)
        now = time.monotonic()
        delta = now - last
        last = now
        if delta > 0.25:
            stalls.append(delta)


async def main() -> int:
    pool = AsyncPool(PoolConfig(dsn=DSN, size=4))
    await pool.start()
    before = await pool.stats()

    # ---- 故障注入：模拟 libpq PQcancel 同步阻塞 / 报错 ----
    real_cancel = psycopg.AsyncConnection.cancel

    def injected_cancel(self):
        if MODE == "hang":
            print(f"[inject] conn.cancel() 同步阻塞 {HANG_SECONDS}s"
                  "（模拟 PQcancel 建连无响应）")
            time.sleep(HANG_SECONDS)
            return real_cancel(self)
        print("[inject] conn.cancel() 抛 OperationalError")
        raise psycopg.OperationalError("injected cancel error")

    psycopg.AsyncConnection.cancel = injected_cancel

    async def timed_slow() -> tuple[float, float]:
        """模拟 API 的查询超时路径：1s 超时后按脏连接归还。"""
        conn = await pool.acquire(5)

        async def q():
            async with conn.cursor() as cur:
                await cur.execute("SELECT pg_sleep(20)")

        task = asyncio.create_task(q())
        t0 = time.monotonic()
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=1)
        except asyncio.TimeoutError:
            pass
        await pool.release(conn, dirty=True, pending=task)
        return time.monotonic() - t0, t0

    async def stats_sampler() -> float:
        """窗口内反复取池指标，记录单次最大端到端延迟。
        计时包住 await 本身：loop 在 await 期间冻结的话，
        这一轮醒来后算出的延迟就包含整段冻结。"""
        worst = 0.0
        deadline = time.monotonic() + WINDOW
        while time.monotonic() < deadline:
            t0 = time.monotonic()
            await pool.stats()
            worst = max(worst, time.monotonic() - t0)
            t0 = time.monotonic()
            await asyncio.sleep(0.05)
            worst = max(worst, time.monotonic() - t0)
        return worst

    async def item_query_loop() -> tuple[float, int]:
        """窗口内反复在其他连接上执行商品查询，记录单次最大延迟。"""
        worst = 0.0
        ok = 0
        deadline = time.monotonic() + WINDOW
        while time.monotonic() < deadline:
            t0 = time.monotonic()
            conn = await pool.acquire(5)
            try:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT id, name FROM items LIMIT 3")
                    await cur.fetchall()
                ok += 1
            finally:
                await pool.release(conn, dirty=False)
            worst = max(worst, time.monotonic() - t0)
            t0 = time.monotonic()
            await asyncio.sleep(0.05)
            worst = max(worst, time.monotonic() - t0)
        return worst, ok

    hb = asyncio.create_task(heartbeat())
    sampler = asyncio.create_task(stats_sampler())
    items = asyncio.create_task(item_query_loop())
    slow = asyncio.create_task(timed_slow())

    slow_dt, _ = await slow
    stats_worst = await sampler
    item_worst, item_ok = await items
    hb.cancel()

    for _ in range(200):
        st = await pool.stats()
        if st["cleaning"] == 0 and st["idle"] == 4:
            break
        await asyncio.sleep(0.1)
    after = await pool.stats()

    psycopg.AsyncConnection.cancel = real_cancel
    await pool.close()

    print()
    print(f"mode                   : {MODE}")
    print(f"504 路径耗时            : {slow_dt:.2f}s（期望 ≈ 1s，不被清理拖累）")
    print(f"stats 轮询最大延迟       : {stats_worst:.2f}s（期望 < 0.3s）")
    print(f"商品查询最大延迟/成功数  : {item_worst:.2f}s / {item_ok} 次"
          f"（期望延迟 < 0.5s）")
    worst_stall = max(stalls, default=0.0)
    print(f"事件循环最大卡顿         : {worst_stall:.2f}s"
          f"（共 {len(stalls)} 次 >0.25s 的卡顿）")
    c0, c1 = before["counters"], after["counters"]
    print(f"计数变化                 : replaced +{c1['replaced']-c0['replaced']}, "
          f"recycled +{c1['recycled']-c0['recycled']}, "
          f"server_cancels +{c1['server_cancels']-c0['server_cancels']}")
    print(f"最终池状态               : idle={after['idle']} "
          f"cleaning={after['cleaning']} opened={after['opened_connections']}")
    frozen = (worst_stall >= HANG_SECONDS * 0.8
              or stats_worst > 2 or item_worst > 2)
    print("结论                   : "
          + ("事件循环被 cancel 阻塞 ❌" if frozen else "事件循环保持响应 ✅"))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
