"""验证 _server_cancel 对“协程版 cancel()”（psycopg 3.2 早期形态）的兼容：
协程在工作线程内的临时事件循环中被驱动，主事件循环不阻塞。"""
from __future__ import annotations

import asyncio
import inspect
import sys
import time

sys.path.insert(0, "/workspace/app")

import psycopg  # noqa: E402
from pool import AsyncPool, PoolConfig  # noqa: E402

DSN = dict(host="127.0.0.1", port=5432, dbname="demo",
           user="app", password="app")


async def main() -> int:
    pool = AsyncPool(PoolConfig(dsn=DSN, size=2, cancel_timeout=5))
    await pool.start()

    real_cancel = psycopg.AsyncConnection.cancel
    calls: list[str] = []

    async def coro_cancel(self):
        # 协程版：被 await 时才取运行中的事件循环
        loop = asyncio.get_running_loop()
        calls.append(loop is main_loop)
        return real_cancel(self)

    main_loop = asyncio.get_running_loop()
    psycopg.AsyncConnection.cancel = coro_cancel
    assert inspect.iscoroutinefunction(psycopg.AsyncConnection.cancel)

    # 取一条连接，制造一个仍在运行的查询，让回收走到服务端取消
    conn = await pool.acquire(5)

    async def q():
        async with conn.cursor() as cur:
            await cur.execute("SELECT pg_sleep(10)")

    task = asyncio.create_task(q())
    await asyncio.sleep(0.3)

    t0 = time.monotonic()
    ok = await pool._server_cancel(conn)
    dt = time.monotonic() - t0
    print(f"coroutine-style cancel -> ok={ok}, dt={dt:.2f}s")
    print("cancel 协程所在循环是否为主循环:", calls)

    await pool.release(conn, dirty=True, pending=task)
    for _ in range(100):
        s = await pool.stats()
        if s["cleaning"] == 0 and s["idle"] == 2:
            break
        await asyncio.sleep(0.1)
    s = await pool.stats()
    psycopg.AsyncConnection.cancel = real_cancel
    await pool.close()

    assert ok is True, "协程版 cancel 应当成功"
    assert dt < 3, f"取消耗时异常: {dt}"
    assert calls == [False], f"协程必须在工作线程的临时循环里驱动: {calls}"
    assert s["idle"] == 2 and s["cleaning"] == 0, str(s)
    assert s["counters"]["server_cancels"] >= 1, str(s["counters"])
    print("OK: 协程版 cancel 在工作线程临时循环中被正确驱动，连接正常回收")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
