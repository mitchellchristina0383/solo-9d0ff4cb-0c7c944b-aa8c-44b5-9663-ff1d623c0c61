"""一个语义显式的异步 PostgreSQL 连接池。

设计目标（详见 README）：

- 固定数量的“槽位”：每条连接始终属于且只属于一个槽位
  （idle / in_use / cleaning 三态之一），因此打开的连接数永远不会
  超过池上限；
- 可能仍在执行查询的连接（客户端超时、断开）或发生过 SQL 错误的
  连接，在确认会话状态已清理之前绝不会被再次借出；无法确认的一律
  关闭并更换新连接；
- 服务端取消（libpq 同步阻塞的 PQcancel）放在独立工作线程执行，
  事件循环不会被冻结；取消受 cancel_timeout 与外层 cleanup_timeout
  双重时限约束，超时或报错即关闭旧连接并补位；
- 排队中的请求不占用连接名额，被取消后立即离开等待队列。
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import threading
import time
from dataclasses import dataclass

import psycopg
from psycopg.pq import TransactionStatus

logger = logging.getLogger("pool")


class AcquireTimeoutError(Exception):
    """在请求的超时时间内没有可用连接。"""


class PoolClosedError(Exception):
    """连接池已关闭。"""


def _consume_task_exception(task: asyncio.Task) -> None:
    """取回被我们有意放弃的任务的异常，避免事件循环告警。"""
    if task.cancelled():
        return
    try:
        task.exception()
    except Exception:
        pass


@dataclass
class PoolConfig:
    dsn: dict
    size: int = 4
    connect_timeout: float = 5.0
    startup_retries: int = 60      # 启动时等待数据库就绪的重试次数
    retry_interval: float = 1.0
    cleanup_timeout: float = 10.0  # 清理单条连接的总预算
    cancel_timeout: float = 5.0    # 等待服务端取消返回的最长时间
    drain_grace: float = 5.0       # 等待被取消的查询收尾的时间
    close_timeout: float = 3.0
    # 空闲超过该秒数的连接，借出前先用 SELECT 1 验证活性
    # （防止数据库重启后把已死的空闲连接借给请求）；None 表示从不验证
    checkout_verify_after: float | None = 10.0
    checkout_verify_timeout: float = 3.0
    # 仅供演示/自测的故障注入："" 关闭；"hang" 让取消调用阻塞
    # cancel_fault_seconds 秒；"error" 让取消调用直接报错。
    cancel_fault: str = ""
    cancel_fault_seconds: float = 20.0


class AsyncPool:
    def __init__(self, config: PoolConfig):
        self.cfg = config
        self._cond = asyncio.Condition()
        # 空闲连接及其进入空闲的时刻（monotonic），用于借出前活性验证
        self._idle: list[tuple[psycopg.AsyncConnection, float]] = []
        self._in_use = 0    # 已借给请求
        self._cleaning = 0  # 正在清理/更换（不可借出）
        self._waiting = 0   # 正在 acquire() 中排队的请求数
        self._opened = 0    # 当前打开的连接数（<= size）
        self._closed = False
        self._bg: set[asyncio.Task] = set()
        self.counters: dict[str, int] = {
            "acquired": 0,           # 成功借出次数
            "acquire_timeouts": 0,   # 等连接超时次数
            "queue_cancelled": 0,    # 排队中客户端取消次数
            "released_clean": 0,     # 干净归还次数
            "recycled": 0,           # 脏连接清理后复用次数
            "replaced": 0,           # 脏连接废弃并更换次数
            "server_cancels": 0,     # 发送且成功返回的服务端取消次数
            "server_cancel_timeouts": 0,  # 服务端取消超过 cancel_timeout 的次数
            "server_cancel_errors": 0,    # 服务端取消调用报错的次数
            "query_timeouts": 0,     # 查询执行超时次数
            "client_disconnects": 0,  # 查询期间客户端断开次数
            "sql_errors": 0,         # SQL 报错次数
        }
        # 运行期故障注入开关（仅演示/自测用），取值 "" / "hang" / "error"，
        # 可由管理接口在不重启进程的情况下切换。
        self.cancel_fault = config.cancel_fault

    # ------------------------------------------------------------ 生命周期

    async def start(self) -> None:
        for _ in range(self.cfg.size):
            conn = await self._connect_with_retry(self.cfg.startup_retries)
            self._idle.append((conn, time.monotonic()))
        logger.info("pool started with %d connections", len(self._idle))

    async def close(self) -> None:
        self._closed = True
        for task in list(self._bg):
            task.cancel()
        async with self._cond:
            idle, self._idle = self._idle, []
            self._cond.notify_all()
        for conn, _ in idle:
            await self._close_quietly(conn)

    # -------------------------------------------------------- 借出 / 归还

    async def acquire(self, timeout: float | None) -> psycopg.AsyncConnection:
        """从池中取一条连接。

        在 `timeout` 秒内没有可用连接则抛出 AcquireTimeoutError
        （None 表示无限等待）。排队中的协程被取消时，其队列名额立即
        释放——排队请求从不持有连接名额。

        借出前如发现连接已死，或空闲时间过长且 SELECT 1 验证失败，
        则将其转入清理/更换并继续等下一条，不会把坏连接交给调用方。
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            conn, idle_since = await self._take_idle(deadline)
            if await self._verify_checkout(conn, idle_since):
                return conn
            # 验证失败：连接已按脏连接送清理/更换，在剩余预算内重试
            if deadline is not None and time.monotonic() >= deadline:
                self.counters["acquire_timeouts"] += 1
                raise AcquireTimeoutError(
                    f"no connection available within {timeout}s")

    async def _take_idle(
        self, deadline: float | None
    ) -> tuple[psycopg.AsyncConnection, float]:
        async with self._cond:
            self._waiting += 1
            try:
                while True:
                    if self._closed:
                        raise PoolClosedError("pool is closed")
                    if self._idle:
                        conn, idle_since = self._idle.pop()
                        self._in_use += 1
                        self.counters["acquired"] += 1
                        return conn, idle_since
                    if deadline is None:
                        await self._cond.wait()
                        continue
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self.counters["acquire_timeouts"] += 1
                        raise AcquireTimeoutError("no connection available")
                    try:
                        await asyncio.wait_for(self._cond.wait(), remaining)
                    except asyncio.TimeoutError:
                        self.counters["acquire_timeouts"] += 1
                        raise AcquireTimeoutError(
                            "no connection available") from None
            finally:
                self._waiting -= 1

    async def _verify_checkout(self, conn, idle_since: float) -> bool:
        """借出前的最后把关。本地状态免费检查总是做；空闲太久的连接
        额外做一次 SELECT 1 活性验证。"""
        if not self._looks_clean(conn):
            await self.release(conn, dirty=True)
            return False
        after = self.cfg.checkout_verify_after
        if after is None or time.monotonic() - idle_since < after:
            return True
        try:
            async with conn.cursor() as cur:
                await asyncio.wait_for(
                    cur.execute("SELECT 1"), self.cfg.checkout_verify_timeout)
        except Exception:
            logger.info("checkout verify failed; recycling connection")
            await self.release(conn, dirty=True)
            return False
        if self._looks_clean(conn):
            return True
        await self.release(conn, dirty=True)
        return False

    async def release(
        self,
        conn: psycopg.AsyncConnection,
        *,
        dirty: bool,
        pending: asyncio.Task | None = None,
    ) -> None:
        """归还 acquire() 借出的连接。

        干净的连接直接回到空闲队列；脏连接（查询超时 / 客户端断开 /
        SQL 报错）转入后台清理，只有在会话状态被验证干净后才可再次
        借出，无法验证的关闭并更换。本函数不会在清理上阻塞调用方。
        """
        if pending is not None:
            pending.add_done_callback(_consume_task_exception)
        if not dirty and self._looks_clean(conn):
            async with self._cond:
                self._in_use -= 1
                self._idle.append((conn, time.monotonic()))
                self.counters["released_clean"] += 1
                self._cond.notify(1)
            return
        async with self._cond:
            self._in_use -= 1
            self._cleaning += 1
        self.spawn(self._recycle(conn, pending))

    # ------------------------------------------------------------ 内部实现

    def _looks_clean(self, conn: psycopg.AsyncConnection) -> bool:
        try:
            return (
                not conn.closed
                and not getattr(conn, "broken", False)
                and conn.info.transaction_status == TransactionStatus.IDLE
            )
        except Exception:
            return False

    def spawn(self, coro) -> asyncio.Task:
        """在池的后台任务集中运行一个协程（供清理与演示任务使用）。"""
        task = asyncio.create_task(coro)
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)
        task.add_done_callback(_consume_task_exception)
        return task

    async def _recycle(self, conn, pending) -> None:
        try:
            ok = await self._sanitize(conn, pending)
        except Exception:
            logger.exception("sanitize crashed")
            ok = False
        if ok:
            reusable = conn
            self.counters["recycled"] += 1
            logger.info("dirty connection cleaned and reused")
        else:
            # 无法确认会话干净（取消超时/报错、查询迟迟不收尾等）：
            # 先取消仍在等待该连接的查询任务。Task.cancel() 会同步把
            # 任务从连接 socket 的读回调上摘除——否则残留的读回调会让
            # psycopg 的 close() 停在 CLOSE_WAIT、socket 得不到释放，
            # 服务端也会一直挂着这条后端连接。注意这里绝不 await 该
            # 任务：它的取消路径仍会操作这条即将关闭的连接，等待会与
            # close() 互相阻塞；任务异常由 release() 注册的回调消费。
            if pending is not None and not pending.done():
                pending.cancel()
            await self._close_quietly(conn)
            self.counters["replaced"] += 1
            if self._closed:
                reusable = None
            else:
                logger.info("connection discarded; opening a replacement")
                reusable = await self._connect_with_retry(0)  # 0 = 一直重试
        async with self._cond:
            self._cleaning -= 1
            if reusable is not None:
                self._idle.append((reusable, time.monotonic()))
            self._cond.notify(1)

    async def _sanitize(self, conn, pending) -> bool:
        """确认连接的会话状态已清理才返回 True。"""
        if conn.closed or getattr(conn, "broken", False):
            return False
        try:
            return await asyncio.wait_for(
                self._sanitize_inner(conn, pending), self.cfg.cleanup_timeout
            )
        except Exception:
            logger.warning("sanitize failed", exc_info=True)
            return False

    async def _sanitize_inner(self, conn, pending) -> bool:
        # 1) 查询可能仍在服务端执行：发送服务端取消，并等待客户端侧的
        #    查询任务观察到取消结果。取消调用本身是 libpq 的同步阻塞
        #    PQcancel()（要新开一条 TCP 连接发送取消信号），因此放到
        #    独立线程里执行，绝不能堵住事件循环；它还受独立的
        #    cancel_timeout 与外层 cleanup_timeout 双重时限约束。
        still_running = pending is not None and not pending.done()
        if still_running or conn.info.transaction_status == TransactionStatus.ACTIVE:
            if not await self._server_cancel(conn):
                # 取消超时或报错：即便旧查询之后自行结束、会话仍能响应，
                # 也无法信任这次取消语义，按约定直接判定不可复用
                # （_recycle 会关掉旧连接并补位一条新连接）。
                return False
        if still_running:
            try:
                await asyncio.wait_for(asyncio.shield(pending), self.cfg.drain_grace)
            except Exception:
                pass
            if not pending.done():
                return False  # 查询不肯结束 -> 无法信任该会话
            # 取消成功后也必须等查询真正收尾，再检查会话是否干净，
            # 避免在连接还处于 ACTIVE / 失败事务状态时误判可复用。
        # 2) 回滚任何遗留 / 失败的事务。
        status = conn.info.transaction_status
        if status in (TransactionStatus.INTRANS, TransactionStatus.INERROR):
            await conn.rollback()
        # 3) 验证会话可用且处于空闲。
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")
            row = await cur.fetchone()
        return (
            row is not None
            and tuple(row) == (1,)
            and conn.info.transaction_status == TransactionStatus.IDLE
        )

    async def _server_cancel(self, conn) -> bool:
        """发送服务端取消。成功返回 True；超时或报错返回 False。

        psycopg 的 ``AsyncConnection.cancel()`` 内部是 libpq 的同步
        ``PQcancel()``：它会另开一条 TCP 连接把取消信号发给服务器，
        对端无响应时该调用会阻塞到 TCP 建连超时（分钟级）。直接在事件
        循环线程里调用会冻结整个服务（``/pool/stats`` 和其他连接上的
        请求一起停住），且 ``asyncio.wait_for`` 也无法中断正在执行的
        同步 C 调用。因此这里把整段取消放到独立工作线程中执行，主循环
        只在一个 future 上等待，``cancel_timeout`` 到点就能立刻返回。
        """
        fault = self.cancel_fault
        if fault == "error":
            logger.warning("injected server-cancel error")
            self.counters["server_cancel_errors"] += 1
            return False

        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()

        def _consume(fut: asyncio.Future) -> None:
            # 超时后工作线程仍可能晚些把异常送回这个已被放弃的 future，
            # 提前消费掉，避免事件循环的“未取异常”告警。
            if not fut.cancelled():
                try:
                    fut.exception()
                except Exception:
                    pass

        future.add_done_callback(_consume)

        def _work() -> None:
            try:
                if fault == "hang":
                    # 故障注入：线程内同步睡眠模拟 PQcancel 长时间不返回。
                    # 只阻塞本工作线程，事件循环照常运转。
                    time.sleep(self.cfg.cancel_fault_seconds)
                    raise psycopg.OperationalError(
                        "injected cancel hang: deadline exceeded")
                result = conn.cancel()
                if inspect.isawaitable(result):
                    # 兼容早期 psycopg 3.2：cancel() 是协程。协程在被
                    # await 时才取运行中的事件循环，因此在本线程新建一个
                    # 临时循环来驱动它，不触碰主循环。
                    inner = asyncio.new_event_loop()
                    try:
                        inner.run_until_complete(result)
                    finally:
                        inner.close()
            except Exception as exc:  # noqa: BLE001 - 跨线程回传
                if not future.done():
                    loop.call_soon_threadsafe(future.set_exception, exc)
            else:
                if not future.done():
                    loop.call_soon_threadsafe(future.set_result, None)

        threading.Thread(
            target=_work, name="pg-server-cancel", daemon=True).start()
        try:
            await asyncio.wait_for(
                asyncio.shield(future), self.cfg.cancel_timeout)
        except asyncio.TimeoutError:
            self.counters["server_cancel_timeouts"] += 1
            logger.warning(
                "server-side cancel did not return within %.1fs; "
                "the connection will be closed and replaced",
                self.cfg.cancel_timeout)
            return False
        except Exception:
            self.counters["server_cancel_errors"] += 1
            logger.warning(
                "server-side cancel failed; the connection will be "
                "closed and replaced", exc_info=True)
            return False
        self.counters["server_cancels"] += 1
        return True

    async def _close_quietly(self, conn) -> None:
        try:
            await asyncio.wait_for(conn.close(), self.cfg.close_timeout)
        except Exception:
            logger.warning("error while closing connection", exc_info=True)
        finally:
            self._opened -= 1

    async def _connect(self) -> psycopg.AsyncConnection:
        conn = await psycopg.AsyncConnection.connect(
            **self.cfg.dsn,
            autocommit=True,
            application_name="ro-api",
            connect_timeout=int(self.cfg.connect_timeout),
            # 会话级只读：本 API 是只读查询服务
            options="-c default_transaction_read_only=on",
        )
        self._opened += 1
        return conn

    async def _connect_with_retry(self, retries: int) -> psycopg.AsyncConnection:
        """retries <= 0 表示一直重试。"""
        attempt = 0
        while True:
            try:
                return await self._connect()
            except Exception:
                attempt += 1
                if 0 < retries <= attempt:
                    raise
                logger.warning("connect failed (attempt %d), retrying", attempt)
                await asyncio.sleep(self.cfg.retry_interval)

    # ------------------------------------------------------------ 指标

    async def stats(self) -> dict:
        async with self._cond:
            return {
                "size": self.cfg.size,
                "idle": len(self._idle),
                "in_use": self._in_use,
                "cleaning": self._cleaning,
                "waiting": self._waiting,
                "opened_connections": self._opened,
                "cancel_fault": self.cancel_fault,
                "counters": dict(self.counters),
            }
