from datetime import datetime, timedelta
from typing import Callable, Optional, List, Dict

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger

from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import logger, AstrBotConfig
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent

import asyncio
import random
import aiohttp

class RandomTimeScheduler:
    def __init__(
        self,
        n: int,
        fn: Callable,
        max_instances: int = 1,
        misfire_grace_time: int = 60,
        start_hour: int = 0,
        end_hour: int = 23
    ):
        self.n = n
        self.fn = fn
        self.max_instances = max_instances
        self.misfire_grace_time = misfire_grace_time
        self.start_hour = start_hour
        self.end_hour = end_hour

        self.scheduler: Optional[AsyncIOScheduler] = None
        self._rescheduler_id = "daily_rescheduler"
        self._is_running = False
        self._current_date: Optional[datetime.date] = None

    @property
    def is_running(self) -> bool:
        """检查调度器是否正在运行"""
        return self._is_running and self.scheduler is not None and self.scheduler.running

    async def _wrapped_fn(self):
        """包装用户函数，捕获异常防止传播"""
        try:
            await self.fn()
        except Exception as e:
            logger.exception(f"任务执行出错: {e}")

    def _clear_daily_jobs(self):
        """清理当天的所有随机任务（保留 rescheduler）"""
        if not self.scheduler:
            return

        for job in self.scheduler.get_jobs():
            if job.id != self._rescheduler_id:
                try:
                    self.scheduler.remove_job(job.id)
                except Exception:
                    pass

    def _generate_random_times(self, date: datetime.date, earliest_time: Optional[datetime] = None) -> List[datetime]:
        """生成指定日期的 n 个随机时间点"""
        start_sec = self.start_hour * 3600
        end_sec = (self.end_hour + 1) * 3600 - 1

        # 如果指定了最早时间，调整 start_sec
        if earliest_time and earliest_time.date() == date:
            earliest_sec = earliest_time.hour * 3600 + earliest_time.minute * 60 + earliest_time.second
            start_sec = max(start_sec, earliest_sec + 1)  # 至少比当前时间晚1秒

        available_seconds = end_sec - start_sec + 1
        if self.n > available_seconds:
            raise ValueError(f"n ({self.n}) 超过了可用秒数 ({available_seconds})")

        random_seconds = sorted(random.sample(range(start_sec, end_sec + 1), self.n))
        base_time = datetime.combine(date, datetime.min.time())

        return [base_time + timedelta(seconds=int(s)) for s in random_seconds]

    def _schedule_day(self, date: Optional[datetime.date] = None, force_today: bool = False):
        if date is None:
            date = datetime.now().date()

        self._current_date = date
        self._clear_daily_jobs()

        now = datetime.now()
        times = self._generate_random_times(date, earliest_time=now if not force_today else None)
        scheduled = 0

        # 添加当天的任务
        for i, run_time in enumerate(times):
            # FIX 问题5: 如果已经过了时间，根据 force_today 决定是否延迟执行（而非跳过）
            if run_time <= now:
                if not force_today:
                    continue
                # force_today=True 时，过期任务安排在当前时间后5秒执行
                run_time = now + timedelta(seconds=5)
            
            job_id = f"random_task_{date.isoformat()}_{i:03d}"
            self.scheduler.add_job(
                self._wrapped_fn,
                trigger=DateTrigger(run_date=run_time),
                id=job_id,
                replace_existing=True,
                max_instances=self.max_instances,
                misfire_grace_time=self.misfire_grace_time
            )
            scheduled += 1

        logger.info(f"已安排 {scheduled} 个任务在 {date} 执行")
        logger.debug(f"时间点：{str(times)}")

        # 安排明天凌晨重新调度
        tomorrow = date + timedelta(days=1)
        next_run = datetime.combine(tomorrow, datetime.min.time()) + timedelta(seconds=1)

        self.scheduler.add_job(
            self._schedule_day,
            trigger=DateTrigger(run_date=next_run),
            args=(tomorrow, False),
            id=self._rescheduler_id,
            replace_existing=True,
            misfire_grace_time=3600
        )

    async def start(self, force_reschedule: bool = True) -> bool:
        if self.is_running:
            return False

        current_loop = asyncio.get_event_loop()
        if self.scheduler is None or getattr(self.scheduler, '_eventloop', None) != current_loop:
            if self.scheduler:
                try:
                    self.scheduler.shutdown(wait=False)
                except:
                    pass
            self.scheduler = AsyncIOScheduler()
            logger.info(f"创建新调度器实例，绑定到循环: {id(current_loop)}")

        self.scheduler.start()
        self._is_running = True

        if force_reschedule:
            self._schedule_day(force_today=False)
        return True

    async def pause(self) -> bool:
        """
        暂停调度器（保留所有任务，可 resume 恢复）
        不同于 stop，pause 后任务不会执行，但配置保留
        """
        if not self.is_running:
            logger.warning("调度器未在运行")
            return False

        self.scheduler.pause()
        self._is_running = False
        logger.info("调度器已暂停")
        return True

    async def stop(self, reset: bool = False, timeout: Optional[float] = None) -> bool:
        if not self.is_running and not (self.scheduler and self.scheduler.running):
            logger.warning("调度器未在运行")
            return False

        try:
            if reset:
                # 完全清理，下次 start 将重新创建 scheduler
                self.scheduler.shutdown(wait=timeout is not None, timeout=timeout)
                self.scheduler = None
                self._current_date = None
                self._is_running = False
                logger.info("调度器已停止并重置")
            else:
                # 只是 pause，保留 scheduler 状态
                self.scheduler.pause()
                self._is_running = False
                logger.info("调度器已停止（可恢复）")

            return True
        except Exception as e:
            logger.error(f"停止调度器时出错: {e}")
            return False

    async def restart(self, reset_schedule: bool = True) -> bool:
        """便捷方法：重启调度器"""
        await self.stop(reset=reset_schedule)
        await asyncio.sleep(0.1)  # 给一点清理时间
        return await self.start(force_reschedule=reset_schedule)

    def reschedule_today(self, force: bool = False):
        """
        手动重新生成今天的时间表（可在运行时调用）
        """
        if not self.is_running:
            logger.error("调度器未运行，无法重新安排")
            return

        self._schedule_day(force_today=force)
        logger.info("时间表已手动刷新")

    def get_today_schedule(self) -> List[str]:
        """获取今天的执行时间表"""
        if not self.scheduler:
            return []

        times = []
        for job in self.scheduler.get_jobs():
            if job.id != self._rescheduler_id and hasattr(job.trigger, 'run_date'):
                times.append(job.trigger.run_date.strftime('%H:%M:%S'))
        return sorted(times)

    def get_stats(self) -> dict:
        """获取当前状态统计"""
        if not self.scheduler:
            return {"running": False, "jobs": 0, "today_schedule": []}

        all_jobs = self.scheduler.get_jobs()
        task_jobs = [j for j in all_jobs if j.id != self._rescheduler_id]

        return {
            "running": self.is_running,
            "total_jobs": len(all_jobs),
            "pending_tasks": len(task_jobs),
            "current_date": self._current_date.isoformat() if self._current_date else None,
            "today_schedule": self.get_today_schedule()
        }

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop(reset=True)
        return False


class DailyRepeatingScheduler:
    """每日指定时间重复执行异步函数的调度器。"""
    def __init__(
        self,
        fn: Callable,
        time_str: str,
        repeat_count: int,
        job_id: Optional[str] = None,
        max_instances: int = 1,
        catch_exceptions: bool = True,
        delay_between_calls: float = 0
    ):
        if repeat_count < 1:
            raise ValueError("repeat_count 必须大于等于1")

        self.fn = fn
        self.repeat_count = repeat_count
        self.job_id = job_id or f"daily_{fn.__name__}_{id(self)}"
        self.max_instances = max_instances
        self.catch_exceptions = catch_exceptions
        self.delay = delay_between_calls

        self.scheduler = None
        self._lock = asyncio.Lock()
        self._running = False
        self._should_stop = False

        self.hour, self.minute, self.second = self._parse_time(time_str)

    def _parse_time(self, time_str: str) -> tuple[int, int, int]:
        sep = '-' if '-' in time_str else ':'
        parts = time_str.split(sep)

        if len(parts) != 3:
            raise ValueError(f"时间格式错误: {time_str}，应为 HH-MM-SS 或 HH:MM:SS")

        try:
            h, m, s = map(int, parts)
            if not (0 <= h < 24 and 0 <= m < 60 and 0 <= s < 60):
                raise ValueError
            return h, m, s
        except ValueError:
            raise ValueError(f"无效时间值: {time_str}")

    async def _execute_sequence(self):
        if self._lock.locked():
            logger.warning(f"[{self.job_id}] 上次执行尚未完成，跳过本次调度")
            return

        async with self._lock:
            if self._should_stop:
                logger.info(f"[{self.job_id}] 收到停止信号，取消执行")
                return
                
            logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                       f"{self.job_id} 开始执行（{self.repeat_count} 次）")

            for i in range(1, self.repeat_count + 1):
                if self._should_stop:
                    logger.info(f"[{self.job_id}] 执行被中断")
                    break
                    
                try:
                    await self.fn()
                    if i < self.repeat_count and self.delay > 0:
                        await asyncio.sleep(self.delay)
                except Exception as e:
                    if self.catch_exceptions:
                        logger.error(f"[{self.job_id}] 第 {i}/{self.repeat_count} 次执行失败: {e}")
                    else:
                        raise

            logger.info(f"[{self.job_id}] 本次调度执行完成")

    async def start(self):
        """启动调度器"""
        if self._running:
            return

        trigger = CronTrigger(
            hour=self.hour,
            minute=self.minute,
            second=self.second,
            day_of_week='*'
        )

        self.scheduler = AsyncIOScheduler()
        self.scheduler.add_job(
            self._execute_sequence,
            trigger=trigger,
            id=self.job_id,
            replace_existing=True,
            max_instances=self.max_instances,
            misfire_grace_time=3600
        )

        self.scheduler.start()
        self._running = True

        job = self.scheduler.get_job(self.job_id)
        if job and job.next_run_time:
            logger.info(f"[{self.job_id}] 已启动，将在每天 {self.hour:02d}:{self.minute:02d}:{self.second:02d} "
                       f"执行 {self.repeat_count} 次，下次执行: {job.next_run_time}")

    def stop(self):
        """停止调度器"""
        self._should_stop = True
        if self._running:
            self.scheduler.shutdown(wait=False)
            self._running = False
            logger.info(f"[{self.job_id}] 已停止")


class HitokotoClient:
    def __init__(self, cache_ttl: int = 300):
        self._session: Optional[aiohttp.ClientSession] = None
        self._cache_ttl = cache_ttl
        self._cache: Optional[Dict] = None
        self._cache_time: Optional[datetime] = None
        self._lock = asyncio.Lock()  # 防止并发请求击穿缓存

    async def _get_session(self) -> aiohttp.ClientSession:
        """延迟初始化，复用 Session"""
        if self._session is None or self._session.closed:
            # 设置连接池限制
            connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
            self._session = aiohttp.ClientSession(connector=connector)
        return self._session

    async def get_hitokoto(self, force_refresh: bool = False) -> str:
        """获取一言，带缓存"""
        if not force_refresh and self._cache and self._cache_time:
            if datetime.now() - self._cache_time < timedelta(seconds=self._cache_ttl):
                return self._format_result(self._cache)

        async with self._lock:  # 确保同时只有一个请求去调用 API
            if not force_refresh and self._cache and self._cache_time:
                if datetime.now() - self._cache_time < timedelta(seconds=self._cache_ttl):
                    return self._format_result(self._cache)

            url = "https://v1.hitokoto.cn/"
            try:
                session = await self._get_session()
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=10),
                    headers={'Accept': 'application/json'}
                ) as response:
                    response.raise_for_status()
                    data = await response.json()

                    # 更新缓存
                    self._cache = data
                    self._cache_time = datetime.now()

                    return self._format_result(data)

            except asyncio.TimeoutError:
                return "获取一言失败：请求超时"
            except aiohttp.ClientResponseError as e:
                if e.status == 429:
                    return "获取一言失败：请求过于频繁"
                return f"获取一言失败：服务异常（{e.status}）"
            except aiohttp.ClientConnectorError:
                return "获取一言失败：无法连接到服务器"
            except Exception as e:
                return f"获取一言失败：{str(e)}"

    def _format_result(self, data: dict) -> str:
        """格式化输出"""
        hitokoto = data.get("hitokoto", "这里应该有一句话...")
        from_who = data.get("from_who") or ""
        from_where = data.get("from") or "未知出处"

        if from_who:
            return f"{hitokoto} —— {from_who}《{from_where}》"
        return f"{hitokoto} —— 《{from_where}》"

    async def close(self):
        """清理资源，建议在程序结束时调用"""
        if self._session and not self._session.closed:
            try:
                await self._session.close()
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.warning(f"关闭 HitokotoClient session 时出错: {e}")
        self._session = None

    # 支持 async with 语法
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


@register("astrbot_plugin_group_autosign_utils", "MLSLi", "自动群打卡续火工具", "1.0.0")
class MyPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)

        self.bot_instance = None
        self.random_time_scheduler = None
        self.daily_scheduler = None
        self.hitokoto_client = None

        self._init_task: Optional[asyncio.Task] = None

        self.group_ids = []
        self.group_send_msg_idx_counter = 0
        self.group_autosign_idx_counter = 0

        self.success_count = 0
        self.failed_count = 0
        self.autosign_group_list_count = 0
        self.auto_send_msg_group_list_count = 0

        self._sign_lock = asyncio.Lock()

        self.results = {
            "not_in_group": "不在群聊内",
            "no_bot_instance": "无机器人实例",
            "ok": "完毕",
            "autosign_error": "打卡异常"
        }

        self.autosign_daily_time = config.get("autosign_daily_time", "00-00-00")
        self.auto_send_msg_random_time_range = config.get("auto_send_msg_random_time_range", "9,23").split(',')
        self.autosign_group_list = config.get("autosign_group_list", "").split(',')
        self.auto_send_msg_group_list = config.get("auto_send_msg_group_list", "").split(',')
        self.autosign_delay = config.get("autosign_delay", 2.0)

    def _filter_empty_str(self, _list):
        return [x for x in _list if x]

    async def _daily_auto_send_msg(self):
        if not self.auto_send_msg_group_list:
            logger.debug("自动续火列表为空，跳过执行")
            return
        
        if self.group_send_msg_idx_counter >= len(self.auto_send_msg_group_list):
            self.group_send_msg_idx_counter = 0
            
        if not self.bot_instance:
            logger.warning("Bot未就绪，跳过群续火")
            return

        try:
            group_id = int(self.auto_send_msg_group_list[self.group_send_msg_idx_counter])
        except (ValueError, IndexError) as e:
            logger.error(f"获取群号失败: {e}")
            self.group_send_msg_idx_counter += 1
            return

        hitokoto = await self.hitokoto_client.get_hitokoto()

        if group_id not in self.group_ids:
            logger.error(f"续火失败：不在群聊{group_id}内")
            self.group_send_msg_idx_counter += 1
            return

        try:
            ret = await self.bot_instance.send_group_msg(group_id=group_id, message=hitokoto)
            logger.debug(f"发送结果：{str(ret)}")
        except Exception as e:
            logger.error(f"发送群消息失败: {e}")
            
        self.group_send_msg_idx_counter += 1

        if self.group_send_msg_idx_counter >= len(self.auto_send_msg_group_list):
            self.group_send_msg_idx_counter = 0

    async def _daily_auto_sign(self):
        if not self.autosign_group_list:
            logger.debug("自动打卡列表为空，跳过执行")
            return
            
        if self.group_autosign_idx_counter >= len(self.autosign_group_list):
            self.group_autosign_idx_counter = 0
            
        if not self.bot_instance:
            logger.warning("Bot未就绪，跳过群打卡")
            return

        try:
            group_id = int(self.autosign_group_list[self.group_autosign_idx_counter])
        except (ValueError, IndexError) as e:
            logger.error(f"获取群号失败: {e}")
            self.group_autosign_idx_counter += 1
            self.failed_count += 1
            return

        if group_id not in self.group_ids:
            logger.warning(f"群打卡失败：不在群聊{group_id}中")
            self.group_autosign_idx_counter += 1
            self.failed_count += 1
            return

        ret, msg = await self._auto_sign_single(group_id)

        if msg == "ok":
            self.success_count += 1
            logger.debug(f"每日打卡结果成功：{group_id} -> {str(ret)}")
        else:
            self.failed_count += 1
            logger.debug(f"为群{group_id}打卡失败：{self.results[msg]}")

        self.group_autosign_idx_counter += 1

        if self.group_autosign_idx_counter >= len(self.autosign_group_list):
            logger.info(f"每日签到完毕，成功{self.success_count}个群组，失败{self.failed_count}个群组")
            self.group_autosign_idx_counter = 0
            self.success_count = 0
            self.failed_count = 0

    async def _init_random_time_scheduler(self):
        try:
            start, end = map(int, self.auto_send_msg_random_time_range)
        except ValueError:
            logger.error("自动续火时间范围格式错误，应为 '9,23' 格式")
            return

        if start > end or start < 0 or end < 0 or end > 23:
            logger.error("自动续火区间数值范围错误或数据不合法")
            return

        if self.auto_send_msg_group_list_count <= 0:
            logger.warning("没有要自动续火的群")
            return

        self.random_time_scheduler = RandomTimeScheduler(
            self.auto_send_msg_group_list_count, 
            self._daily_auto_send_msg, 
            start_hour=start, 
            end_hour=end
        )
        await self.random_time_scheduler.start()

    async def _init_daily_time_scheduler(self):
        if self.autosign_group_list_count <= 0:
            logger.warning("没有要自动打卡的群")
            return

        self.daily_scheduler = DailyRepeatingScheduler(
            self._daily_auto_sign, 
            self.autosign_daily_time, 
            self.autosign_group_list_count, 
            job_id='auto_sign', 
            delay_between_calls=self.autosign_delay
        )

        await self.daily_scheduler.start()

    async def _delayed_start(self):
        """延迟启动"""
        try:
            await asyncio.sleep(2)  # 给 AstrBot 完成初始化的时间
            await self._init_random_time_scheduler()
            await self._init_daily_time_scheduler()
            self.hitokoto_client = HitokotoClient(cache_ttl=120)
        except Exception as e:
            logger.exception(f"延迟启动失败: {e}")
            raise

    async def initialize(self):
        """可选择实现异步的插件初始化方法，当实例化该插件类之后会自动调用该方法。"""
        self.autosign_group_list = self._filter_empty_str(self.autosign_group_list)
        self.auto_send_msg_group_list = self._filter_empty_str(self.auto_send_msg_group_list)

        self.autosign_group_list_count = len(self.autosign_group_list)
        self.auto_send_msg_group_list_count = len(self.auto_send_msg_group_list)

        self._init_task = asyncio.create_task(self._delayed_start())
        
        def handle_task_exception(task):
            exc = task.exception()
            if exc is None:
                return
            if isinstance(exc, asyncio.CancelledError):
                return
            logger.exception(f"初始化任务异常: {exc}", exc_info=exc)
            
            self._init_task.add_done_callback(handle_task_exception)

        logger.info(f"插件加载完毕，配置了{self.autosign_group_list_count}个自动打卡群，{self.auto_send_msg_group_list_count}个自动续火群")

    @filter.event_message_type(filter.EventMessageType.ALL, priority=512)
    async def _get_bot_instance(self, event: AstrMessageEvent):
        if event.get_platform_name() == "aiocqhttp" and self.bot_instance is None and isinstance(event, AiocqhttpMessageEvent):
            self.bot_instance = event.bot
            logger.info("Bot实例已获取")

        if self.bot_instance and (not self.group_ids):
            try:
                group_list = await self.bot_instance.get_group_list()

                for group in group_list:
                    self.group_ids.append(group['group_id'])

                logger.info(f"加入群聊的id集合： {str(self.group_ids)}")
            except Exception as e:
                logger.error(f"无法列出群聊: {str(e)}")

    async def _auto_sign_single(self, group_id):  # group_id是int
        payloads = {
            "group_id": str(group_id)
        }

        if group_id not in self.group_ids:
            return None, "not_in_group"

        if not self.bot_instance:
            return None, "no_bot_instance"

        try:
            ret = await self.bot_instance.api.call_action('send_group_sign', **payloads)

            return ret, "ok"
        except Exception as e:
            logger.error(f"打卡失败：{str(e)}")
            return None, "autosign_error"

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("list_autosign_group")
    async def list_autosign_group(self, event: AstrMessageEvent):
        """列出自动签到的群聊"""
        yield event.plain_result(f"自动签到列表：{self.autosign_group_list}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("single_sign")
    async def single_sign(self, event: AstrMessageEvent):
        """单个群聊签到"""
        async with self._sign_lock:  # 哪些管理员闲的没事干一起打卡(bushi)
            args = event.message_str.strip().split()
            current_group_id = event.get_group_id()

            if len(args) < 2:
                if not current_group_id:
                    yield event.plain_result("请在群聊内使用该指令，或指定群号")
                    return
                group_id = current_group_id
            else:
                input_id = args[1]
                if not input_id.isdigit():
                    yield event.plain_result("请输入合规的群号（纯数字）")
                    return
                group_id = input_id

            try:
                ret, msg = await self._auto_sign_single(int(group_id))

                if msg != "ok":
                    logger.error(f"打卡失败：{self.results[msg]}")
                    yield event.plain_result(f"打卡失败：{self.results[msg]}")
                else:
                    logger.info(f"打卡成功：{str(ret)}")
                    yield event.plain_result("打卡成功")

            except Exception as e:
                logger.error(f"签到过程异常：{e}")
                yield event.plain_result(f"签到异常：{str(e)}")

    async def terminate(self):
        """可选择实现异步的插件销毁方法，当插件被卸载/停用时会调用。"""
        if self._init_task and not self._init_task.done():
            self._init_task.cancel()
            try:
                await self._init_task
            except asyncio.CancelledError:
                pass
        
        if self.random_time_scheduler:
            try:
                await self.random_time_scheduler.stop(reset=True)
            except Exception as e:
                logger.error(f"停止随机时间调度器失败: {e}")
                
        if self.daily_scheduler:
            try:
                self.daily_scheduler.stop()
            except Exception as e:
                logger.error(f"停止每日调度器失败: {e}")

        if self.hitokoto_client:
            try:
                await self.hitokoto_client.close()
            except Exception as e:
                logger.error(f"关闭 HitokotoClient 失败: {e}")

        await asyncio.sleep(0.5)

        self.random_time_scheduler = None
        self.daily_scheduler = None
        self._init_task = None