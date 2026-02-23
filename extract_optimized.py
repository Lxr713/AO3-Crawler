#!/usr/bin/env python3
"""
AO3 搜索页 work_id 提取脚本 - 优化版本

优化内容：
- 异步并发爬取（aiohttp）
- 指数退避重试机制
- 断点续传（checkpoint）
- HTTP 525/超时错误处理
- 优雅关闭（信号处理）
- 标准logging日志
"""

import asyncio
import aiohttp
import aiofiles
import re
import time
import json
import signal
import sys
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from dataclasses import dataclass


# ============ 日志配置 ============

def setup_logging() -> logging.Logger:
    """配置标准日志"""
    logger = logging.getLogger("ao3_crawler")
    logger.setLevel(logging.DEBUG)

    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    # 文件处理器
    file_handler = logging.FileHandler("crawler.log", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)

    # 格式
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


# 全局logger
logger = setup_logging()


# ============ 配置类 ============

@dataclass
class CrawlerConfig:
    """爬虫配置"""
    max_concurrent: int = 3
    max_retries: int = 5
    retry_base_delay: float = 1.0
    retry_max_delay: float = 60.0
    connection_timeout: int = 10
    read_timeout: int = 30
    page_delay: float = 0.5
    checkpoint_interval: int = 5
    checkpoint_file: str = "checkpoint.json"


# ============ 工具函数 ============

def build_page_url(base_url: str, page_num: int) -> str:
    """构造分页URL"""
    if '?' in base_url:
        return f"{base_url}&page={page_num}"
    else:
        return f"{base_url}?page={page_num}"


def extract_work_ids_from_html(html: str) -> List[str]:
    """从HTML中提取所有 work_id"""
    work_links = re.findall(r'href="/works/([0-9]+)"', html)
    return sorted(set(work_links), key=int)


# ============ 检查点管理 ============

class CheckpointManager:
    """检查点管理器 - 支持断点续传"""

    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.data: Dict[str, Any] = {
            "pages_completed": [],
            "work_ids": [],
            "current_page": 0,
            "start_time": None,
            "last_update": None,
        }

    async def load(self) -> bool:
        """加载检查点文件"""
        try:
            async with aiofiles.open(self.config.checkpoint_file, 'r', encoding='utf-8') as f:
                content = await f.read()
                loaded = json.loads(content)
                self.data.update(loaded)
                logger.info(f"[检查点] 已恢复: {len(self.data['pages_completed'])} 页完成, "
                            f"{len(self.data['work_ids'])} 个work_id")
                return True
        except FileNotFoundError:
            logger.info("[检查点] 未找到，从头开始")
            self.data["start_time"] = datetime.now().isoformat()
            return False
        except Exception as e:
            logger.warning(f"[检查点] 加载失败: {e}，从头开始")
            self.data["start_time"] = datetime.now().isoformat()
            return False

    async def save(self):
        """保存检查点"""
        self.data["last_update"] = datetime.now().isoformat()
        try:
            async with aiofiles.open(self.config.checkpoint_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(self.data, indent=2, ensure_ascii=False))
            logger.debug("[检查点] 已保存")
        except Exception as e:
            logger.error(f"[检查点] 保存失败: {e}")

    def add_page(self, page_num: int, work_ids: List[str]):
        """添加完成的页面"""
        if page_num not in self.data["pages_completed"]:
            self.data["pages_completed"].append(page_num)
        self.data["current_page"] = page_num
        existing = set(self.data["work_ids"])
        for wid in work_ids:
            if wid not in existing:
                self.data["work_ids"].append(wid)
                existing.add(wid)

    def is_completed(self, page_num: int) -> bool:
        return page_num in self.data["pages_completed"]

    def get_work_ids(self) -> List[str]:
        return self.data["work_ids"]

    def get_stats(self) -> Dict[str, Any]:
        return {
            "pages_completed": len(self.data["pages_completed"]),
            "total_work_ids": len(self.data["work_ids"]),
            "current_page": self.data["current_page"],
        }


# ============ 异步HTTP客户端 ============

class AsyncCrawler:
    """异步爬虫"""

    def __init__(self, config: CrawlerConfig, checkpoint: CheckpointManager):
        self.config = config
        self.checkpoint = checkpoint
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore: Optional[asyncio.Semaphore] = None
        self._shutdown = False

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit=10, limit_per_host=5,
            enable_cleanup_closed=True, force_close=True,
        )
        timeout = aiohttp.ClientTimeout(
            connect=self.config.connection_timeout,
            sock_read=self.config.read_timeout,
        )
        self.session = aiohttp.ClientSession(
            connector=connector, timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/120.0.0.0 Safari/537.36"},
        )
        self.semaphore = asyncio.Semaphore(self.config.max_concurrent)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def shutdown(self):
        self._shutdown = True

    def _calc_delay(self, attempt: int, is_429: bool = False, is_ssl: bool = False) -> float:
        """计算退避延迟"""
        if is_429:
            return min(self.config.retry_base_delay * (3 ** (attempt - 1)), self.config.retry_max_delay)
        elif is_ssl:
            return min(self.config.retry_base_delay * (2 ** (attempt - 1)) + (attempt * 2), self.config.retry_max_delay)
        else:
            return min(self.config.retry_base_delay * (2 ** (attempt - 1)), self.config.retry_max_delay)

    async def _fetch_one(self, url: str, page_num: int) -> Optional[str]:
        """获取单页，带重试"""
        for attempt in range(1, self.config.max_retries + 1):
            try:
                async with self.semaphore:
                    logger.debug(f"[页{page_num}] 尝试 {attempt}/{self.config.max_retries}: {url[:80]}...")
                    async with self.session.get(url) as resp:
                        if resp.status == 200:
                            logger.debug(f"[页{page_num}] 成功")
                            return await resp.text()

                        # 特定错误处理
                        if resp.status == 525:
                            raise aiohttp.ClientConnectorSSLError("HTTP 525 SSL Handshake Failed")
                        elif resp.status == 429:
                            raise aiohttp.ClientResponseError(
                                request_info=resp.request_info, history=resp.history,
                                status=429, message="Too Many Requests", headers=resp.headers
                            )
                        elif 500 <= resp.status < 600:
                            raise aiohttp.ClientResponseError(
                                request_info=resp.request_info, history=resp.history,
                                status=resp.status, message=f"Server Error {resp.status}", headers=resp.headers
                            )
                        else:
                            # 4xx 不重试
                            logger.warning(f"[页{page_num}] HTTP {resp.status}，不重试")
                            return None

            except (aiohttp.ClientConnectorSSLError, aiohttp.ClientConnectorError) as e:
                delay = self._calc_delay(attempt, is_ssl=True)
                logger.warning(f"[页{page_num}] SSL错误 (尝试{attempt}/{self.config.max_retries}): {e}")
                if attempt < self.config.max_retries:
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"[页{page_num}] SSL错误，放弃")
                    return None

            except aiohttp.ClientResponseError as e:
                is_429 = e.status == 429
                delay = self._calc_delay(attempt, is_429=is_429)
                logger.warning(f"[页{page_num}] HTTP {e.status} (尝试{attempt}/{self.config.max_retries})")
                if attempt < self.config.max_retries:
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"[页{page_num}] HTTP错误，放弃")
                    return None

            except asyncio.TimeoutError:
                delay = self._calc_delay(attempt)
                logger.warning(f"[页{page_num}] 超时 (尝试{attempt}/{self.config.max_retries})")
                if attempt < self.config.max_retries:
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"[页{page_num}] 超时，放弃")
                    return None

            except Exception as e:
                delay = self._calc_delay(attempt)
                logger.warning(f"[页{page_num}] 错误 {type(e).__name__} (尝试{attempt}/{self.config.max_retries})")
                if attempt < self.config.max_retries:
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"[页{page_num}] 错误，放弃")
                    return None

        return None

    async def crawl_pages(
        self, base_url: str, start_page: int, end_page: int
    ) -> List[str]:
        """爬取页面范围"""
        # 过滤已完成的页面
        pending = [p for p in range(start_page, end_page + 1)
                   if not self.checkpoint.is_completed(p)]

        if not pending:
            logger.info("所有页面已完成！")
            return self.checkpoint.get_work_ids()

        logger.info(f"[任务] 范围 {start_page}-{end_page}，跳过 {end_page-start_page+1-len(pending)}，"
                    f"待处理 {len(pending)}")

        async def process_one(page_num: int):
            """处理单个页面"""
            if self._shutdown:
                return page_num, None

            url = build_page_url(base_url, page_num)
            html = await self._fetch_one(url, page_num)

            if html is None:
                return page_num, None

            work_ids = extract_work_ids_from_html(html)
            self.checkpoint.add_page(page_num, work_ids)
            return page_num, work_ids

        # 创建任务
        tasks = [process_one(p) for p in pending]
        completed = 0
        failed_pages = []

        # 按完成顺序处理
        for coro in asyncio.as_completed(tasks):
            if self._shutdown:
                logger.info("[中断] 取消剩余任务...")
                for t in tasks:
                    if not t.done():
                        t.cancel()
                break

            try:
                page_num, work_ids = await coro

                if work_ids is None:
                    logger.error(f"[页{page_num}] 最终失败")
                    failed_pages.append(page_num)
                else:
                    logger.info(f"[页{page_num}] 成功，{len(work_ids)} 个work_id")
                    completed += 1

                    # 定期保存
                    if completed % self.config.checkpoint_interval == 0:
                        await self.checkpoint.save()
                        stats = self.checkpoint.get_stats()
                        logger.info(f"[检查点] 已保存 | {stats['pages_completed']} 页 | "
                                    f"{stats['total_work_ids']} 个work_id")

            except asyncio.CancelledError:
                logger.debug("[取消] 任务被取消")
            except Exception as e:
                logger.error(f"[错误] 处理异常: {type(e).__name__}: {e}")

        await self.checkpoint.save()

        logger.info("=" * 60)
        logger.info(f"批次完成: 成功 {completed} 页, 失败 {len(failed_pages)} 页")
        if failed_pages:
            logger.warning(f"  失败页码: {failed_pages}")
        stats = self.checkpoint.get_stats()
        logger.info(f"  累计: {stats['pages_completed']} 页, {stats['total_work_ids']} 个work_id")
        logger.info("=" * 60)

        return self.checkpoint.get_work_ids()


def setup_signal_handlers(crawler):
    """设置信号处理器"""
    def handler(signum, frame):
        sig_name = signal.Signals(signum).name
        logger.warning(f"[信号] 收到 {sig_name}，准备优雅关闭...")
        crawler.shutdown()
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


async def main():
    """主函数"""
    config = CrawlerConfig(
        max_concurrent=3,
        max_retries=5,
        retry_base_delay=1.0,
        retry_max_delay=60.0,
        connection_timeout=10,
        read_timeout=30,
        page_delay=0.5,
        checkpoint_interval=5,
    )

    search_url = 'https://archiveofourown.org/works/search?work_search%5Blanguage_id%5D=zh&commit=Search'
    start_page = 1
    end_page = 2000

    logger.info("=" * 60)
    logger.info("AO3 Work ID 提取器 - 优化版")
    logger.info("=" * 60)
    logger.info(f"搜索URL: {search_url}")
    logger.info(f"页码范围: {start_page} - {end_page}")
    logger.info(f"并发数: {config.max_concurrent}, 最大重试: {config.max_retries}")
    logger.info("=" * 60)

    checkpoint = CheckpointManager(config)
    await checkpoint.load()

    async with AsyncCrawler(config, checkpoint) as crawler:
        setup_signal_handlers(crawler)
        work_ids = await crawler.crawl_pages(search_url, start_page, end_page)

    # 保存结果
    async with aiofiles.open("work_ids.txt", 'w', encoding='utf-8') as f:
        for wid in work_ids:
            await f.write(f"{wid}\n")

    stats = checkpoint.get_stats()
    logger.info("\n" + "=" * 60)
    logger.info("提取完成!")
    logger.info(f"总页数: {stats['pages_completed']}, work_id: {stats['total_work_ids']}")
    logger.info("=" * 60)


if __name__ == '__main__':
    asyncio.run(main())
