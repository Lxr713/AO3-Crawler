#!/usr/bin/env python3
"""
AO3 批量文章爬虫 - 优化版本

优化内容：
- 异步并发爬取（aiohttp）
- 从 checkpoint.json 读取 work_id 列表
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
import json
import signal
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any, Set
from dataclasses import dataclass, asdict


# ============ 日志配置 ============

def setup_logging() -> logging.Logger:
    """配置标准日志"""
    logger = logging.getLogger("batch_crawler")
    logger.setLevel(logging.DEBUG)

    # 避免重复添加handler
    if logger.handlers:
        return logger

    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    # 文件处理器
    file_handler = logging.FileHandler("batch_crawler.log", encoding="utf-8")
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
    max_concurrent: int = 3          # 最大并发数
    max_retries: int = 5             # 最大重试次数
    retry_base_delay: float = 1.0    # 基础延迟（秒）
    retry_max_delay: float = 60.0    # 最大延迟（秒）
    connection_timeout: int = 10     # 连接超时（秒）
    read_timeout: int = 30           # 读取超时（秒）
    work_delay: float = 1.0          # 作品间延迟（秒）
    checkpoint_interval: int = 5     # 每N个作品保存检查点
    checkpoint_file: str = "batch_checkpoint.json"
    input_checkpoint_file: str = "checkpoint.json"  # 从extract_optimized.py获取


# ============ 检查点管理 ============

class CheckpointManager:
    """检查点管理器 - 支持断点续传"""

    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.data: Dict[str, Any] = {
            "works_completed": [],       # 已完成的作品ID
            "works_failed": {},          # 失败的作品ID: 错误信息
            "work_ids": [],              # 所有待处理的作品ID
            "current_index": 0,          # 当前处理位置
            "start_time": None,
            "last_update": None,
            "total_works": 0,
        }

    async def load(self) -> bool:
        """加载检查点文件"""
        checkpoint_path = Path(self.config.checkpoint_file)
        if not checkpoint_path.exists():
            logger.info("[检查点] 未找到，从头开始")
            self.data["start_time"] = datetime.now().isoformat()
            return False

        try:
            async with aiofiles.open(checkpoint_path, 'r', encoding='utf-8') as f:
                content = await f.read()
                loaded = json.loads(content)
                self.data.update(loaded)
                logger.info(f"[检查点] 已恢复: {len(self.data['works_completed'])}/{self.data['total_works']} 作品完成, "
                            f"{len(self.data['works_failed'])} 失败")
                return True
        except Exception as e:
            logger.warning(f"[检查点] 加载失败: {e}，从头开始")
            self.data["start_time"] = datetime.now().isoformat()
            return False

    async def load_input_checkpoint(self, input_file: str) -> bool:
        """从extract_optimized.py的检查点加载work_id列表"""
        input_path = Path(input_file)
        if not input_path.exists():
            logger.error(f"[输入] 找不到检查点文件: {input_file}")
            logger.error("请先运行 extract_optimized.py 生成检查点")
            return False

        try:
            with open(input_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            work_ids = data.get('work_ids', [])
            if not work_ids:
                logger.error(f"[输入] 检查点中没有work_id")
                return False

            # 合并到当前数据
            existing = set(self.data['work_ids'])
            new_ids = [wid for wid in work_ids if wid not in existing]

            self.data['work_ids'].extend(new_ids)
            self.data['total_works'] = len(self.data['work_ids'])

            logger.info(f"[输入] 从 {input_file} 加载了 {len(new_ids)} 个新work_id")
            logger.info(f"[输入] 总work_id数量: {self.data['total_works']}")
            return True

        except Exception as e:
            logger.error(f"[输入] 加载检查点失败: {e}")
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

    def add_completed(self, work_id: str):
        """添加完成的作品"""
        if work_id not in self.data["works_completed"]:
            self.data["works_completed"].append(work_id)
        # 从失败列表中移除（如果之前失败过）
        if work_id in self.data["works_failed"]:
            del self.data["works_failed"][work_id]

    def add_failed(self, work_id: str, error_msg: str):
        """添加失败的作品"""
        self.data["works_failed"][work_id] = {
            "error": error_msg,
            "timestamp": datetime.now().isoformat()
        }

    def is_completed(self, work_id: str) -> bool:
        """检查作品是否已完成"""
        return work_id in self.data["works_completed"]

    def is_failed(self, work_id: str) -> bool:
        """检查作品是否已标记为失败"""
        return work_id in self.data["works_failed"]

    def get_pending_works(self) -> List[str]:
        """获取待处理的作品列表"""
        all_ids = set(self.data['work_ids'])
        completed = set(self.data['works_completed'])
        failed = set(self.data['works_failed'].keys())
        pending = all_ids - completed - failed
        return list(pending)

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "total_works": self.data["total_works"],
            "completed": len(self.data["works_completed"]),
            "failed": len(self.data["works_failed"]),
            "pending": len(self.get_pending_works()),
        }


# ============ 主爬虫类 ============

class BatchCrawler:
    """批量爬虫"""

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

    async def _fetch_work(self, work_id: str) -> Optional[str]:
        """获取单个作品，带重试"""
        url = f"https://archiveofourown.org/works/{work_id}?view_full_work=true"

        for attempt in range(1, self.config.max_retries + 1):
            try:
                async with self.semaphore:
                    logger.debug(f"[{work_id}] 尝试 {attempt}/{self.config.max_retries}")
                    async with self.session.get(url) as resp:
                        if resp.status == 200:
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
                            logger.warning(f"[{work_id}] HTTP {resp.status}，不重试")
                            return None

            except (aiohttp.ClientConnectorSSLError, aiohttp.ClientConnectorError) as e:
                delay = self._calc_delay(attempt, is_ssl=True)
                logger.warning(f"[{work_id}] SSL错误 (尝试{attempt}/{self.config.max_retries}): {e}")
                if attempt < self.config.max_retries:
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"[{work_id}] SSL错误，放弃")
                    return None

            except aiohttp.ClientResponseError as e:
                is_429 = e.status == 429
                delay = self._calc_delay(attempt, is_429=is_429)
                logger.warning(f"[{work_id}] HTTP {e.status} (尝试{attempt}/{self.config.max_retries})")
                if attempt < self.config.max_retries:
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"[{work_id}] HTTP错误，放弃")
                    return None

            except asyncio.TimeoutError:
                delay = self._calc_delay(attempt)
                logger.warning(f"[{work_id}] 超时 (尝试{attempt}/{self.config.max_retries})")
                if attempt < self.config.max_retries:
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"[{work_id}] 超时，放弃")
                    return None

            except Exception as e:
                delay = self._calc_delay(attempt)
                logger.warning(f"[{work_id}] 错误 {type(e).__name__} (尝试{attempt}/{self.config.max_retries})")
                if attempt < self.config.max_retries:
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"[{work_id}] 错误，放弃")
                    return None

        return None

    async def crawl_works(self) -> List[Dict[str, Any]]:
        """爬取所有待处理的作品"""
        pending = self.checkpoint.get_pending_works()

        if not pending:
            logger.info("所有作品已完成！")
            return []

        stats = self.checkpoint.get_stats()
        logger.info(f"[任务] 总计 {stats['total_works']}, 已完成 {stats['completed']}, "
                    f"失败 {stats['failed']}, 待处理 {len(pending)}")

        results = []
        completed = 0

        # 创建任务
        tasks = [self._process_work(wid) for wid in pending]

        # 按完成顺序处理
        for coro in asyncio.as_completed(tasks):
            if self._shutdown:
                logger.info("[中断] 取消剩余任务...")
                for t in tasks:
                    if not t.done():
                        t.cancel()
                break

            try:
                work_id, work_data, status = await coro

                if status == "success":
                    logger.info(f"[{work_id}] ✓ 成功: {work_data.get('title', 'Unknown')}")
                    results.append(work_data)
                    completed += 1
                elif status == "interrupted":
                    logger.warning(f"[{work_id}] ⚠ 被中断")
                else:
                    logger.error(f"[{work_id}] ✗ 失败: {status}")

                # 定期保存
                if completed % self.config.checkpoint_interval == 0:
                    await self.checkpoint.save()
                    stats = self.checkpoint.get_stats()
                    logger.info(f"[检查点] 已保存 | 完成 {stats['completed']}/{stats['total_works']}")

            except asyncio.CancelledError:
                logger.debug("[取消] 任务被取消")
            except Exception as e:
                logger.error(f"[错误] 处理异常: {type(e).__name__}: {e}")

        await self.checkpoint.save()

        # 生成汇总
        await self._generate_summary(results)

        return results

    async def _process_work(self, work_id: str) -> tuple[str, Optional[Dict[str, Any]], str]:
        """处理单个作品"""
        if self._shutdown:
            return work_id, None, "interrupted"

        try:
            logger.info(f"[{work_id}] 开始爬取...")
            html = await self._fetch_work(work_id)

            if html is None:
                self.checkpoint.add_failed(work_id, "fetch_failed")
                return work_id, None, "fetch_failed"

            work_data = self._parse_work(html, work_id)

            if work_data is None:
                self.checkpoint.add_failed(work_id, "parse_failed")
                return work_id, None, "parse_failed"

            # 保存到文件
            output_file = f"ao3_{work_id}.json"
            async with aiofiles.open(output_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(work_data, ensure_ascii=False, indent=2))

            self.checkpoint.add_completed(work_id)
            logger.info(f"[{work_id}] 已保存: {output_file}")
            return work_id, work_data, "success"

        except Exception as e:
            logger.error(f"[{work_id}] 处理异常: {e}")
            self.checkpoint.add_failed(work_id, str(e))
            return work_id, None, f"error: {e}"

    async def _generate_summary(self, results: List[Dict[str, Any]]):
        """生成汇总报告"""
        stats = self.checkpoint.get_stats()

        summary = {
            'total': stats['total_works'],
            'success': len(results),
            'failed': stats['failed'],
            'completed_ids': self.checkpoint.data['works_completed'],
            'failed_details': self.checkpoint.data['works_failed'],
            'timestamp': datetime.now().isoformat(),
        }

        async with aiofiles.open('batch_summary.json', 'w', encoding='utf-8') as f:
            await f.write(json.dumps(summary, ensure_ascii=False, indent=2))

        logger.info("=" * 60)
        logger.info("批次完成!")
        logger.info(f"  总计: {stats['total_works']}")
        logger.info(f"  成功: {len(results)}")
        logger.info(f"  失败: {stats['failed']}")
        if stats['total_works'] > 0:
            logger.info(f"  完成率: {len(results)/stats['total_works']*100:.1f}%")
        logger.info("=" * 60)

    def _parse_work(self, html: str, work_id: str) -> Optional[Dict[str, Any]]:
        """解析作品数据"""
        try:
            # 提取标题
            title_match = re.search(
                r'<h2[^>]*class="[^"]*title[^"]*"[^>]*>(.*?)</h2>',
                html, re.DOTALL | re.IGNORECASE
            )
            title = title_match.group(1).strip() if title_match else "未知标题"
            title = re.sub(r'<[^>]+>', '', title).strip()

            # 提取作者
            author = "未知作者"
            author_patterns = [
                r'<a[^>]*class="[^"]*author[^"]*"[^>]*>([^<]+)</a>',
                r'<a[^>]*"rel="author"[^>]*>([^<]+)</a>',
                r'by\s*<a[^>]*>([^<]+)</a>',
            ]
            for pattern in author_patterns:
                author_match = re.search(pattern, html, re.IGNORECASE)
                if author_match:
                    author = author_match.group(1).strip()
                    break

            # 章节数
            total_chapters = 1
            chapter_match = re.search(r'(\d+)/(\d+)', html)
            if chapter_match:
                total_chapters = int(chapter_match.group(2))

            # 解析章节
            chapters = self._parse_chapters(html)

            return {
                'work_id': work_id,
                'url': f"https://archiveofourown.org/works/{work_id}",
                'title': title,
                'author': author,
                'total_chapters': total_chapters,
                'chapters_fetched': len(chapters),
                'chapters': chapters
            }
        except Exception as e:
            logger.error(f"[{work_id}] 解析失败: {e}")
            return None

    def _parse_chapters(self, html: str) -> List[Dict[str, Any]]:
        """解析章节内容"""
        chapters = []
        chapter_areas = []

        for match in re.finditer(r'id="chapter-([0-9]+)"', html, re.IGNORECASE):
            chapter_id = match.group(1)
            start_pos = max(0, match.start() - 1000)
            end_pos = min(len(html), match.end() + 15000)
            section = html[start_pos:end_pos]
            chapter_areas.append((chapter_id, section))

        if chapter_areas:
            for chapter_id, section in chapter_areas:
                chapter_title = f'Chapter {chapter_id}'
                title_patterns = [
                    r'<h3[^>]*>(.*?)</h3>',
                    r'<h4[^>]*>(.*?)</h4>',
                ]
                for pattern in title_patterns:
                    title_match = re.search(pattern, section, re.DOTALL | re.IGNORECASE)
                    if title_match:
                        raw_title = title_match.group(1)
                        clean_title = re.sub(r'<[^>]+>', '', raw_title).strip()
                        if clean_title:
                            chapter_title = clean_title
                            break

                userstuff_match = re.search(
                    r'<div[^>]*class="[^"]*userstuff[^"]*"[^>]*>(.*?)</div>',
                    section, re.DOTALL | re.IGNORECASE
                )
                if userstuff_match:
                    content = userstuff_match.group(1)
                    clean_content = re.sub(r'<[^>]+>', '', content)
                    clean_content = re.sub(r'\s+', ' ', clean_content).strip()
                    clean_content = clean_content.replace(' . ', '.\n')

                    chapters.append({
                        'chapter_id': chapter_id,
                        'chapter_title': chapter_title,
                        'content': clean_content
                    })

        # 单章节作品处理
        if not chapters:
            userstuff_matches = re.findall(
                r'<div[^>]*class="[^"]*userstuff[^"]*"[^>]*>(.*?)</div>',
                html, re.DOTALL | re.IGNORECASE
            )
            if userstuff_matches:
                for i, content in enumerate(userstuff_matches, 1):
                    clean_content = re.sub(r'<[^>]+>', '', content)
                    clean_content = re.sub(r'\s+', ' ', clean_content).strip()
                    clean_content = clean_content.replace(' . ', '.\n')

                    chapters.append({
                        'chapter_id': str(i),
                        'chapter_title': f'Chapter {i}',
                        'content': clean_content
                    })

        return chapters


# ============ 信号处理 ============

def setup_signal_handlers(crawler: 'BatchCrawler'):
    """设置信号处理器"""
    def handler(signum, frame):
        sig_name = signal.Signals(signum).name
        logger.warning(f"[信号] 收到 {sig_name}，准备优雅关闭...")
        crawler.shutdown()
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


# ============ 主函数 ============

async def main():
    """主函数"""
    config = CrawlerConfig(
        max_concurrent=3,        # 最多同时3个请求
        max_retries=5,           # 失败重试5次
        retry_base_delay=1.0,    # 初始延迟1秒
        retry_max_delay=60.0,    # 最大延迟60秒
        connection_timeout=10,     # 连接超时10秒
        read_timeout=30,         # 读取超时30秒
        work_delay=1.0,          # 作品间延迟1秒
        checkpoint_interval=5,   # 每5个作品保存检查点
    )

    logger.info("=" * 60)
    logger.info("AO3 批量爬虫 - 优化版")
    logger.info("=" * 60)
    logger.info(f"并发数: {config.max_concurrent}")
    logger.info(f"最大重试: {config.max_retries}")
    logger.info(f"输入检查点: {config.input_checkpoint_file}")
    logger.info("=" * 60)

    # 初始化检查点管理器
    checkpoint = CheckpointManager(config)
    await checkpoint.load()

    # 从extract_optimized.py的检查点加载work_id列表
    if not await checkpoint.load_input_checkpoint(config.input_checkpoint_file):
        logger.error("无法加载输入检查点，退出")
        return

    # 创建并运行爬虫
    async with BatchCrawler(config, checkpoint) as crawler:
        setup_signal_handlers(crawler)
        results = await crawler.crawl_works()

    logger.info("\n" + "=" * 60)
    logger.info("所有任务完成!")
    logger.info(f"成功爬取: {len(results)} 个作品")
    logger.info("=" * 60)


if __name__ == '__main__':
    asyncio.run(main())
