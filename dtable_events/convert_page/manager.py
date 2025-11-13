"""
RobustPlaywrightManager
- Runs a dedicated worker thread with an asyncio event loop that owns Playwright and browser instances.
- Exposes synchronous thread-safe APIs for callers to submit PDF tasks (list of {"url","filename"}).
- Configurable number of browser instances and pages-per-browser concurrency.
- Saves PDFs to local files.
- Auto-restarts browser instances when they fail, without losing queued tasks.

Usage example is included at the bottom.
"""

import asyncio
import logging
import threading
import time
import os
import traceback
from typing import List, Dict, Optional, Any, Tuple
from concurrent.futures import Future, wait as wait_futures, FIRST_EXCEPTION
import queue

import psutil
from playwright.async_api import async_playwright, Playwright, Browser

from dtable_events.convert_page.utils import wait_for_images

logger = logging.getLogger(__name__)


class TaskCancelled(Exception):
    pass


class RobustPlaywrightManager:
    """
    Manager runs a background worker thread that owns an asyncio loop and Playwright.

    Key config:
      num_browsers: number of browser processes to launch in worker
      pages_per_browser: concurrent pages per browser
      page_timeout: default timeout per page operation in ms
      launch_args: list of chromium args or executable_path via dict
    """

    def __init__(
        self,
        num_browsers: int = 2,
        pages_per_browser: int = 3,
        page_timeout: int = 30_000,
        browser_launch_kwargs: Optional[Dict[str, Any]] = None,
        health_check_interval: int = 30,
    ):
        self.num_browsers = max(1, num_browsers)
        self.pages_per_browser = max(1, pages_per_browser)
        self.page_timeout = page_timeout
        self.browser_launch_kwargs = browser_launch_kwargs or {}
        self.health_check_interval = health_check_interval

        # thread & loop
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_event = threading.Event()

        # task queue (thread-safe for producers)
        self._task_queue: "queue.Queue[Tuple[Dict, Future]]" = queue.Queue()

        # worker-internal state (created in worker loop)
        self._playwright: Optional[Playwright] = None
        self._browsers: List[Optional[Browser]] = []
        self._slot_queue: Optional[asyncio.Queue] = None  # available browser slots (browser_idx)
        self._worker_ready = threading.Event()

        # stats
        self._total_tasks = 0
        self._failed_tasks = 0
        self._last_health: Dict[str, Any] = {}

    # ---------------------- public sync API ----------------------
    def start(self):
        """Start the background worker thread and event loop."""
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._thread_main, daemon=True)
        self._thread.start()

        # wait until worker signals it's ready
        self._worker_ready.wait()

    def stop(self):
        """Stop background worker and clean resources. Waits for thread to exit."""
        if not self._thread:
            return
        self._stop_event.set()
        # put a dummy task to wake consumer
        self._task_queue.put(({'__stop__': True}, Future()))
        self._thread.join(timeout=30)
        self._thread = None

    def batch_urls_to_pdf_sync(
        self,
        items: List[Dict[str, str]],
        output_dir: str,
        timeout_ms: Optional[int] = None,
    ) -> List[str]:
        """
        Submit a batch of tasks. `items` is List[{'filename': 'abc.pdf', 'url': 'https://...'}]
        Returns list of file paths for successfully generated PDFs. Raises if the manager isn't started.
        """
        if not self._thread or not self._thread.is_alive():
            raise RuntimeError("RobustPlaywrightManager not started. Call start() first.")

        os.makedirs(output_dir, exist_ok=True)
        timeout_ms = timeout_ms or self.page_timeout

        futures: List[Future] = []
        for item in items:
            if 'url' not in item or 'filename' not in item:
                raise ValueError("each item must have 'url' and 'filename'")
            dest = os.path.join(output_dir, item['filename'])
            fut: Future = Future()
            task_dict = {'url': item['url'], 'path': dest, 'timeout_ms': timeout_ms}
            if 'selector' in task_dict:
                task_dict['selector'] = task_dict['selector']
            task = (task_dict, fut)
            self._task_queue.put(task)
            futures.append(fut)
            self._total_tasks += 1

        # wait for all futures
        wait_futures(futures)

        results: List[str] = []
        for fut in futures:
            if fut.cancelled():
                continue
            exc = fut.exception()
            if exc:
                # don't raise aggregate exception - collect failed count and continue
                self._failed_tasks += 1
                continue
            results.append(fut.result())
        return results

    def get_stats(self) -> Dict[str, Any]:
        return {
            'total_tasks': self._total_tasks,
            'failed_tasks': self._failed_tasks,
            'last_health': self._last_health,
            'num_browsers': self.num_browsers,
            'pages_per_browser': self.pages_per_browser,
        }

    # ---------------------- worker thread & loop ----------------------
    def _thread_main(self):
        """Thread target: setup and run asyncio event loop."""
        try:
            asyncio.run(self._worker_main())
        except Exception:
            traceback.print_exc()
        finally:
            self._worker_ready.clear()

    async def _worker_main(self):
        """Async worker main: start playwright, browsers, and consumer tasks."""
        self._loop = asyncio.get_event_loop()
        try:
            self._playwright = await async_playwright().start()
        except Exception:
            traceback.print_exc()
            raise

        # launch browsers
        self._browsers = [None] * self.num_browsers
        for i in range(self.num_browsers):
            try:
                self._browsers[i] = await self._launch_browser(i)
            except Exception:
                traceback.print_exc()
                self._browsers[i] = None

        # slot queue contains browser indices for available page slots
        self._slot_queue = asyncio.Queue()
        for i in range(self.num_browsers):
            for _ in range(self.pages_per_browser):
                await self._slot_queue.put(i)

        # start a background health monitor
        health_task = asyncio.create_task(self._health_monitor())

        # start consumer
        consumer = asyncio.create_task(self._consumer_loop())

        # signal ready
        self._worker_ready.set()

        # wait until stop event set
        while not self._stop_event.is_set():
            await asyncio.sleep(0.5)

        # shutdown
        consumer.cancel()
        health_task.cancel()
        try:
            await consumer
        except asyncio.CancelledError:
            pass
        try:
            await health_task
        except asyncio.CancelledError:
            pass

        # close browsers and playwright
        await self._cleanup_browsers()
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None

    async def _launch_browser(self, idx: int) -> Browser:
        """Launch a single Chromium browser instance. Isolated to worker loop."""
        kwargs = dict(executable_path='/usr/bin/google-chrome', headless=True)
        kwargs.update(self.browser_launch_kwargs)
        # safe defaults
        if 'args' not in kwargs:
            kwargs['args'] = [
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--disable-software-rasterizer',
            ]
        browser = await self._playwright.chromium.launch(**kwargs)
        return browser

    async def _cleanup_browsers(self):
        for i, b in enumerate(self._browsers):
            try:
                if b:
                    await b.close()
            except Exception:
                pass
        self._browsers = []

    async def _health_monitor(self):
        while True:
            try:
                await asyncio.sleep(self.health_check_interval)
                # sample memory usage of chromium-like processes
                total_mem_mb, total_cpu = self._aggregate_browser_process_stats()
                self._last_health = {
                    'memory_mb': total_mem_mb,
                    'cpu_percent': total_cpu,
                    'timestamp': time.time(),
                }
            except asyncio.CancelledError:
                return
            except Exception:
                traceback.print_exc()

    def _aggregate_browser_process_stats(self) -> Tuple[float, float]:
        try:
            total_mem = 0
            total_cpu = 0.0
            for p in psutil.process_iter(['name', 'memory_info', 'cpu_percent']):
                try:
                    name = (p.info.get('name') or '').lower()
                    if any(k in name for k in ('chrome', 'chromium', 'playwright')):
                        mi = p.info.get('memory_info')
                        if mi:
                            total_mem += getattr(mi, 'rss', 0)
                        total_cpu += float(p.info.get('cpu_percent') or 0.0)
                except Exception:
                    continue
            return total_mem / (1024**2), total_cpu
        except Exception:
            return 0.0, 0.0

    async def _consumer_loop(self):
        """Consume tasks from thread-safe queue (blocking queue.Queue.get executed in executor).
        Each queue item is (task_dict, future).
        """
        while True:
            try:
                # blocking get in threadpool, returns when main thread queued an item
                item = await asyncio.get_event_loop().run_in_executor(None, self._task_queue.get)
                task_dict, fut = item

                if isinstance(task_dict, dict) and task_dict.get('__stop__'):
                    # wake up for shutdown
                    fut.set_result(None)
                    break

                # schedule worker for this task
                asyncio.create_task(self._handle_task(task_dict, fut))
            except asyncio.CancelledError:
                break
            except Exception:
                traceback.print_exc()
                await asyncio.sleep(0.1)

    async def _handle_task(self, task: Dict[str, Any], fut: Future):
        url = task['url']
        dest_path = task['path']
        timeout_ms = int(task.get('timeout_ms', self.page_timeout))

        # ensure parent dir
        try:
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        except Exception:
            pass

        # get a browser slot (browser idx) from slot_queue
        try:
            browser_idx = await self._slot_queue.get()
        except Exception as e:
            fut.set_exception(e)
            return

        try:
            browser = self._browsers[browser_idx]
            if browser is None or not browser.is_connected():
                # try to relaunch this browser
                try:
                    browser = await self._launch_browser(browser_idx)
                    self._browsers[browser_idx] = browser
                except Exception as e:
                    fut.set_exception(e)
                    return

            # per-task: create context and page to ensure isolation
            context = await browser.new_context(viewport={'width': 1920, 'height': 1080}, ignore_https_errors=True)
            page = await context.new_page()
            try:
                page.set_default_timeout(timeout_ms)
                await page.goto(url, wait_until='load', timeout=timeout_ms)
                await page.wait_for_timeout(500)  # small wait for dynamic renders
                if 'selector' in task:
                    await page.wait_for_selector(task['selector'], state='attached', timeout=5*1000)
                await wait_for_images(page)
                await page.pdf(path=dest_path, format='A4', print_background=True,
                               margin={'top': '0.5cm','right':'0.5cm','bottom':'0.5cm','left':'0.5cm'})

                fut.set_result(dest_path)
            except Exception as e:
                # try to capture diagnostics (optional): write page screenshot
                try:
                    # best-effort screenshot
                    ss_path = dest_path + '.failed.png'
                    await page.screenshot(path=ss_path, full_page=True)
                except Exception:
                    pass
                fut.set_exception(e)
            finally:
                try:
                    await page.close()
                except Exception:
                    pass
                try:
                    await context.close()
                except Exception:
                    pass

        except Exception as e:
            fut.set_exception(e)
        finally:
            # release slot for reuse
            try:
                self._slot_queue.put_nowait(browser_idx)
            except Exception:
                pass

playwright_manager = None

def get_playwright_manager():
    global playwright_manager
    if not playwright_manager:
        playwright_manager = RobustPlaywrightManager()
    return playwright_manager
