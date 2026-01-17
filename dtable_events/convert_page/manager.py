"""
RobustPlaywrightManager
- Runs a dedicated worker thread with an asyncio event loop that owns Playwright and browser instances.
- Exposes synchronous thread-safe APIs for callers to submit PDF tasks (list of {"url","filename"[,"selector"]}).
- Configurable number of browser instances and contexts-per-browser concurrency.
- Saves PDFs to local files.
- Auto-restarts browser instances when they fail, without losing queued tasks.
"""

import asyncio
import logging
import threading
import time
import os
from typing import List, Dict, Optional, Any, Tuple
from concurrent.futures import Future, wait as wait_futures
import queue
import uuid

import psutil
from playwright.async_api import async_playwright, Playwright, Browser, BrowserContext

from dtable_events.convert_page.utils import wait_for_images

logger = logging.getLogger(__name__)


class RobustPlaywrightManager:
    """
    Manager runs a background worker thread that owns an asyncio loop and Playwright.

    Key config:
      num_browsers: number of browser processes to launch in worker
      contexts_per_browser: concurrent contexts per browser
      page_timeout: default timeout per page operation in ms
      launch_args: list of chromium args or executable_path via dict
    """

    def __init__(
        self,
        num_browsers: int = 2,
        contexts_per_browser: int = 3,
        page_timeout: int = 30_000,
        browser_launch_kwargs: Optional[Dict[str, Any]] = None,
        health_check_interval: int = 30,
    ):
        self.num_browsers = max(1, num_browsers)
        self.contexts_per_browser = max(1, contexts_per_browser)
        self.page_timeout = page_timeout
        self.browser_launch_kwargs = browser_launch_kwargs or {}
        self.health_check_interval = health_check_interval

        # thread & loop
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_event = threading.Event()

        # task queue (thread-safe for producers)
        self._task_queue: "queue.Queue[Tuple[Dict, Future]]" = queue.Queue()

        # worker-internal state (initialized in worker loop)
        self._playwright: Optional[Playwright] = None
        self._browsers: List[Optional[Browser]] = []
        # _contexts: for each browser, a list of active contexts (each corresponds to a batch)
        self._contexts: List[List[BrowserContext]] = []
        # batch map: batch_id -> {'browser_idx': int, 'context': BrowserContext, 'pending': int}
        self._batch_map: Dict[str, Dict[str, Any]] = {}
        self._slot_queue: Optional[asyncio.Queue] = None  # available browser slots (browser_idx)
        self._worker_ready = threading.Event()

        # stats
        self._total_tasks = 0
        self._failed_tasks = 0
        self._last_health: Dict[str, Any] = {}

    # ---------------------- public sync API ----------------------
    def start(self):
        """Start background worker thread and event loop."""
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

        This call is one batch: a unique batch_id is generated and a dedicated BrowserContext will be
        created for this batch (on some browser).
        """
        if not self._thread or not self._thread.is_alive():
            raise RuntimeError("RobustPlaywrightManager not started. Call start() first.")

        os.makedirs(output_dir, exist_ok=True)
        timeout_ms = timeout_ms or self.page_timeout

        # create a batch_id
        batch_id = uuid.uuid4().hex

        num_tasks = len(items)
        # ask worker to create a batch context and reserve it
        fut_new = Future()
        self._task_queue.put(({'__new_batch__': True, 'batch_id': batch_id, 'num_tasks': num_tasks}, fut_new))
        # wait for worker ack (context created)
        fut_new.result()  # will raise if worker failed to create

        # now enqueue tasks with batch_id
        futures: List[Future] = []
        for item in items:
            if 'url' not in item or 'filename' not in item:
                raise ValueError("each item must have 'url' and 'filename'")
            dest = os.path.join(output_dir, item['filename'])
            fut: Future = Future()
            task_dict = {'url': item['url'], 'path': dest, 'timeout_ms': timeout_ms, 'batch_id': batch_id}
            if 'selector' in item:
                task_dict['selector'] = item['selector']
            self._task_queue.put((task_dict, fut))
            futures.append(fut)
            self._total_tasks += 1

        # block until all tasks in this batch finish
        wait_futures(futures)

        # collect results
        results: List[str] = []
        for fut in futures:
            if fut.cancelled():
                continue
            exc = fut.exception()
            if exc:
                # count failures, but keep going
                self._failed_tasks += 1
                continue
            results.append(fut.result())

        # No explicit cleanup call - batch context is closed in worker when pending hits zero.
        return results

    def get_stats(self) -> Dict[str, Any]:
        return {
            'total_tasks': self._total_tasks,
            'failed_tasks': self._failed_tasks,
            'last_health': self._last_health,
            'num_browsers': self.num_browsers,
            'contexts_per_browser': self.contexts_per_browser,
        }

    # ---------------------- worker thread & loop ----------------------
    def _thread_main(self):
        """Thread target: setup and run asyncio event loop."""
        try:
            asyncio.run(self._worker_main())
        except Exception as e:
            logger.exception(e)
        finally:
            self._worker_ready.clear()

    async def _worker_main(self):
        """Async worker main: start playwright, browsers, and consumer tasks."""
        self._loop = asyncio.get_event_loop()
        try:
            self._playwright = await async_playwright().start()
        except Exception as e:
            logger.exception(e)
            raise

        # launch browsers (no persistent context at browser start)
        self._browsers = [None] * self.num_browsers
        self._contexts = [[] for _ in range(self.num_browsers)]
        self._batch_map = {}

        for i in range(self.num_browsers):
            try:
                self._browsers[i] = await self._launch_browser(i)
            except Exception as e:
                logger.exception(e)
                self._browsers[i] = None
                self._contexts[i] = []

        # slot queue contains browser_idx for available page slots
        self._slot_queue = asyncio.Queue()
        for i in range(self.num_browsers):
            for _ in range(self.contexts_per_browser):
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
        kwargs = dict(executable_path=os.environ.get('CONVERT_PDF_CHROME_PATH', '/usr/bin/google-chrome'), headless=True)
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
        # close all contexts per browser
        for i, ctx_list in enumerate(self._contexts):
            for c in list(ctx_list):
                try:
                    await c.close()
                except Exception:
                    pass
        # close browsers
        for i, b in enumerate(self._browsers):
            try:
                if b:
                    await b.close()
            except Exception:
                pass
        self._browsers = []
        self._contexts = []
        self._batch_map = {}

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
                logger.info(f"playwright health: {self._last_health}")
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.exception(e)

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
        Special task types:
          - {'__new_batch__': True, 'batch_id': id, 'num_tasks': n}  -> create batch context
          - {'__stop__': True} -> shutdown
          - normal task: contains 'url', 'path', and 'batch_id'
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

                if isinstance(task_dict, dict) and task_dict.get('__new_batch__'):
                    # create a context for this batch and record mapping
                    await self._handle_new_batch(task_dict, fut)
                    continue

                # schedule worker for this task
                asyncio.create_task(self._handle_task(task_dict, fut))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(e)
                await asyncio.sleep(0.1)

    async def _handle_new_batch(self, task: Dict[str, Any], fut: Future):
        """
        Create a new BrowserContext for this batch and register in _batch_map.
        Chooses the browser with the fewest active contexts to balance load.
        """
        batch_id = task.get('batch_id')
        num_tasks = int(task.get('num_tasks', 0))
        if not batch_id:
            fut.set_exception(ValueError("batch_id missing"))
            return

        try:
            # choose browser with minimal contexts (least loaded)
            min_idx = None
            min_len = None
            for i, b in enumerate(self._browsers):
                if b is None or (not b.is_connected()):
                    # try relaunching
                    try:
                        self._browsers[i] = await self._launch_browser(i)
                        self._contexts[i] = []
                    except Exception:
                        # put a large value so it won't be chosen if other browsers available
                        continue
                cur_len = len(self._contexts[i])
                if min_len is None or cur_len < min_len:
                    min_len = cur_len
                    min_idx = i

            if min_idx is None:
                raise RuntimeError("No available browser to host new batch")

            browser_idx = min_idx
            browser = self._browsers[browser_idx]

            # create a new context for the batch
            context = await browser.new_context(ignore_https_errors=True)

            # register context
            self._contexts[browser_idx].append(context)
            self._batch_map[batch_id] = {
                'browser_idx': browser_idx,
                'context': context,
                'pending': num_tasks,
            }

            fut.set_result(True)
        except Exception as e:
            logger.exception(e)
            fut.set_exception(e)

    async def _handle_task(self, task: Dict[str, Any], fut: Future):
        url = task.get('url')
        dest_path = task.get('path')
        timeout_ms = int(task.get('timeout_ms', self.page_timeout))
        batch_id = task.get('batch_id')

        if not batch_id:
            fut.set_exception(ValueError("task missing batch_id"))
            return

        # ensure parent dir exists
        try:
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        except Exception:
            pass

        try:
            # get a browser slot (browser idx) from slot_queue
            browser_idx = await self._slot_queue.get()
        except Exception as e:
            fut.set_exception(e)
            return

        try:
            # lookup batch context (must exist)
            batch_info = self._batch_map.get(batch_id)
            if not batch_info:
                raise RuntimeError(f"Unknown batch_id {batch_id}")

            browser_idx_for_batch = batch_info['browser_idx']
            context = batch_info['context']
            browser = self._browsers[browser_idx_for_batch]

            # if browser crashed or context invalid, try to re-create browser+context and update mapping
            if browser is None or not browser.is_connected():
                try:
                    browser = await self._launch_browser(browser_idx_for_batch)
                    self._browsers[browser_idx_for_batch] = browser
                except Exception as e:
                    raise RuntimeError("Failed to relaunch browser for batch") from e

                # create a fresh context for the batch and record it
                try:
                    new_context = await browser.new_context(viewport={'width': 1920, 'height': 1080}, ignore_https_errors=True)
                    # replace the old context in contexts list (best-effort)
                    try:
                        # remove old context reference if present
                        if context in self._contexts[browser_idx_for_batch]:
                            self._contexts[browser_idx_for_batch].remove(context)
                    except Exception:
                        pass
                    self._contexts[browser_idx_for_batch].append(new_context)
                    context = new_context
                    self._batch_map[batch_id]['context'] = context
                except Exception as e:
                    raise RuntimeError("Failed to create new context for batch after browser relaunch") from e

            # create only page — reuse batch context
            page = await context.new_page()

            page.on("pageerror", lambda e: logger.error("PageError: %s", e))

            async def handle_console(msg):
                args = [await arg.json_value() for arg in msg.args]
                logger.debug("Console [%s]: %s %s", msg.type, msg.text, args)

            page.on("console", lambda msg: asyncio.create_task(handle_console(msg)))
            page.on("request", lambda req: logger.debug("Request: %s %s", req.method, req.url))
            page.on("response", lambda res: logger.debug("Response: %d %s", res.status, res.url))
            page.on("crash", lambda: logger.debug("PAGE CRASHED"))
            page.on("pageerror", lambda e: logger.debug("PAGE ERROR: %s", e))
            page.on("requestfailed", lambda req: logger.debug("REQUEST FAILED: %s", req.url))

            try:
                page.set_default_timeout(timeout_ms)
                await page.goto(url, wait_until='load', timeout=timeout_ms)
                await page.wait_for_load_state('networkidle')
                await page.wait_for_timeout(500)  # small wait for dynamic renders

                if 'selector' in task:
                    await page.wait_for_selector(task['selector'], state='attached', timeout=5 * 1000)

                await wait_for_images(page)

                ss_path = dest_path + '.test.png'
                await page.screenshot(path=ss_path, full_page=True)

                await page.pdf(
                    path=dest_path,
                    format='A4',
                    print_background=True,
                    landscape=False,
                    display_header_footer=False,
                    prefer_css_page_size=True
                )

                fut.set_result(dest_path)
            except Exception as e:
                # try to capture diagnostics (optional): write page screenshot
                if logger.root.level == logging.DEBUG:
                    try:
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

        except Exception as e:
            fut.set_exception(e)
        finally:
            # decrement batch pending count and possibly cleanup context
            try:
                # protect against batch_map missing (race)
                if batch_id in self._batch_map:
                    self._batch_map[batch_id]['pending'] -= 1
                    pending = int(self._batch_map[batch_id]['pending'])
                    if pending <= 0:
                        # close and remove context
                        try:
                            ctx = self._batch_map[batch_id]['context']
                            # find which browser list holds this context
                            bidx = self._batch_map[batch_id]['browser_idx']
                            try:
                                await ctx.close()
                            except Exception:
                                pass
                            try:
                                if ctx in self._contexts[bidx]:
                                    self._contexts[bidx].remove(ctx)
                            except Exception:
                                pass
                        except Exception:
                            pass
                        # remove mapping
                        try:
                            del self._batch_map[batch_id]
                        except Exception:
                            pass
            except Exception:
                pass

            # release slot for reuse
            try:
                self._slot_queue.put_nowait(browser_idx)
            except Exception:
                pass

playwright_manager = None

def get_playwright_manager():
    global playwright_manager
    if not playwright_manager:
        try:
            num_browsers = int(os.environ.get('CONVERT_PDF_BROWSERS', '2'))
        except:
            num_browsers = 2
        try:
            contexts_per_browser = int(os.environ.get('CONVERT_PDF_SESSIONS_PER_BROWSER', '3'))
        except:
            contexts_per_browser = 3
        playwright_manager = RobustPlaywrightManager(
            num_browsers=num_browsers,
            contexts_per_browser=contexts_per_browser
        )
    return playwright_manager
