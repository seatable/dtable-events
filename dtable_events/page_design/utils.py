import base64
import io
import json
import logging
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait

from dtable_events.app.config import DTABLE_WEB_SERVICE_URL
from dtable_events.utils import uuid_str_to_36_chars

logger = logging.getLogger(__name__)

CHROME_DATA_DIR = '/tmp/chrome-user-datas'


def get_driver(user_data_path):
    webdriver_options = Options()

    webdriver_options.add_argument('--no-sandbox')
    webdriver_options.add_argument('--headless')
    webdriver_options.add_argument('--disable-gpu')
    webdriver_options.add_argument('--disable-dev-shm-usage')
    webdriver_options.add_argument(f'--user-data-dir={user_data_path}')

    driver = webdriver.Chrome('/usr/local/bin/chromedriver', options=webdriver_options)
    return driver


def open_page_view(driver: webdriver.Chrome, dtable_uuid, page_id, row_id, access_token):
    url = DTABLE_WEB_SERVICE_URL.strip('/') + '/dtable/%s/page-design/%s/' % (uuid_str_to_36_chars(dtable_uuid), page_id)
    if row_id:
        url = DTABLE_WEB_SERVICE_URL.strip('/') + '/dtable/%s/page-design/%s/row/%s/' % (uuid_str_to_36_chars(dtable_uuid), page_id, row_id)

    url += '?access-token=%s&need_convert=%s' % (access_token, 0)
    logger.debug('url: %s', url)
    driver.execute_script(f"window.open('{url}')")
    return driver.window_handles[-1]


def wait_page_view(driver: webdriver.Chrome, session_id, row_id, output):
    def check_images_and_networks(driver, frequency=0.5):
        """
        make sure all images complete
        make sure no new connections in 0.5s.
        TODO: Unreliable and need to be continuously updated.
        """
        images_done = driver.execute_script('''
            let p = window.performance || window.mozPerformance || window.msPerformance || window.webkitPerformance || {};
            let entries = p.getEntries();
            let images = Array.from(document.images).filter(image => image.src.indexOf('/asset/') !== -1);
            if (images.length === 0) return true;
            return images.filter(image => image.complete).length == images.length;
        ''')
        if not images_done:
            return False

        entries_count = None
        while True:
            now_entries_count = driver.execute_script('''
                let p = window.performance || window.mozPerformance || window.msPerformance || window.webkitPerformance || {};
                return p.getEntries().length;
            ''')
            if entries_count is None:
                entries_count = now_entries_count
                time.sleep(frequency)
                continue
            else:
                if now_entries_count == entries_count and \
                    driver.execute_script("return document.readyState === 'complete'"):
                    return True
                break
        return False

    await_react_render = 60
    sleep_time = 2
    if not row_id:
        await_react_render = 180
        sleep_time = 6

    driver.switch_to.window(session_id)

    try:
        # make sure react is rendered, timeout await_react_render, rendering is not completed within 3 minutes, and rendering performance needs to be improved
        WebDriverWait(driver, await_react_render).until(lambda driver: driver.find_element_by_id('page-design-render-complete') is not None, message='wait react timeout')
        # make sure images from asset are rendered, timeout 120s
        WebDriverWait(driver, 120, poll_frequency=1).until(lambda driver: check_images_and_networks(driver), message='wait images and networks timeout')
        time.sleep(sleep_time) # wait for all rendering
    except Exception as e:
        logger.warning('wait for page design error: %s', e)
    finally:
        calculated_print_options = {
            'landscape': False,
            'displayHeaderFooter': False,
            'printBackground': True,
            'preferCSSPageSize': True,
        }

        resource = "/session/%s/chromium/send_command_and_get_result" % driver.session_id
        url = driver.command_executor._url + resource
        body = json.dumps({'cmd': 'Page.printToPDF', 'params': calculated_print_options})

        try:
            response = driver.command_executor._request('POST', url, body)
            if not response:
                logger.error('execute printToPDF error no response')
            v = response.get('value')['data']
            if isinstance(output, str):
                with open(output, 'wb') as f:
                    f.write(base64.b64decode(v))
            elif isinstance(output, io.BytesIO):
                output.write(base64.b64decode(v))
            logger.info('convert page to pdf success!')
        except Exception as e:
            logger.exception('execute printToPDF error: {}'.format(e))

        # debug page-design view in chrome, console log and network log, don't delete
        # logger.debug('browser console: %s', list(driver.get_log('browser')))
        # network_logs = driver.execute_script("var performance = window.performance || window.mozPerformance || window.msPerformance || window.webkitPerformance || {}; var network = performance.getEntries() || {}; return network;")
        # logger.debug('network logs start')
        # for item in network_logs:
        #     logger.debug(str(item))
        # logger.debug('network logs end')


def convert_page_to_pdf(driver: webdriver.Chrome, dtable_uuid, page_id, row_id, access_token, output):
    session_id = open_page_view(driver, dtable_uuid, page_id, row_id, access_token)
    wait_page_view(driver, session_id, row_id, output)
