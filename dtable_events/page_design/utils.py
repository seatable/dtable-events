import base64
import io
import json
import logging
import os
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait

from dtable_events.app.config import DTABLE_WEB_SERVICE_URL

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


def convert_page_to_pdf(driver: webdriver.Chrome, dtable_uuid, page_id, row_id, access_token, output):
    if not row_id:
        url = DTABLE_WEB_SERVICE_URL.strip('/') + '/dtable/%s/page-design/%s/' % (dtable_uuid, page_id)
    if row_id:
        url = DTABLE_WEB_SERVICE_URL.strip('/') + '/dtable/%s/page-design/%s/row/%s/' % (dtable_uuid, page_id, row_id)
    url += '?access-token=%s&need_convert=%s' % (access_token, 0)

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

    awaitReactRender = 60
    sleepTime = 2
    if not row_id:
        awaitReactRender = 180
        sleepTime = 6

    try:
        # make sure react is rendered, timeout awaitReactRender, rendering is not completed within 3 minutes, and rendering performance needs to be improved
        WebDriverWait(driver, awaitReactRender).until(lambda driver: driver.find_element_by_id('page-design-render-complete') is not None, message='wait react timeout')
        # make sure images from asset are rendered, timeout 120s
        WebDriverWait(driver, 120, poll_frequency=1).until(lambda driver: check_images_and_networks(driver), message='wait images and networks timeout')
        time.sleep(sleepTime) # wait for all rendering
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
        json.dump()
        # driver.quit()
