import requests
from bs4 import BeautifulSoup
import time
import logging
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
from collections import deque

logging.basicConfig(level=logging.INFO)

visited = set()
queue = deque()
level_urls = {}

def can_fetch(url, user_agent='*'):
    parsed_url = urlparse(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    robots_url = urljoin(base_url, '/robots.txt')
    
    try:
        robots_response = requests.get(robots_url)
        if robots_response.status_code == 200:
            robots_txt = robots_response.text
            rp = RobotFileParser()
            rp.parse(robots_txt.splitlines())
            return rp.can_fetch(user_agent, url)
        else:
            return True
    except Exception as e:
        logging.error(f"Error fetching robots.txt from {robots_url}: {e}")
        return False

def fetch_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return None

def parse_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    return soup

def extract_links(soup, base_url):
    links = set()
    for link in soup.find_all('a', href=True):
        full_url = urljoin(base_url, link['href'])
        links.add(full_url)
    return links

def crawl(url, level, user_agent='*', delay=1):
    if url in visited:
        logging.info(f"Already visited {url}")
        return
    
    if not can_fetch(url, user_agent):
        logging.info(f"Fetching not allowed for {url}")
        return

    visited.add(url)
    
    # Initialize the queue at level 0
    if level == 0:
        queue.append((url, level))
        level_urls[level] = [url]
    else:
        queue.append((url, level))
        level_urls.setdefault(level, []).append(url)

    while queue:
        current_url, current_level = queue.popleft()
        logging.info(f"Processing {current_url} at level {current_level}")
        
        html = fetch_url(current_url)
        if not html:
            continue
        
        soup = parse_html(html)
        base_url = f"{urlparse(current_url).scheme}://{urlparse(current_url).netloc}"
        links = extract_links(soup, base_url)
        
        logging.info(f"Found {len(links)} links on {current_url}")

        for link in links:
            if link not in visited:
                time.sleep(delay)
                queue.append((link, current_level + 1))
                visited.add(link)
                level_urls.setdefault(current_level + 1, []).append(link)

if __name__ == '__main__':
    start_url = 'https://jiji.com.gh'
    crawl(start_url, level=0)
    
    # Output the results
    for lvl, urls in level_urls.items():
        logging.info(f"Level {lvl}: {len(urls)} URLs")
        for url in urls:
            logging.info(f"  - {url}")
