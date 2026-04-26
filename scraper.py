import re
from urllib.parse import urlparse, urljoin, urldefrag
from collections import defaultdict
from bs4 import BeautifulSoup
import hashlib

import json
import os

#for print report
DATA_DIR = "crawl_data"
PAGES_FILE = os.path.join(DATA_DIR, "pages.jsonl")
WORDS_FILE = os.path.join(DATA_DIR, "words.txt")
SUBDOMAINS_FILE = os.path.join(DATA_DIR, "subdomains.jsonl")

os.makedirs(DATA_DIR, exist_ok=True)


# for report
visited = set() # unique pages
word_freq = defaultdict(int) # word: count
page_freq = {} # url: word count
subdomains = defaultdict(set) # subdomain: set of pages
fingerprints = set() # dupe-detection

'''
Requirements:
- Honor the politeness delay for each site
- Crawl all pages with high textual information content
- Detect and avoid infinite traps
- Detect and avoid sets of similar pages with no information
- Detect and avoid dead URLs that return a 200 status but no data (click here to see what the different HTTP status codes meanLinks to an external site.)
- Detect and avoid crawling very large files, especially if they have low information value
'''

MAX_SIZE = 10 * 1024 * 1024 # 10 MB cap (large file)
MIN_WORDS = 50 # low-value (dead url)

# Query/action patterns that usually create duplicate/generated pages
QUERY_TRAP_PATTERNS = re.compile(
    r"(sid=|session|replytocom|share=|print=|lang=|"
    r"sort=|filter=|page=\d{3,}|"
    r"do=export_pdf|do=edit|do=login|do=index|do=media|do=recent|do=revisions|do=backlink|do=$|"
    r"tab_files=|tab_details=|image=|mediado=|sectok=|export_code|"
    r"idx=|rev=|subPage=|skin=|"
    r"s%5B%5D=|s\[\]=|"
    r"ical=1|outlook-ical=1|format=txt)",
    re.IGNORECASE
)

# Calendar/date archive patterns that can create infinite spaces
CALENDAR_TRAP_PATTERNS = re.compile(
    r"(/events/\d{4}-\d{2}-\d{2}/?|"
    r"/events/month/\d{4}-\d{2}/?|"
    r"/events/category/[^?]*/\d{4}-\d{2}/?|"
    r"/day/\d{4}-\d{2}-\d{2}/?|"
    r"\d{4}[/-]\d{2}[/-]\d{2}[/-]\d{2})",
    re.IGNORECASE
)

# Wiki/history/export/download patterns
WIKI_TRAP_PATTERNS = re.compile(
    r"(timeline\?|precision=second|from=|zip-attachment)",
    re.IGNORECASE
)

# Login pages should not be crawled/counted
LOGIN_TRAP_PATTERNS = re.compile(
    r"(/login|login\?)",
    re.IGNORECASE
)

# for pages we don't have access to (needs ics log in)
ACCESS_DENIED_PATTERNS = re.compile(
    r"(insufficient access privileges|access to most of the information.*restricted|"
    r"you are currently not logged in|enter your authentication credentials|"
    r"please make sure to login|username/password|"
    r"permission denied|access denied|login required|you need to log in|forbidden)",
    re.IGNORECASE
)

# stop words from https://www.ranks.nl/stopwords
STOP_WORDS =  ['a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', "aren't", 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 
               'between', 'both', 'but', 'by', "can't", 'cannot', 'could', "couldn't", 'did', "didn't", 'do', 'does', "doesn't", 'doing', "don't", 'down', 'during', 'each', 'few', 
               'for', 'from', 'further', 'had', "hadn't", 'has', "hasn't", 'have', "haven't", 'having', 'he', "he'd", "he'll", "he's", 'her', 'here', "here's", 'hers', 'herself', 
               'him', 'himself', 'his', 'how', "how's", 'i', "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its', 'itself', "let's", 'me', 'more', 
               'most', "mustn't", 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our', 'oursourselves', 'out', 'over', 'own', 'same', 
               "shan't", 'she', "she'd", "she'll", "she's", 'should', "shouldn't", 'so', 'some', 'such', 'than', 'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves', 
               'then', 'there', "there's", 'these', 'they', "they'd", "they'll", "they're", "they've", 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', 
               "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were', "weren't", 'what', "what's", 'when', "when's", 'where', "where's", 'which', 'while', 'who', "who's", 'whom', 
               'why', "why's", 'with', "won't", 'would', "wouldn't", 'you', "you'd", "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves']

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content

    # check status
    if resp.status != 200 or resp.raw_response is None:
        return []
    
    content = resp.raw_response.content

    # skip files too large
    if len(content) > MAX_SIZE:
        return []

    # parse HTML
    try:
        soup = BeautifulSoup(content, "lxml")
    except Exception:
        return [] # skip page if crash/smt goes wrong
    
    # get tokens (Ryan's tokenizer from assignment 1 w/o file)
    text = soup.get_text(separator=" ", strip=True) # strip HTML, just actual text

    # skip login/access-denied pages that still return 200
    if ACCESS_DENIED_PATTERNS.search(text):
        return []

    tokens = []
    cur = ""

    for c in text:
        if c.isascii() and c.isalnum():
            cur += c.lower()
        else:
            if cur:
                tokens.append(cur)
                cur = ""

    if cur:
        tokens.append(cur)

    # skip dead (near-empty) URLs
    if len(tokens) < MIN_WORDS:
        return []
    
    # check for near-dupe detection
    # fp = hashlib.md5(text.encode("utf-8", errors="ignore")).hexdigest()
    # if fp in fingerprints:
    #     return []
    # fingerprints.add(fp)

    # defragment
    page_url = resp.raw_response.url if getattr(resp.raw_response, "url", None) else url
    page_url = urldefrag(page_url)[0] # [url, fragment]

    # skip if final redirected URL is invalid/trap
    if not is_valid(page_url):
        return []

    # update visited
    if page_url not in visited:
        visited.add(page_url)

        # stats for report (word freq, longest page, subdomains)
        os.makedirs(DATA_DIR, exist_ok=True)

        with open(PAGES_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "url": page_url,
                "word_count": len(tokens)
            }) + "\n")

        with open(WORDS_FILE, "a", encoding="utf-8") as f:
            for token in tokens:
                if token not in STOP_WORDS:
                    word_freq[token] += 1
                    f.write(token + "\n")

        page_freq[page_url] = len(tokens)

        parsed = urlparse(page_url) # [scheme/protocol (http), netloc (domain), path (/), query (?), fragment (#)]
        host = parsed.netloc.lower()
        if host.endswith(".uci.edu"):
            subdomains[host].add(page_url)

            with open(SUBDOMAINS_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "subdomain": host,
                    "url": page_url
                }) + "\n")

    # extract new links to crawl
    links = []
    for tag in soup.find_all("a", href=True):
        # <a href="link">
        href = tag["href"].strip()

        try:
            # base + relative -> absolute url (full link on web)
            href = urljoin(page_url, href)

            # defragment
            href = urldefrag(href)[0]

        except ValueError:
            continue

        links.append(href)

    return links

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        
        # allowed domains
        host = parsed.netloc.lower()
        allowed = (
            host.endswith(".ics.uci.edu")        or host == "ics.uci.edu"        or
            host.endswith(".cs.uci.edu")          or host == "cs.uci.edu"          or
            host.endswith(".informatics.uci.edu") or host == "informatics.uci.edu" or
            host.endswith(".stat.uci.edu")        or host == "stat.uci.edu"
        )
        if not allowed:
            return False
        
        # check trap patterns
        full_url = parsed.path + ("?" + parsed.query if parsed.query else "")

        if QUERY_TRAP_PATTERNS.search(full_url):
            return False

        if CALENDAR_TRAP_PATTERNS.search(full_url):
            return False

        if WIKI_TRAP_PATTERNS.search(full_url):
            return False

        if LOGIN_TRAP_PATTERNS.search(full_url):
            return False

        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|pps|ppsx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except (TypeError, ValueError):
        return False


def print_report():
    print("\n=== CRAWL REPORT ===\n")

    # unique pages
    print(f"Unique pages crawled: {len(visited)}\n")

    # longest page by word count
    if page_freq:
        longest = max(page_freq, key=lambda url: page_freq[url])
        print(f"Longest page: {longest}  ({page_freq[longest]} words)\n")

    # 50 most common words
    print("50 most common words:")
    for word, count in sorted(word_freq.items(), key=lambda x: -x[1])[:50]:
        print(f"   {word:30s} {count}")

    # subdomains of ics.uci.edu
    print(f"\nSubdomains of ics.uci.edu ({len(subdomains)} total):")
    for sub in sorted(subdomains):
        print(f"   {sub}, {len(subdomains[sub])}")