import re
import hashlib
import json
import os
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from collections import defaultdict

# file paths
DATA_DIR = "crawl_data"
PAGES_FILE = os.path.join(DATA_DIR, "pages.jsonl")
WORDS_FILE = os.path.join(DATA_DIR, "words.txt")
SUBDOMAINS_FILE = os.path.join(DATA_DIR, "subdomains.jsonl")

os.makedirs(DATA_DIR, exist_ok=True)

# globals (perists thru entire crawl, too expensive to re-open to read/write for each page)
visited = set()

def _load_visited():
    if os.path.exists(PAGES_FILE):
        with open(PAGES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                visited.add(json.loads(line)["url"])
 
_load_visited()

# constants
MAX_SIZE = 10 * 1024 * 1024 # 10 MB cap (large file)
MIN_TOKEN = 100 # low-value (dead url/empty page)
TRAP_PATTERNS = re.compile(
    r"("

    # -------------------------------
    # WordPress / dynamic query traps
    # -------------------------------
    r"[?&](p|page_id)=\d+|"
    r"[?&](replytocom|share|print)=|"

    # -------------------------------
    # Permissions
    # -------------------------------
    r"/login|/register|/preferences|"
    r"/wp-admin|/wp-login|"

    # -------------------------------
    # Search / filter / sorting traps
    # -------------------------------
    r"[?&](q|search|filter|category|tag|sort|order|format|convert|version|view|output|download|redirect_to|redirect)=|"
    r"[?&]sort=|[?&]order=|"
    r"filter%5B|filter\[|"

    # -------------------------------
    # Pagination traps
    # -------------------------------
    r"/page/\d+|"
    r"[?&]page=\d{2,}|"
    r"[?&]paged=\d+|"

    # -------------------------------
    # Calendar / event / timeline traps
    # -------------------------------
    r"/calendar|/events|"
    r"[?&]ical="
    r"/day/\d{4}-\d{2}-\d{2}|"
    r"/\d{4}/\d{2}/|"
    r"/timeline|"

    # -------------------------------
    # DokuWiki / ICS internal traps
    # -------------------------------
    r"\?do=|" # very similar + little text, shouldn't pass MIN_WORDS test
    r"[?&]idx=" # open dif sections, same page

    r")",
    re.IGNORECASE
)

# stop words from https://www.ranks.nl/stopwords, set > list
STOP_WORDS =  set(['a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', "aren't", 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 
               'between', 'both', 'but', 'by', "can't", 'cannot', 'could', "couldn't", 'did', "didn't", 'do', 'does', "doesn't", 'doing', "don't", 'down', 'during', 'each', 'few', 
               'for', 'from', 'further', 'had', "hadn't", 'has', "hasn't", 'have', "haven't", 'having', 'he', "he'd", "he'll", "he's", 'her', 'here', "here's", 'hers', 'herself', 
               'him', 'himself', 'his', 'how', "how's", 'i', "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its', 'itself', "let's", 'me', 'more', 
               'most', "mustn't", 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our', 'oursourselves', 'out', 'over', 'own', 'same', 
               "shan't", 'she', "she'd", "she'll", "she's", 'should', "shouldn't", 'so', 'some', 'such', 'than', 'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves', 
               'then', 'there', "there's", 'these', 'they', "they'd", "they'll", "they're", "they've", 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', 
               "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were', "weren't", 'what', "what's", 'when', "when's", 'where', "where's", 'which', 'while', 'who', "who's", 'whom', 
               'why', "why's", 'with', "won't", 'would', "wouldn't", 'you', "you'd", "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves'])

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
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

    # parse HTML (get actual text)
    try:
        soup = BeautifulSoup(content, "lxml")
    except Exception:
        return [] # skip page if crash/smt goes wrong
    
    # get tokens (Ryan's tokenizer from assignment 1 w/o file)
    text = soup.get_text(separator=" ", strip=True) # strip HTML, just actual text

    if "Error: Forbidden" in text or "not logged in" in text:
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
    if cur: # end of line
        tokens.append(cur)

    # skip dead (near-empty) URLs
    if len(tokens) < MIN_TOKEN:
        return []

    # defragment
    page_url = urldefrag(resp.raw_response.url)[0] # [url, fragment]

    # check visited, update if new
    if page_url in visited:
        return _extract_links(soup, resp.raw_response.url)
    visited.add(page_url)

    # record page: url, word_count
    with open(PAGES_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps({
            "url": page_url,
            "word_count": len(tokens)
        }) + "\n")

    # record word freq
    with open(WORDS_FILE, "a", encoding="utf-8") as f:
        for token in tokens:
            if len(token) >= 3 and not token.isdigit() and token not in STOP_WORDS:
                f.write(token + "\n")

    # record subdomains
    host = urlparse(page_url).netloc.lower()
    if host.endswith(".uci.edu"):
        with open(SUBDOMAINS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "subdomain": host,
                "url": page_url
            }) + "\n")

    return _extract_links(soup, resp.raw_response.url)
 
 
def _extract_links(soup, base_url):
    links = []
    for tag in soup.find_all("a", href=True):
        # <a href="link">
        href = tag["href"].strip()

        try: # bad links
            # base + relative -> absolute url (full link on web)
            href = urljoin(base_url, href)

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
            host.endswith(".ics.uci.edu")         or host == "ics.uci.edu"         or
            host.endswith(".cs.uci.edu")          or host == "cs.uci.edu"          or
            host.endswith(".informatics.uci.edu") or host == "informatics.uci.edu" or
            host.endswith(".stat.uci.edu")        or host == "stat.uci.edu"
        )
        if not allowed:
            return False
        
        # check trap patterns
        full_url = parsed.path + ("?" + parsed.query if parsed.query else "")
        if TRAP_PATTERNS.search(full_url):
            return False

        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|ppsx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"

            # other
            + r"|txt|sql"  # text/data
            + r"|py|java|c|cpp|h|hpp|cc|cs|js|ts|jsx|tsx|rkt" # programming / source code
            + r"|json|yaml|yml|svg" # markup / data formats
            + r"|sh|bash|zsh" # scripts
            + r"|log|cfg|ini|conf" # config / logs
            + r"|ipynb" # notebooks
            + r"|bib|nb|hs|lsp|scm|lif|m|als|dsp|ma|inc|mhcid|cls|ff|results|hqx|pov|edelsbrunner|class|ss|grm" # misc

            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise


def print_report():
    print("\n\n=== CRAWL REPORT ===\n")
 
    # read pages
    pages = {}
    if os.path.exists(PAGES_FILE):
        with open(PAGES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                entry = json.loads(line.strip())
                pages[entry["url"]] = entry["word_count"]
 
    # unique pages
    print(f"1. Unique pages: {len(pages)}\n")
 
    # longest page
    if pages:
        longest = max(pages, key=lambda u: pages[u])
        print(f"2. Longest page: {longest}  ({pages[longest]} words)\n")
 
    # 50 most common words
    word_counts = defaultdict(int)
    if os.path.exists(WORDS_FILE):
        with open(WORDS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                word = line.strip()
                if word:
                    word_counts[word] += 1
 
    print("3. 50 most common words:")
    for word, count in sorted(word_counts.items(), key=lambda x: -x[1])[:50]:
        print(f"   {word:30s} {count}")
 
    # subdomains
    subdomain_pages = defaultdict(set)
    if os.path.exists(SUBDOMAINS_FILE):
        with open(SUBDOMAINS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                entry = json.loads(line.strip())
                subdomain_pages[entry["subdomain"]].add(entry["url"])
 
    print(f"\n4. Subdomains of .uci.edu ({len(subdomain_pages)} total):")
    for sub in sorted(subdomain_pages):
        print(f"   {sub}, {len(subdomain_pages[sub])}")