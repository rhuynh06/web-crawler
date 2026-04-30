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
# Query/action patterns that usually create duplicate/generated pages
QUERY_TRAP_PATTERNS = re.compile(
    r"(sid=|session|replytocom|share=|print=|lang=|"
    r"sort=|filter=|page=\d{3,}|"
    r"action=diff|version=\d+|"
    r"do=export_pdf|do=edit|do=login|do=index|do=media|do=recent|do=revisions|do=backlink|do=$|"
    r"tab_files=|tab_details=|image=|mediado=|sectok=|export_code|"
    r"idx=|rev=|subPage=|skin=|"
    r"s%5B%5D=|s\[\]=|"
    r"ical=1|outlook-ical=1|format=txt|"
    r"post_type=tribe_events|eventDisplay=day|eventDate=\d{4}-\d{2}-\d{2}|"
    r"tribe-bar-date=\d{4}-\d{2}-\d{2})",
    re.IGNORECASE
)

# Calendar/date archive patterns that can create infinite spaces
CALENDAR_TRAP_PATTERNS = re.compile(
    r"(/events/\d{4}-\d{2}-\d{2}/?|"
    r"/events/month/\d{4}-\d{2}/?|"
    r"/events/category/[^?]*/\d{4}-\d{2}/?|"
    r"/events/list/page/\d+/?|"
    r"/day/\d{4}-\d{2}-\d{2}/?|"
    r"\d{4}[/-]\d{2}[/-]\d{2}[/-]\d{2})",
    re.IGNORECASE
)

# Wiki/history/export/download patterns
WIKI_TRAP_PATTERNS = re.compile(
    r"(timeline\?|precision=second|from=|"
    r"zip-attachment|raw-attachment|/attachment/|format=txt|"
    r"projects:maint|"
    r"\?C=[A-Z];O=[A-Z])",
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
STOP_WORDS =  set(['a', 'able', 'about', 'above', 'abst', 'accordance', 'according', 'accordingly', 'across', 'act', 'actually', 'added', 'adj', 'affected', 
'affecting', 'affects', 'after', 'afterwards', 'again', 'against', 'ah', 'all', 'almost', 'alone', 'along', 'already', 'also', 'although', 'always', 'am', 
'among', 'amongst', 'an', 'and', 'announce', 'another', 'any', 'anybody', 'anyhow', 'anymore', 'anyone', 'anything', 'anyway', 'anyways', 'anywhere', 
'apparently', 'approximately', 'are', 'aren', 'arent', 'arise', 'around', 'as', 'aside', 'ask', 'asking', 'at', 'auth', 'available', 'away', 'awfully', 'b', 
'back', 'be', 'became', 'because', 'become', 'becomes', 'becoming', 'been', 'before', 'beforehand', 'begin', 'beginning', 'beginnings', 'begins', 'behind', 
'being', 'believe', 'below', 'beside', 'besides', 'between', 'beyond', 'biol', 'both', 'brief', 'briefly', 'but', 'by', 'c', 'ca', 'came', 'can', 'cannot', 
"can't", 'cause', 'causes', 'certain', 'certainly', 'co', 'com', 'come', 'comes', 'contain', 'containing', 'contains', 'could', 'couldnt', 'd', 'date', 'did', 
"didn't", 'different', 'do', 'does', "doesn't", 'doing', 'done', "don't", 'down', 'downwards', 'due', 'during', 'e', 'each', 'ed', 'edu', 'effect', 'eg', 
'eight', 'eighty', 'either', 'else', 'elsewhere', 'end', 'ending', 'enough', 'especially', 'et', 'et-al', 'etc', 'even', 'ever', 'every', 'everybody', 
'everyone', 'everything', 'everywhere', 'ex', 'except', 'f', 'far', 'few', 'ff', 'fifth', 'first', 'five', 'fix', 'followed', 'following', 'follows', 'for', 
'former', 'formerly', 'forth', 'found', 'four', 'from', 'further', 'furthermore', 'g', 'gave', 'get', 'gets', 'getting', 'give', 'given', 'gives', 'giving', 
'go', 'goes', 'gone', 'got', 'gotten', 'h', 'had', 'happens', 'hardly', 'has', "hasn't", 'have', "haven't", 'having', 'he', 'hed', 'hence', 'her', 'here', 
'hereafter', 'hereby', 'herein', 'heres', 'hereupon', 'hers', 'herself', 'hes', 'hi', 'hid', 'him', 'himself', 'his', 'hither', 'home', 'how', 'howbeit', 
'however', 'hundred', 'i', 'id', 'ie', 'if', "i'll", 'im', 'immediate', 'immediately', 'importance', 'important', 'in', 'inc', 'indeed', 'index', 
'information', 'instead', 'into', 'invention', 'inward', 'is', "isn't", 'it', 'itd', "it'll", 'its', 'itself', "i've", 'j', 'just', 'k', 'keepkeeps', 'kept', 
'kg', 'km', 'know', 'known', 'knows', 'l', 'largely', 'last', 'lately', 'later', 'latter', 'latterly', 'least', 'less', 'lest', 'let', 'lets', 'like', 'liked', 
'likely', 'line', 'little', "'ll", 'look', 'looking', 'looks', 'ltd', 'm', 'made', 'mainly', 'make', 'makes', 'many', 'may', 'maybe', 'me', 'mean', 'means', 
'meantime', 'meanwhile', 'merely', 'mg', 'might', 'million', 'miss', 'ml', 'more', 'moreover', 'most', 'mostly', 'mr', 'mrs', 'much', 'mug', 'must', 'my', 
'myself', 'n', 'na', 'name', 'namely', 'nay', 'nd', 'near', 'nearly', 'necessarily', 'necessary', 'need', 'needs', 'neither', 'never', 'nevertheless', 'new', 
'next', 'nine', 'ninety', 'no', 'nobody', 'non', 'none', 'nonetheless', 'noone', 'nor', 'normally', 'nos', 'not', 'noted', 'nothing', 'now', 'nowhere', 'o', 
'obtain', 'obtained', 'obviously', 'of', 'off', 'often', 'oh', 'ok', 'okay', 'old', 'omitted', 'on', 'once', 'one', 'ones', 'only', 'onto', 'or', 'ord', 
'other', 'others', 'otherwise', 'ought', 'our', 'ours', 'ourselves', 'out', 'outside', 'over', 'overall', 'owing', 'own', 'p', 'page', 'pages', 'part', 
'particular', 'particularly', 'past', 'per', 'perhaps', 'placed', 'please', 'plus', 'poorly', 'possible', 'possibly', 'potentially', 'pp', 'predominantly', 
'present', 'previously', 'primarily', 'probably', 'promptly', 'proud', 'provides', 'put', 'q', 'que', 'quickly', 'quite', 'qv', 'r', 'ran', 'rather', 'rd', 
're', 'readily', 'really', 'recent', 'recently', 'ref', 'refs', 'regarding', 'regardless', 'regards', 'related', 'relatively', 'research', 'respectively', 
'resulted', 'resulting', 'results', 'right', 'run', 's', 'said', 'same', 'saw', 'say', 'saying', 'says', 'sec', 'section', 'see', 'seeing', 'seem', 'seemed', 
'seeming', 'seems', 'seen', 'self', 'selves', 'sent', 'seven', 'several', 'shall', 'she', 'shed', "she'll", 'shes', 'should', "shouldn't", 'show', 'showed', 
'shown', 'showns', 'shows', 'significant', 'significantly', 'similar', 'similarly', 'since', 'six', 'slightly', 'so', 'some', 'somebody', 'somehow', 'someone', 
'somethan', 'something', 'sometime', 'sometimes', 'somewhat', 'somewhere', 'soon', 'sorry', 'specifically', 'specified', 'specify', 'specifying', 'still', 
'stop', 'strongly', 'sub', 'substantially', 'successfully', 'such', 'sufficiently', 'suggest', 'sup', 'sure'])

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
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
        return []
    
    # get tokens (Ryan's tokenizer from assignment 1 w/o file)
    text = soup.get_text(separator=" ", strip=True)

    # check for access denied pages
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
    if len(tokens) < MIN_TOKEN:
        return []

    # defragment
    page_url = urldefrag(resp.raw_response.url)[0]

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
        href = tag["href"].strip()

        try:
            href = urljoin(base_url, href)
            href = urldefrag(href)[0]
        except ValueError:
            continue

        links.append(href)

    return links

def is_valid(url):
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
        
        # check all trap patterns
        full_url = parsed.path + ("?" + parsed.query if parsed.query else "")
        if (QUERY_TRAP_PATTERNS.search(full_url) or
            CALENDAR_TRAP_PATTERNS.search(full_url) or
            WIKI_TRAP_PATTERNS.search(full_url) or
            LOGIN_TRAP_PATTERNS.search(full_url)):
            return False

        return not re.match(
            r".*[./](css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|ppsx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|txt|sql"
            + r"|py|java|c|cpp|h|hpp|cc|cs|js|ts|jsx|tsx|rkt|makefile"
            + r"|json|yaml|yml|svg"
            + r"|sh|bash|zsh"
            + r"|log|cfg|ini|conf"
            + r"|ipynb"
            + r"|bib|nb|hs|lsp|scm|lif|m|als|dsp|ma|inc|mhcid|cls|ff|results|hqx|pov|edelsbrunner|class|ss|grm"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print("TypeError for ", parsed)
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