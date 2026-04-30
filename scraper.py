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
MIN_TOKEN = 50 # low-value (dead url/empty page)
TRAP_PATTERNS = re.compile(
    r"("

    # Pagination
    # r"|/page/\d+" # https://ngs.ics.uci.edu/[blog/category/tag/author]/[...]/page/{num}/, all unique
    r"[?&]page=" # cml: same page dif subPage

    # Login / permissions
    r"|/(login|register)(/|$)"
    r"|/wp-admin|/wp-login"

    # Contacts
    r"|[?&]replytocom=" # only once on cloudberry

    # Format / redirect traps
    r"|[?&](format|version|view|download|redirect)="

    # Calendar / event / date archive
    r"|/calendar|/events"
    r"|[?&]ical="
    r"|/day/\d{4}-\d{2}-\d{2}"
    # r"|/\d{4}/\d{2}/" # valuable found
    # r"|/timeline" # TODO: check

    # DokuWiki
    r"|[?&]do="
    r"|[?&]idx="
    r"|projects:maint"

    # Code / Dir to src
    r"|/src(/|$)"
    r"|[?&]C=[NMSD];O=[AD]" # name/modifed/size/desc; asc/desc (lots in flamingo & ~smyth)

    r")",
    re.IGNORECASE
)

# stop words from https://www.ranks.nl/stopwords, set > list
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
    'stop', 'strongly', 'sub', 'substantially', 'successfully', 'such', 'sufficiently', 'suggest', 'sup', 'sure', 'the', 'there', 'their']) # added last 3

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

    # TODO: check if needed
    # if "Error: Forbidden" in text or \
    #     "Insufficient Access Privileges" in text or \
    #     "This page does not exist anymore" in text:
    #     return []

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

    # check visited, update if new (assuming no changes made)
    if page_url in visited:
        return _extract_links(soup, resp.raw_response.url)
    visited.add(page_url)

    # record page: url, word_count
    # TODO: which page (redirected?)
    with open(PAGES_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps({
            "url": page_url,
            "word_count": len(tokens)
        }) + "\n")

    # record word freq
    with open(WORDS_FILE, "a", encoding="utf-8") as f:
        for token in tokens:
            if len(token) > 2 and not token.isdigit() and token not in STOP_WORDS:
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

        try: # bad/invalid that are syntaxly correct links, ex: https://YOUR_IP_ADDRESS...
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
            host.endswith(".ics.uci.edu") or host == "ics.uci.edu"  or
            host.endswith(".cs.uci.edu") or host == "cs.uci.edu" or
            host.endswith(".informatics.uci.edu") or host == "informatics.uci.edu" or
            host.endswith(".stat.uci.edu") or host == "stat.uci.edu"
        )
        if not allowed:
            return False
        
        # check trap patterns
        full_url = parsed.path + ("?" + parsed.query if parsed.query else "")
        if TRAP_PATTERNS.search(full_url):
            return False

        return not re.match(
            r".*[./](css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|ppsx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"

            # other
            + r"|txt|sql" # text/data
            + r"|py|java|c|cpp|h|hpp|cc|cs|ts|jsx|tsx|rkt|makefile" # programming
            + r"|json|yaml|yml|svg" # markup / data formats
            + r"|sh|bash|zsh" # scripts
            + r"|log|cfg|ini|conf" # config / logs
            + r"|ipynb" # notebook
            + r"|bib|nb|hs|lsp|scm|lif|m|als|dsp|ma|inc|mhcid|cls|ff|results|hqx|pov|class|ss|grm" # misc edelsbrunner?? TODO: check

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