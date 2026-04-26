import re
from urllib.parse import urlparse, urljoin, urldefrag
from collections import defaultdict
from bs4 import BeautifulSoup
import hashlib

'''
allowed URL paths:
 *.ics.uci.edu/*
 *.cs.uci.edu/*  
 *.informatics.uci.edu/* 
 *.stat.uci.edu/*
'''

# for report
visited = set() # unique pages
word_freq = defaultdict(int) # word: count
page_freq = {} # url: word count
subdomains = defaultdict(set) # subdomain: set of pages
fingerprints = set() # dupe-detection

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

    # parse HTML
    try:
        soup = BeautifulSoup(content, "lxml")
    except Exception:
        return [] # skip page if crash/smt goes wrong
    
    # get tokens (Ryan's tokenizer from assignment 1 w/o file)
    text = soup.get_text(seperator=" ", strip=True) # strip HTML, just actual text
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
    
    # check for near-dupe detection?
    # fingerprints
    

    # defragment
    url = urldefrag(url)[0] # [url, fragment]

    # update visited
    visited.add(url)

    # stats for report (word freq, longest page, subdomains)
    for token in tokens:
        if token not in STOP_WORDS:
            word_freq[token] += 1

    page_freq[url] = len(tokens)

    parsed = urlparse(url) # [scheme/protocol (http), netloc (domain), path (/), query (?), fragment (#)]
    host = parsed.netloc.lower()
    if host.endswith(".ics.uci.edu"):
        subdomains[host].add(url)

    # extract new links to crawl
    links = []
    for tag in soup.find_all("a", href=True):
        # <a href="link">
        href = tag["href"].strip()

        # base + relative -> absolute url (full link on web)
        href = urljoin(resp.raw_response.url, href)

        # defragment
        href = urldefrag(href)[0]

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
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise