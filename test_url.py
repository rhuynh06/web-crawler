from scraper import is_valid, TRAP_PATTERNS
from urllib.parse import urlparse

TEST_URLS = [
    "https://password.ics.uci.edu/ssp/index.php?action=resetbyquestions"
]

def test_url(url):
    print("=" * 80)
    print(f"URL: {url}")

    parsed = urlparse(url)

    if parsed.scheme not in ["http", "https"]:
        print("  ❌ Invalid scheme")
        return

    full_url = parsed.path + ("?" + parsed.query if parsed.query else "")
    if TRAP_PATTERNS.search(full_url):
        print("  ❌ Blocked by TRAP_PATTERNS")
    else:
        print("  ✅ Passed TRAP_PATTERNS")

    if is_valid(url):
        print("  ✅ is_valid = TRUE  (will crawl)")
    else:
        print("  ❌ is_valid = FALSE (will NOT crawl)")

def main():
    print("\n=== URL TESTS ===\n")
    for url in TEST_URLS:
        try:
            test_url(url)
        except Exception as e:
            print(f"  ⚠️ Error: {e}")

if __name__ == "__main__":
    main()