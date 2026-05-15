"""
Tests for HTMLPatternExtractor.

Covers JSON-LD, meta tags, embedded JSON, microdata, structural patterns,
merge priority, edge cases, and a realistic full-page scenario.
"""

import pytest
from harvest_ui.extraction.html_pattern_extractor import HTMLPatternExtractor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def extractor() -> HTMLPatternExtractor:
    return HTMLPatternExtractor()


# ---------------------------------------------------------------------------
# 1. JSON-LD — Product
# ---------------------------------------------------------------------------

def test_extract_json_ld_product():
    html = """
    <html><head>
    <script type="application/ld+json">
    {
      "@context": "https://schema.org",
      "@type": "Product",
      "name": "Acme Widget Pro",
      "sku": "WGT-9001",
      "description": "The best widget around.",
      "offers": {
        "@type": "Offer",
        "price": "49.99",
        "priceCurrency": "USD",
        "availability": "https://schema.org/InStock"
      }
    }
    </script>
    </head><body></body></html>
    """
    result = extractor().extract(html)
    assert result["name"] == "Acme Widget Pro"
    assert result["sku"] == "WGT-9001"
    assert result["price"] == 49.99
    assert result["currency"] == "USD"
    assert result["availability"] == "in_stock"


# ---------------------------------------------------------------------------
# 2. JSON-LD — Article
# ---------------------------------------------------------------------------

def test_extract_json_ld_article():
    html = """
    <html><head>
    <script type="application/ld+json">
    {
      "@context": "https://schema.org",
      "@type": "NewsArticle",
      "headline": "Breaking: Major Discovery Found",
      "author": [{"@type": "Person", "name": "Jane Doe"}],
      "datePublished": "2024-03-15",
      "description": "Scientists announce a major breakthrough."
    }
    </script>
    </head><body></body></html>
    """
    result = extractor().extract(html)
    assert result["headline"] == "Breaking: Major Discovery Found"
    assert result["author"] == ["Jane Doe"]
    assert result["datePublished"] == "2024-03-15"


# ---------------------------------------------------------------------------
# 3. JSON-LD — malformed block should not crash; other fields still extracted
# ---------------------------------------------------------------------------

def test_extract_json_ld_malformed_graceful():
    html = """
    <html><head>
    <script type="application/ld+json">
    { this is not valid json !!!
    </script>
    <script type="application/ld+json">
    {
      "@type": "Product",
      "name": "Valid Product",
      "sku": "VALID-001",
      "offers": {"price": "19.99", "priceCurrency": "USD"}
    }
    </script>
    </head></html>
    """
    result = extractor().extract(html)
    # No crash
    assert result.get("name") == "Valid Product"
    assert result.get("price") == 19.99


# ---------------------------------------------------------------------------
# 4. Meta tags — OG title and description
# ---------------------------------------------------------------------------

def test_extract_meta_og_tags():
    html = """
    <html><head>
    <meta property="og:title" content="My Awesome Page" />
    <meta property="og:description" content="A great description here." />
    <meta property="og:url" content="https://example.com/page" />
    </head><body></body></html>
    """
    result = extractor().extract(html)
    assert result["title"] == "My Awesome Page"
    assert result["description"] == "A great description here."
    assert result["canonical_url"] == "https://example.com/page"


# ---------------------------------------------------------------------------
# 5. Meta tags — product price
# ---------------------------------------------------------------------------

def test_extract_meta_product_price():
    html = """
    <html><head>
    <meta property="og:title" content="Blue Widget" />
    <meta property="product:price:amount" content="29.95" />
    <meta property="product:price:currency" content="GBP" />
    </head><body></body></html>
    """
    result = extractor().extract(html)
    assert result["price"] == 29.95
    assert result["currency"] == "GBP"


# ---------------------------------------------------------------------------
# 6. Embedded JSON — __NEXT_DATA__
# ---------------------------------------------------------------------------

def test_extract_next_data():
    html = (
        '<html><head>'
        '<script id="__NEXT_DATA__" type="application/json">'
        '{"props":{"pageProps":{"product":{"name":"Widget","price":9.99}}}}'
        '</script>'
        '</head><body></body></html>'
    )
    result = extractor().extract(html)
    assert result.get("name") == "Widget"
    assert result.get("price") == 9.99


# ---------------------------------------------------------------------------
# 7. Microdata — Product
# ---------------------------------------------------------------------------

def test_extract_microdata_product():
    html = """
    <html><body>
    <div itemscope itemtype="https://schema.org/Product">
      <span itemprop="name">Gadget X500</span>
      <span itemprop="sku">GDX-500</span>
      <span itemprop="description">A very fine gadget.</span>
      <span itemprop="price" content="99.00">$99.00</span>
      <span itemprop="priceCurrency" content="USD">USD</span>
    </div>
    </body></html>
    """
    result = extractor().extract(html)
    assert result.get("name") == "Gadget X500"
    assert result.get("sku") == "GDX-500"
    # price may come from content attr or text; either is acceptable
    assert result.get("price") == 99.0 or result.get("price_raw") is not None


# ---------------------------------------------------------------------------
# 8. Structural — ecommerce
# ---------------------------------------------------------------------------

def test_extract_structural_ecommerce():
    html = """
    <html><body>
    <h1>Super Widget Deluxe</h1>
    <p>Only <strong>$29.99</strong> — In Stock now!</p>
    <p>SKU: ABC-123</p>
    <button>Add to cart</button>
    </body></html>
    """
    result = extractor().extract(html, schema_hint="ecommerce product price sku")
    assert result.get("price") == 29.99
    assert result.get("currency") == "USD"
    assert result.get("sku") == "ABC-123"
    assert result.get("availability") == "in_stock"


# ---------------------------------------------------------------------------
# 9. Structural — news
# ---------------------------------------------------------------------------

def test_extract_structural_news():
    html = """
    <html><body>
    <h1>City Council Approves New Budget</h1>
    <p>By John Smith</p>
    <p>Published: 2024-01-15</p>
    <p>The city council voted unanimously to approve the new budget plan.</p>
    </body></html>
    """
    result = extractor().extract(html, schema_hint="news article headline author published")
    assert result.get("author") is not None
    assert "John Smith" in result["author"]
    assert result.get("published_date") == "2024-01-15"


# ---------------------------------------------------------------------------
# 10. Structural — legal
# ---------------------------------------------------------------------------

def test_extract_structural_legal():
    html = """
    <html><body>
    <h1>Case: Smith v. Jones</h1>
    <p>United States District Court, Southern District of New York</p>
    <p>Decision Date: 2023-11-20</p>
    <p>The district court ruled in favor of the plaintiff.</p>
    </body></html>
    """
    result = extractor().extract(html, schema_hint="legal case court citation plaintiff defendant")
    assert result.get("case_name") is not None
    assert "Smith" in result["case_name"]
    assert "Jones" in result["case_name"]
    assert result.get("court") is not None


# ---------------------------------------------------------------------------
# 11. Merge — JSON-LD beats meta for same field
# ---------------------------------------------------------------------------

def test_merge_prefers_json_ld():
    html = """
    <html><head>
    <meta property="og:title" content="Meta Title (lower priority)" />
    <script type="application/ld+json">
    {
      "@type": "Article",
      "headline": "JSON-LD Headline (higher priority)"
    }
    </script>
    </head><body></body></html>
    """
    result = extractor().extract(html)
    # JSON-LD provides headline; meta provides title — they are different keys,
    # so both should be present.  Critically, if both map to 'title', JSON-LD wins.
    assert result.get("headline") == "JSON-LD Headline (higher priority)"
    # meta title is still present under its own key
    assert result.get("title") == "Meta Title (lower priority)"


def test_merge_json_ld_name_beats_structural():
    """JSON-LD name should not be overwritten by structural h1 extraction."""
    html = """
    <html><head>
    <script type="application/ld+json">
    {"@type": "Product", "name": "JSON-LD Product Name", "offers": {"price": "5.00", "priceCurrency": "USD"}}
    </script>
    </head><body>
    <h1>Structural H1 Title</h1>
    </body></html>
    """
    result = extractor().extract(html, schema_hint="product ecommerce")
    # JSON-LD name must survive; structural title goes into 'title' key
    assert result.get("name") == "JSON-LD Product Name"


# ---------------------------------------------------------------------------
# 12. Empty input — no crash
# ---------------------------------------------------------------------------

def test_extract_empty_html_no_crash():
    result = extractor().extract("")
    assert result == {}


def test_extract_whitespace_only_no_crash():
    result = extractor().extract("   \n  ")
    assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# 13. Full realistic product page
# ---------------------------------------------------------------------------

def test_full_extraction_real_product_page():
    html = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Wireless Headphones XZ-900 | AudioStore</title>
  <meta property="og:title" content="Wireless Headphones XZ-900" />
  <meta property="og:description" content="Premium noise-cancelling wireless headphones." />
  <meta property="og:image" content="https://cdn.audiostore.com/xz900.jpg" />
  <meta property="product:price:amount" content="199.99" />
  <meta property="product:price:currency" content="USD" />

  <script type="application/ld+json">
  {
    "@context": "https://schema.org",
    "@type": "Product",
    "name": "Wireless Headphones XZ-900",
    "sku": "XZ-900-BLK",
    "description": "Premium noise-cancelling wireless headphones with 40hr battery.",
    "brand": {"@type": "Brand", "name": "AudioCorp"},
    "image": ["https://cdn.audiostore.com/xz900.jpg"],
    "offers": {
      "@type": "Offer",
      "price": "199.99",
      "priceCurrency": "USD",
      "availability": "https://schema.org/InStock"
    },
    "aggregateRating": {
      "@type": "AggregateRating",
      "ratingValue": "4.7",
      "reviewCount": "1284"
    }
  }
  </script>
</head>
<body>
  <h1>Wireless Headphones XZ-900</h1>
  <p class="price">$199.99</p>
  <p>SKU: XZ-900-BLK</p>
  <p>In Stock — ships in 24 hours.</p>
  <button>Add to cart</button>
  <p class="desc">Premium noise-cancelling wireless headphones with 40hr battery.</p>
</body>
</html>"""

    result = extractor().extract(html, schema_hint="ecommerce product price sku")

    # Core fields from JSON-LD
    assert result["name"] == "Wireless Headphones XZ-900"
    assert result["sku"] == "XZ-900-BLK"
    assert result["price"] == 199.99
    assert result["currency"] == "USD"
    assert result["availability"] == "in_stock"
    assert result["rating"] == 4.7
    assert result["review_count"] == 1284
    assert result["brand"] == "AudioCorp"
    # Meta/OG fields also present
    assert result.get("title") == "Wireless Headphones XZ-900"
    assert "description" in result
