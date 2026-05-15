"""
HTMLPatternExtractor — structural HTML extraction using pure stdlib.

Strategy (applied in order, results merged with JSON-LD taking priority):
  1. JSON-LD  (<script type="application/ld+json">)
  2. Meta tags (Open Graph, Twitter Card, product:*)
  3. Embedded JSON (__NEXT_DATA__, __NUXT__, window.__data__, etc.)
  4. Microdata (itemscope / itemtype / itemprop)
  5. RDFa    (property="schema:…")
  6. Structural patterns (domain heuristics via schema_hint)

No external dependencies — pure stdlib: re, json, html, urllib.parse.
"""

from __future__ import annotations

import html as _html_mod
import json
import re
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unescape(s: str) -> str:
    """HTML-unescape and strip whitespace."""
    return _html_mod.unescape(s).strip()


def _safe_float(val: Any) -> Optional[float]:
    """Convert val to float, returning None on failure."""
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _first_nonempty(*values: Any) -> Any:
    """Return first value that is not None and not empty string."""
    for v in values:
        if v is not None and v != "":
            return v
    return None


# ---------------------------------------------------------------------------
# Main extractor
# ---------------------------------------------------------------------------

class HTMLPatternExtractor:
    """Extracts structured data from HTML using DOM patterns.

    Works on both server-rendered and embedded-JSON pages.
    No external dependencies — pure stdlib regex + string operations.
    """

    def extract(self, content: str, schema_hint: str = "") -> Dict[str, Any]:
        """Main entry point. Returns structured dict from content.

        Strategy (try in order, merge results):
        1. _extract_json_ld       — schema.org JSON-LD blocks
        2. _extract_meta_tags     — og:title, og:description, og:price, etc.
        3. _extract_embedded_json — __NEXT_DATA__, window.__data__, etc.
        4. _extract_microdata     — itemtype/itemprop HTML attributes
        5. _extract_rdfa          — property="schema:name" etc.
        6. _extract_structural_patterns — CSS-like heuristics
        """
        if not content:
            return {}

        json_ld   = self._extract_json_ld(content)
        meta      = self._extract_meta_tags(content)
        embedded  = self._extract_embedded_json(content)
        microdata = self._extract_microdata(content)
        rdfa      = self._extract_rdfa(content)
        structural = self._extract_structural_patterns(content, schema_hint)

        return self._merge_results(json_ld, microdata, embedded, meta, rdfa, structural)

    # ------------------------------------------------------------------
    # 1. JSON-LD
    # ------------------------------------------------------------------

    def _extract_json_ld(self, content: str) -> Dict[str, Any]:
        """Extract and parse all <script type="application/ld+json"> blocks."""
        result: Dict[str, Any] = {}
        pattern = re.compile(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            re.IGNORECASE | re.DOTALL,
        )
        for m in pattern.finditer(content):
            raw = m.group(1).strip()
            try:
                data = json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                # Try to salvage by stripping trailing commas (common issue)
                try:
                    cleaned = re.sub(r",\s*([}\]])", r"\1", raw)
                    data = json.loads(cleaned)
                except Exception:
                    continue

            # data might be a list of objects (JSON-LD array)
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        result.update(self._flatten_json_ld(item))
            elif isinstance(data, dict):
                result.update(self._flatten_json_ld(data))

        return result

    def _flatten_json_ld(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten a single JSON-LD object by @type into known field names."""
        result: Dict[str, Any] = {}
        dtype = data.get("@type", "")
        if isinstance(dtype, list):
            dtype = dtype[0] if dtype else ""

        # --- Product / Offer ---
        if dtype in ("Product", "ItemPage"):
            if "name" in data:
                result["name"] = data["name"]
            if "description" in data:
                result["description"] = data["description"]
            if "sku" in data:
                result["sku"] = data["sku"]
            if "brand" in data:
                brand = data["brand"]
                result["brand"] = brand.get("name", brand) if isinstance(brand, dict) else brand
            if "image" in data:
                imgs = data["image"]
                result["images"] = imgs if isinstance(imgs, list) else [imgs]
            # Offer / price
            offer = data.get("offers") or data.get("offer")
            if offer:
                if isinstance(offer, list):
                    offer = offer[0]
                price_val = _safe_float(offer.get("price"))
                if price_val is not None:
                    result["price"] = price_val
                currency = offer.get("priceCurrency")
                if currency:
                    result["currency"] = currency
                avail = offer.get("availability", "")
                if avail:
                    avail_lower = avail.lower()
                    if "instock" in avail_lower or "in_stock" in avail_lower:
                        result["availability"] = "in_stock"
                    elif "outofstock" in avail_lower or "out_of_stock" in avail_lower:
                        result["availability"] = "out_of_stock"
                    elif "preorder" in avail_lower:
                        result["availability"] = "preorder"
            # Rating
            agg = data.get("aggregateRating")
            if agg and isinstance(agg, dict):
                rv = _safe_float(agg.get("ratingValue"))
                if rv is not None:
                    result["rating"] = rv
                rc = agg.get("reviewCount") or agg.get("ratingCount")
                if rc is not None:
                    result["review_count"] = int(rc)

        # --- Article / NewsArticle / BlogPosting ---
        elif dtype in ("Article", "NewsArticle", "BlogPosting", "WebPage"):
            if "headline" in data:
                result["headline"] = data["headline"]
            if "name" in data and "headline" not in result:
                result["headline"] = data["name"]
            if "description" in data:
                result["description"] = data["description"]
            author = data.get("author")
            if author:
                if isinstance(author, list):
                    result["author"] = [
                        a.get("name", a) if isinstance(a, dict) else a for a in author
                    ]
                elif isinstance(author, dict):
                    result["author"] = [author.get("name", "")]
                else:
                    result["author"] = [author]
            if "datePublished" in data:
                result["datePublished"] = data["datePublished"]
            if "dateModified" in data:
                result["dateModified"] = data["dateModified"]
            if "url" in data:
                result["canonical_url"] = data["url"]
            publisher = data.get("publisher")
            if publisher and isinstance(publisher, dict):
                result["publisher"] = publisher.get("name", "")

        # --- LegalCase / Event / Organization (generic fallback) ---
        else:
            # Generic extraction of top-level scalar/list fields
            for k, v in data.items():
                if k.startswith("@"):
                    continue
                if isinstance(v, (str, int, float, bool)):
                    result[k] = v
                elif isinstance(v, list) and all(isinstance(i, str) for i in v):
                    result[k] = v

        return result

    # ------------------------------------------------------------------
    # 2. Meta tags
    # ------------------------------------------------------------------

    def _extract_meta_tags(self, content: str) -> Dict[str, Any]:
        """Extract Open Graph, Twitter Card, and product:* meta tags."""
        result: Dict[str, Any] = {}

        # Match both <meta property="…" content="…"> and <meta name="…" content="…">
        pattern = re.compile(
            r'<meta\s[^>]*?(?:property|name)=["\']([^"\']+)["\'][^>]*?content=["\']([^"\']*)["\'][^>]*?>|'
            r'<meta\s[^>]*?content=["\']([^"\']*)["\'][^>]*?(?:property|name)=["\']([^"\']+)["\'][^>]*?>',
            re.IGNORECASE | re.DOTALL,
        )
        for m in pattern.finditer(content):
            if m.group(1):
                prop = m.group(1).strip()
                val  = _unescape(m.group(2))
            else:
                prop = m.group(4).strip()
                val  = _unescape(m.group(3))

            prop_lower = prop.lower()

            # Open Graph
            if prop_lower == "og:title":
                result["title"] = val
            elif prop_lower == "og:description":
                result.setdefault("description", val)
            elif prop_lower == "og:url":
                result["canonical_url"] = val
            elif prop_lower == "og:image":
                result.setdefault("images", [val])
            elif prop_lower == "og:type":
                result["og_type"] = val
            elif prop_lower == "og:site_name":
                result["site_name"] = val

            # Product price tags (Facebook / Pinterest commerce)
            elif prop_lower in ("product:price:amount", "og:price:amount"):
                pf = _safe_float(val)
                if pf is not None:
                    result["price"] = pf
            elif prop_lower in ("product:price:currency", "og:price:currency"):
                result["currency"] = val
            elif prop_lower == "product:availability":
                result["availability"] = val

            # Twitter Card
            elif prop_lower == "twitter:title":
                result.setdefault("title", val)
            elif prop_lower == "twitter:description":
                result.setdefault("description", val)
            elif prop_lower == "twitter:image":
                result.setdefault("images", [val])

            # Standard meta name tags
            elif prop_lower == "description":
                result.setdefault("description", val)
            elif prop_lower == "author":
                result.setdefault("author", [val])
            elif prop_lower == "keywords":
                result["keywords"] = [k.strip() for k in val.split(",") if k.strip()]

        return result

    # ------------------------------------------------------------------
    # 3. Embedded JSON
    # ------------------------------------------------------------------

    def _extract_embedded_json(self, content: str) -> Dict[str, Any]:
        """Extract JSON from __NEXT_DATA__, __NUXT__, window.__data__, etc."""
        result: Dict[str, Any] = {}

        # Patterns to look for
        embedded_patterns = [
            # __NEXT_DATA__ script tag
            r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>\s*(\{.*?\})\s*</script>',
            # __NUXT__ / Nuxt 3 useNuxtApp
            r'window\.__NUXT__\s*=\s*(\{.*?\});',
            r'window\.__NUXT_DATA__\s*=\s*(\[.*?\]);',
            # Generic window.__data__ / window.__INITIAL_STATE__ / window.__STATE__
            r'window\.__(?:data|DATA|INITIAL_STATE|STATE|STORE|APP_STATE|REDUX_STATE)__\s*=\s*(\{.*?\});',
            # Alpine.js / Vue data embedded
            r'window\.__initialData__\s*=\s*(\{.*?\});',
        ]

        for pat in embedded_patterns:
            for m in re.finditer(pat, content, re.DOTALL | re.IGNORECASE):
                raw = m.group(1)
                # Try to limit to balanced braces/brackets to avoid runaway matches
                data = self._try_parse_json(raw)
                if data is None:
                    data = self._try_parse_json_balanced(raw, content, m.start(1))
                if data is None:
                    continue

                extracted = self._dig_embedded(data)
                for k, v in extracted.items():
                    result.setdefault(k, v)

        return result

    def _try_parse_json(self, s: str) -> Optional[Any]:
        try:
            return json.loads(s)
        except (json.JSONDecodeError, ValueError):
            return None

    def _try_parse_json_balanced(self, raw: str, content: str, start: int) -> Optional[Any]:
        """Walk forward from start to find the balanced JSON object/array end."""
        if not raw:
            return None
        opener = raw[0]
        closer = "}" if opener == "{" else "]"
        depth = 0
        in_string = False
        escape = False
        for i, ch in enumerate(raw):
            if escape:
                escape = False
                continue
            if ch == "\\" and in_string:
                escape = True
                continue
            if ch == '"' and not in_string:
                in_string = True
                continue
            if ch == '"' and in_string:
                in_string = False
                continue
            if in_string:
                continue
            if ch == opener:
                depth += 1
            elif ch == closer:
                depth -= 1
                if depth == 0:
                    return self._try_parse_json(raw[: i + 1])
        return None

    def _dig_embedded(self, data: Any, _depth: int = 0) -> Dict[str, Any]:
        """Recursively search for product/article fields in embedded JSON."""
        result: Dict[str, Any] = {}
        if _depth > 5 or not isinstance(data, dict):
            return result

        # Direct field mapping
        field_map = {
            "name": "name", "title": "title", "headline": "headline",
            "price": "price", "sku": "sku", "description": "description",
            "author": "author", "datePublished": "datePublished",
            "availability": "availability", "currency": "currency",
            "image": "images", "images": "images",
        }
        for src, dst in field_map.items():
            if src in data and data[src] not in (None, "", [], {}):
                result.setdefault(dst, data[src])

        # Look inside known container keys
        for key in ("pageProps", "props", "product", "article", "page",
                    "initialData", "data", "payload", "item", "listing"):
            if key in data and isinstance(data[key], dict):
                child = self._dig_embedded(data[key], _depth + 1)
                for k, v in child.items():
                    result.setdefault(k, v)

        return result

    # ------------------------------------------------------------------
    # 4. Microdata
    # ------------------------------------------------------------------

    def _extract_microdata(self, content: str) -> Dict[str, Any]:
        """Parse itemscope/itemtype/itemprop HTML attributes."""
        result: Dict[str, Any] = {}

        # Find itemprop="…" content="…" or itemprop="…">value</tag>
        # Pattern 1: <tag itemprop="key" content="value">
        content_pat = re.compile(
            r'<[a-zA-Z]+[^>]+itemprop=["\']([^"\']+)["\'][^>]+content=["\']([^"\']*)["\'][^>]*>',
            re.IGNORECASE,
        )
        for m in content_pat.finditer(content):
            prop = m.group(1).strip()
            val  = _unescape(m.group(2))
            self._apply_itemprop(result, prop, val)

        # Pattern 1b: reversed attribute order
        content_pat2 = re.compile(
            r'<[a-zA-Z]+[^>]+content=["\']([^"\']*)["\'][^>]+itemprop=["\']([^"\']+)["\'][^>]*>',
            re.IGNORECASE,
        )
        for m in content_pat2.finditer(content):
            val  = _unescape(m.group(1))
            prop = m.group(2).strip()
            self._apply_itemprop(result, prop, val)

        # Pattern 2: <tag itemprop="key">text content</tag>
        text_pat = re.compile(
            r'<([a-zA-Z]+)[^>]+itemprop=["\']([^"\']+)["\'][^>]*>(.*?)</\1>',
            re.IGNORECASE | re.DOTALL,
        )
        for m in text_pat.finditer(content):
            prop = m.group(2).strip()
            # Strip inner tags from value
            inner = re.sub(r"<[^>]+>", " ", m.group(3))
            val   = _unescape(inner).strip()
            if val:
                self._apply_itemprop(result, prop, val)

        return result

    def _apply_itemprop(self, result: Dict[str, Any], prop: str, val: str) -> None:
        prop_lower = prop.lower()
        if prop_lower in ("name", "productname"):
            result.setdefault("name", val)
        elif prop_lower == "price":
            pf = _safe_float(val)
            if pf is not None:
                result.setdefault("price", pf)
            else:
                result.setdefault("price_raw", val)
        elif prop_lower == "pricecurrency":
            result.setdefault("currency", val)
        elif prop_lower == "sku":
            result.setdefault("sku", val)
        elif prop_lower == "description":
            result.setdefault("description", val)
        elif prop_lower == "image":
            result.setdefault("images", [val])
        elif prop_lower == "availability":
            result.setdefault("availability", val)
        elif prop_lower in ("headline", "articlebody"):
            result.setdefault("headline", val)
        elif prop_lower == "author":
            result.setdefault("author", [val])
        elif prop_lower in ("datepublished", "publishdate"):
            result.setdefault("datePublished", val)
        elif prop_lower == "ratingvalue":
            rv = _safe_float(val)
            if rv is not None:
                result.setdefault("rating", rv)
        elif prop_lower in ("reviewcount", "ratingcount"):
            try:
                result.setdefault("review_count", int(val))
            except (ValueError, TypeError):
                pass

    # ------------------------------------------------------------------
    # 5. RDFa
    # ------------------------------------------------------------------

    def _extract_rdfa(self, content: str) -> Dict[str, Any]:
        """Extract RDFa property attributes (property="schema:name", etc.)."""
        result: Dict[str, Any] = {}

        # <tag property="schema:name" content="value">
        rdfa_content = re.compile(
            r'<[a-zA-Z]+[^>]+property=["\']([^"\']+)["\'][^>]+content=["\']([^"\']*)["\'][^>]*>',
            re.IGNORECASE,
        )
        for m in rdfa_content.finditer(content):
            prop = m.group(1).strip().lower()
            val  = _unescape(m.group(2))
            self._apply_rdfa_prop(result, prop, val)

        # reversed
        rdfa_content2 = re.compile(
            r'<[a-zA-Z]+[^>]+content=["\']([^"\']*)["\'][^>]+property=["\']([^"\']+)["\'][^>]*>',
            re.IGNORECASE,
        )
        for m in rdfa_content2.finditer(content):
            val  = _unescape(m.group(1))
            prop = m.group(2).strip().lower()
            self._apply_rdfa_prop(result, prop, val)

        return result

    def _apply_rdfa_prop(self, result: Dict[str, Any], prop: str, val: str) -> None:
        # Strip namespace prefix if present
        if ":" in prop:
            prop = prop.split(":")[-1]
        if prop == "name":
            result.setdefault("name", val)
        elif prop == "headline":
            result.setdefault("headline", val)
        elif prop == "description":
            result.setdefault("description", val)
        elif prop == "price":
            pf = _safe_float(val)
            if pf is not None:
                result.setdefault("price", pf)
        elif prop == "datepublished":
            result.setdefault("datePublished", val)
        elif prop == "author":
            result.setdefault("author", [val])

    # ------------------------------------------------------------------
    # 6. Structural patterns
    # ------------------------------------------------------------------

    def _extract_structural_patterns(self, content: str, schema_hint: str) -> Dict[str, Any]:
        """Domain-specific structural extraction based on schema_hint."""
        result: Dict[str, Any] = {}
        hint_lower = schema_hint.lower()

        # Strip HTML tags to get visible text for pattern matching
        text = re.sub(r"<[^>]+>", " ", content)
        text = re.sub(r"\s+", " ", text).strip()
        text = _unescape(text)

        is_ecommerce = any(k in hint_lower for k in ("price", "product", "sku", "ecommerce", "shop", "buy"))
        is_news      = any(k in hint_lower for k in ("headline", "author", "article", "news", "published"))
        is_legal     = any(k in hint_lower for k in ("case", "court", "legal", "citation", "plaintiff", "defendant"))

        # --- Generic h1 as title ---
        h1 = re.search(r"<h1[^>]*>(.*?)</h1>", content, re.IGNORECASE | re.DOTALL)
        if h1:
            heading_text = re.sub(r"<[^>]+>", "", h1.group(1)).strip()
            heading_text = _unescape(heading_text)
            if heading_text:
                result["title"] = heading_text

        # --- First paragraph as summary ---
        p_match = re.search(r"<p[^>]*>(.*?)</p>", content, re.IGNORECASE | re.DOTALL)
        if p_match:
            p_text = re.sub(r"<[^>]+>", "", p_match.group(1)).strip()
            p_text = _unescape(p_text)
            if len(p_text) > 20:
                result["summary"] = p_text[:500]

        # --- Date patterns (generic) ---
        date_m = re.search(
            r"\b(\d{4}-\d{2}-\d{2}|\w{3,9}\s+\d{1,2},?\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})\b",
            text,
        )
        if date_m:
            result["date"] = date_m.group(1)

        # ------------------------------------------------------------------
        if is_ecommerce or (not is_news and not is_legal):
            # Price: $X.XX, £X.XX, €X.XX, X.XX USD, etc.
            price_m = re.search(
                r"(?:[\$£€¥₹]\s*(\d{1,6}(?:[.,]\d{2,3})?))"
                r"|(\d{1,6}(?:[.,]\d{2})?)\s*(?:USD|EUR|GBP|CAD|AUD)\b",
                text,
            )
            if price_m:
                raw_price = price_m.group(1) or price_m.group(2)
                pf = _safe_float(raw_price)
                if pf is not None:
                    result.setdefault("price", pf)
                # Detect currency symbol
                if "$" in price_m.group(0):
                    result.setdefault("currency", "USD")
                elif "£" in price_m.group(0):
                    result.setdefault("currency", "GBP")
                elif "€" in price_m.group(0):
                    result.setdefault("currency", "EUR")
                elif "USD" in price_m.group(0):
                    result.setdefault("currency", "USD")

            # SKU patterns
            sku_m = re.search(
                r"(?i)\b(?:sku|item\s*#?|model\s*#?|part\s*#?|product\s*id)[:\s#]+([A-Z0-9][\w\-]{2,24})\b",
                text,
            )
            if sku_m:
                result.setdefault("sku", sku_m.group(1))

            # Availability
            text_lower = text.lower()
            if "in stock" in text_lower or "add to cart" in text_lower or "add to bag" in text_lower:
                result.setdefault("availability", "in_stock")
            elif "out of stock" in text_lower or "sold out" in text_lower or "unavailable" in text_lower:
                result.setdefault("availability", "out_of_stock")
            elif "pre-order" in text_lower or "preorder" in text_lower:
                result.setdefault("availability", "preorder")

        # ------------------------------------------------------------------
        if is_news:
            # Byline: "By John Smith", "Author: Jane Doe"
            # Capture only 1–3 name words; stop at punctuation / line boundaries
            author_m = re.search(
                r"(?i)(?:^|\b)(?:by|author)[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})(?=\s*[,\.\|]|\s+(?:Published|Posted|Updated|on\b|\d{4})|\s*$)",
                text,
            )
            if author_m:
                result.setdefault("author", [author_m.group(1).strip()])

            # Published date — more specific than generic date
            pub_m = re.search(
                r"(?i)(?:published|posted|updated)[:\s]+(\d{4}-\d{2}-\d{2}|\w{3,9}\s+\d{1,2},?\s+\d{4})",
                text,
            )
            if pub_m:
                result.setdefault("published_date", pub_m.group(1))

            # h1 as headline
            if "title" in result:
                result.setdefault("headline", result["title"])

        # ------------------------------------------------------------------
        if is_legal:
            # Case name: "Smith v. Jones" or "Smith v Jones"
            case_m = re.search(
                r"([A-Z][a-zA-Z\s,\.]+\s+v\.?\s+[A-Z][a-zA-Z\s,\.]+)",
                text,
            )
            if case_m:
                result.setdefault("case_name", case_m.group(1).strip())

            # Court name
            court_m = re.search(
                r"(?i)(supreme court|court of appeals|district court|circuit court|"
                r"appellate court|superior court|bankruptcy court)",
                text,
            )
            if court_m:
                result.setdefault("court", court_m.group(1))

            # Citation: e.g. "123 U.S. 456" or "456 F.3d 789"
            citation_m = re.search(
                r"\b(\d+\s+(?:U\.S\.|F\.\d[a-z]*|S\.Ct\.|L\.Ed\.|A\.(?:\d[a-z]*))\s+\d+)\b",
                text,
            )
            if citation_m:
                result.setdefault("citation", citation_m.group(1))

            # Jurisdiction
            juris_m = re.search(
                r"(?i)\b(federal|state of [A-Z][a-z]+|[A-Z][a-z]+\s+(?:state|circuit))\b",
                text,
            )
            if juris_m:
                result.setdefault("jurisdiction", juris_m.group(1))

        return result

    # ------------------------------------------------------------------
    # Merge
    # ------------------------------------------------------------------

    def _merge_results(self, *dicts: Dict[str, Any]) -> Dict[str, Any]:
        """Merge extraction results.

        Priority (first dict = highest): JSON-LD, microdata, embedded, meta, rdfa, structural.
        For each key, the first non-None, non-empty value wins.
        """
        result: Dict[str, Any] = {}
        for d in dicts:
            for k, v in d.items():
                if k not in result:
                    result[k] = v
                else:
                    # Prefer non-empty value
                    existing = result[k]
                    if existing is None or existing == "" or existing == [] or existing == {}:
                        result[k] = v
        return result
