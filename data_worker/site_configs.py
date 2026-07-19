"""
Site configuration registry.

Each entry defines:
  - base_url      : root URL up to and including the search path
  - fixed_params  : query params that are always added (dict)
  - selectors     : XPath expressions for every field the spider extracts
"""

SITE_CONFIGS: dict = {

    "workplacerelations": {
        "base_url": "https://www.workplacerelations.ie/en/search/",
        "content_selector": "div.col-sm-9",  # Target selector for WRC core content

        "fixed_params": {
            "decisions": "1",
        },
        "selectors": {
            "records":   '//li[@class="each-item clearfix"]',
            "Id":        './/h2[@class="title"]/a/text()',
            "date":      './/span[@class="date"]/text()',
            "desc":      'string(.//p[@class="description"])',
            "refNo":     './/span[@class="refNO"]/text()',
            "doc_url":   './/h2[@class="title"]/a/@href',
            "next_page": '//a[@class="next"]/@href',
        },
    },
    
    # ── Add more sites below ──────────────────────────────────────────────
    # "another_site": {
    #     "base_url": "https://example.com/search/",
    #     "fixed_params": {},
    #     "selectors": { ... },
    # },
}
