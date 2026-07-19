import os
import sys
from pathlib import Path

data_worker_path = Path(__file__).resolve().parents[3]
if str(data_worker_path) not in sys.path:
    sys.path.insert(0, str(data_worker_path))

from site_configs import SITE_CONFIGS  

###

import scrapy
from urllib.parse import urlencode
from datetime import datetime
from scraper.items import LegalRecordItem

class LegalRecordsSpiderSpider(scrapy.Spider):
    name = "legal_records_spider"

    # site      = "workplacerelations"
    # from_date = None
    # to_date   = None
    # body      = '2,1,3,15376'

    async def start(self):  # this func is linked in middleware.py 

        # Safely pull from kwargs passed via CLI (-a site=...) or fallback to env
        self.site = getattr(self, 'site', os.environ.get("DEFAULT_SITE_KEY"))
        self.body = getattr(self, 'body', os.environ.get("DEFAULT_TARGET_BODY_ID"))

        if self.site not in SITE_CONFIGS:
            raise ValueError(f"Unknown site '{self.site}'. Available: {list(SITE_CONFIGS.keys())}")

        self._config = SITE_CONFIGS[self.site]
        self.allowed_domains = [self._config["base_url"].split("/")[2]]

        if not self.from_date or not self.to_date:
            raise ValueError(f"CRITICAL: Both 'from_date' and 'to_date' arguments must be explicitly provided.")

        start_dt = datetime.strptime(self.from_date, "%d/%m/%Y")
        end_dt = datetime.strptime(self.to_date, "%d/%m/%Y")

        if start_dt > end_dt:
            raise ValueError(f"CRITICAL: Chronological failure. 'from_date' ({self.from_date}) cannot be later than 'to_date' ({self.to_date}).")

        self.calculated_partition_date = start_dt.strftime("%Y-%m-%d")
        self.calculated_partition_len = (end_dt - start_dt).days

        base_params = dict(self._config["fixed_params"])
        base_params["from"] = self.from_date
        base_params["to"]   = self.to_date

        body_ids = [b.strip() for b in self.body.split(",")] if self.body else [None]
        for body_id in body_ids:
            params = dict(base_params)
            if body_id:
                params["body"] = body_id
            url = self._config["base_url"] + "?" + urlencode(params)

            # Context-rich structured log event replacing raw prints            
            ctx = {
                "start_date": self.from_date,
                "end_date": self.to_date,
                "partition_len": self.calculated_partition_len,
                "body_id": body_id
            }
            self.logger.info(f"Initializing scraping sequence targeted window for body target", extra=ctx)
            yield scrapy.Request(url, callback=self.parse, cb_kwargs={"body_id": body_id})

    def parse(self, response, body_id=None):
        sel = self._config["selectors"]
        records = response.xpath(sel["records"])
        
        ctx = {
            "start_date": self.from_date,
            "end_date": self.to_date,
            "partition_len": self.calculated_partition_len,
            "body_id": body_id
        }

        for record in records:
            desc_raw = record.xpath(sel["desc"]).get()
            
            item = LegalRecordItem()
            item["site"] = self.site
            item["body_id"] = body_id
            item["identifier"] = record.xpath(sel["Id"]).get()
            item["title"] = record.xpath(sel["Id"]).get()                   # not found in WCR webpage; dup of "identifier"
            item["publication_date"] = record.xpath(sel["date"]).get()
            item["description"] = desc_raw.strip() if desc_raw else None
            item["reference_number"] = record.xpath(sel["refNo"]).get()
            item["partition_date"] = self.calculated_partition_date
            item["partition_len"] = self.calculated_partition_len
            
            # Resolve absolute URL dynamically based on the site domain
            raw_doc_url = record.xpath(sel["doc_url"]).get()
            if raw_doc_url:
                absolute_doc_url = response.urljoin(raw_doc_url)
                item["link_to_doc"] = absolute_doc_url
                
                # Yield a network request for the file, passing the item forward
                self.logger.info(f"Queueing async download for: {absolute_doc_url}", extra=ctx)
                yield scrapy.Request(
                    url=absolute_doc_url, 
                    callback=self.parse_document, 
                    cb_kwargs={"item": item}
                )
            else:
                self.logger.warning(f"Item {item['identifier']} missing link_to_doc. Yielding metadata only.", extra=ctx)
                yield item

        next_page = response.xpath(sel["next_page"]).get()
        if next_page:
            self.logger.info("Traversing pagination reference", extra=dict(ctx, next_page_url=next_page))
            yield response.follow(next_page, callback=self.parse, cb_kwargs={"body_id": body_id})

    def parse_document(self, response, item):
        """Asynchronously handles the downloaded file payload."""
        # Inject the raw bytes and content type into the item
        item["file_bytes"] = response.body
        
        content_type_header = response.headers.get("Content-Type", b"application/octet-stream")
        item["content_type"] = content_type_header.decode("utf-8")
        
        yield item