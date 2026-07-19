# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class LegalRecordItem(scrapy.Item):
    # ── Source identification ─────────────────────────────────────────────
    site             = scrapy.Field()   # Identifies the source layout key (e.g., "workplacerelations")
    body_id          = scrapy.Field()   # body ID passed via -a body=
    identifier       = scrapy.Field()   # case / decision reference number (e.g. "ADJ-00061411")

    # ── Record content ────────────────────────────────────────────────────
    title            = scrapy.Field()   # unspecified in the record search page???
    description      = scrapy.Field()   
    reference_number = scrapy.Field()
    publication_date = scrapy.Field()   

    # ── Document link ─────────────────────────────────────────────────────
    link_to_doc      = scrapy.Field()   # relative or absolute URL to the decision document

    # ── Partitioning ──────────────────────────────────────────────────────
    partition_date   = scrapy.Field()   # date used to partition storage (YYYY-MM-DD)
    partition_len    = scrapy.Field()   # number of records in the same partition batch

    # ── Audit / lineage ───────────────────────────────────────────────────
    scraped_at       = scrapy.Field()   # ISO-8601 UTC timestamp of when the item was scraped
    file_path        = scrapy.Field()   # path / object key inside the Bronze MinIO bucket
    file_hash        = scrapy.Field()   # SHA-256 hex digest of the stored document bytes

    # ── Transient Network Payload (Deleted before DB insert) ──────────────
    file_bytes       = scrapy.Field()
    content_type     = scrapy.Field()
