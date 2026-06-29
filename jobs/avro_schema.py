"""
Defining kafka Avro schema
"""


STREAMING_AVRO_SCHEMA_JSON = """
{
  "type": "record",
  "name": "ProductRecord",
  "fields": [
    {"name": "title", "type": ["null", "string"], "default": null},
    {"name": "name", "type": ["null", "string"], "default": null},
    {"name": "product_current_price", "type": ["null", "string"], "default": null},
    {"name": "product_old_price", "type": ["null", "string"], "default": null},
    {"name": "product_discount", "type": ["null", "string"], "default": null},
    {"name": "product_url", "type": ["null", "string"], "default": null},
    {"name": "product_image_url", "type": ["null", "string"], "default": null},
    {"name": "product_seller", "type": ["null", "string"], "default": null},
    {"name": "product_availability", "type": ["null", "string"], "default": null},
    {"name": "product_category", "type": ["null", "string"], "default": null},
    {"name": "product_subcategory", "type": ["null", "string"], "default": null},
    {"name": "product_unit", "type": ["null", "string"], "default": null},
    {"name": "product_weight", "type": ["null", "string"], "default": null},
    {"name": "scraping_time", "type": ["null", "string"], "default": null},
    {"name": "timestamp_timezone", "type": ["null", "string"], "default": null},
    {"name": "product_brand", "type": ["null", "string"], "default": null},
    {"name": "product_ram", "type": ["null", "string"], "default": null},
    {"name": "product_storage", "type": ["null", "string"], "default": null}
  ]
}
"""