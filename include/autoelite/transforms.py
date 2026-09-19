import re


def to_snake(key: str) -> str:
    """Convert a PascalCase or camelCase field name to snake_case.

    Also strips Salesforce-isms:
      - __c suffix (custom fields)
      - Product2 → product, Pricebook2 → pricebook
    """
    key = re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower()
    key = re.sub(r"__c$", "", key)
    key = key.replace("product2_", "product_").replace("pricebook2_", "pricebook_")
    return key


def normalize_rows(rows: list[dict]) -> list[dict]:
    """Apply to_snake to all keys in a list of dicts."""
    return [{to_snake(k): v for k, v in row.items()} for row in rows]
