def format_citation(source: str, page: int | None = None) -> str:

    if not source:
        return ""

    source = source.strip()

    if page:
        return f"[Source: {source}, Page {page}]"

    return f"[Source: {source}]"