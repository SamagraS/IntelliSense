def ensure_key(data: dict, key: str):

    if key not in data:
        raise ValueError(f"Missing key: {key}")


def ensure_nested_key(data: dict, path: str):

    keys = path.split(".")

    current = data

    for k in keys:

        if k not in current:
            raise ValueError(f"Missing key: {path}")

        current = current[k]