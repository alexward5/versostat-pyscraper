from typing import Any


def map_pandas_dtype_to_postgres(dtype: Any) -> str:
    """Map pandas dtype to PostgreSQL column type."""
    dtype_str = str(dtype).lower()
    
    if "int" in dtype_str:
        return "INTEGER"
    elif "float" in dtype_str:
        return "DOUBLE PRECISION"
    elif "bool" in dtype_str:
        return "BOOLEAN"
    elif "datetime" in dtype_str:
        return "TIMESTAMP"
    elif "date" in dtype_str:
        return "DATE"
    else:
        # Default to TEXT for string and unknown types
        return "TEXT"
