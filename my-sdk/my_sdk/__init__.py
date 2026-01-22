

from typing import Any

__version__ = "0.0.1"
__all__ = ["sql"]

def get_provider_info() -> dict[str, Any]:
    return {
        "name": "My SDK",
        "package-name": "my-sdk",
        "version": [__version__],
        "description": "My SDK is a package that provides a set of tools for buildin Airflow DAGs.",
        "task-decorators":[
            {
                "name": "sql",
                "class-name": "my_sdk.decorators.sql.sql_task"
            }
        ]
    }