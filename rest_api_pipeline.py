# TODO Fix this whole damn thing, is very slow and doesnt seem to be working as intended

from typing import Any, Optional

import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dlt.sources.rest_api import (
    RESTAPIConfig,
    rest_api_resources,
)


@dlt.source(name="coordinador", parallelized=True)
def coordinador_source(access_token: Optional[str] = dlt.secrets.value) -> Any:
    # Create a REST API configuration for the GitHub API
    # Use RESTAPIConfig to get autocompletion and type checking
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://sipub.api.coordinador.cl:443/costo-marginal-real/v4/"
        },
        "resource_defaults": {"write_disposition": "replace"},
        "resources": [
            {
                "name": "Costo Marginal Real by date",
                "endpoint": {
                    "path": "findByDate",
                    "method": "GET",
                    "data_selector": "data",
                    "paginator": PageNumberPaginator(
                        total_path="totalPages",
                        base_page=1,
                        page_param="page",
                    ),
                    # Query parameters for the endpoint
                    "params": {
                        "user_key": access_token,
                        "startDate": pendulum.today()
                        .subtract(days=30)
                        .format("YYYY-MM-DD"),
                        "endDate": pendulum.today().format("YYYY-MM-DD"),
                        "limit": 10000,
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)


def load_prec_marg() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="precio_real_bydate",
        destination="duckdb",
        dataset_name="precioReal_rest_api_data",
    )

    load_info = pipeline.run(
        coordinador_source(dlt.secrets["sources.rest_api.coordinador_sip_token"])
    )
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    load_prec_marg()
