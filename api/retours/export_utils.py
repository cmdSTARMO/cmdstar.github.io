import csv
import io
from datetime import date, datetime
from typing import Iterable, Mapping

from fastapi.responses import Response


def csv_response(rows: Iterable[Mapping], filename: str) -> Response:
    data = list(rows)
    fieldnames = list(data[0].keys()) if data else []

    buffer = io.StringIO()
    buffer.write("\ufeff")
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in data:
        writer.writerow({key: _csv_value(value) for key, value in row.items()})

    return Response(
        content=buffer.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _csv_value(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value
