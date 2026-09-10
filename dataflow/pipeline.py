import argparse
import csv
import io

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class ParseOrders(beam.DoFn):

    def process(self, line):

        row = next(csv.reader(io.StringIO(line)))

        # Skip CSV header
        if row[0] == "order_id":
            return

        quantity = int(row[4])
        price = float(row[5])

        yield {
            "order_id": row[0],
            "customer_id": row[1],
            "product": row[2],
            "category": row[3],
            "quantity": quantity,
            "price": price,
            "order_date": row[6],
            "total_amount": quantity * price,
        }


def run():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
        help="GCS path to input CSV"
    )

    parser.add_argument(
        "--output_table",
        required=True,
        help="BigQuery table in project.dataset.table format"
    )

    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args)

    table_schema = {
        "fields": [
            {"name": "order_id", "type": "STRING"},
            {"name": "customer_id", "type": "STRING"},
            {"name": "product", "type": "STRING"},
            {"name": "category", "type": "STRING"},
            {"name": "quantity", "type": "INTEGER"},
            {"name": "price", "type": "FLOAT"},
            {"name": "order_date", "type": "DATE"},
            {"name": "total_amount", "type": "FLOAT"},
        ]
    }

    with beam.Pipeline(options=pipeline_options) as pipeline:

        (
            pipeline
            | "Read CSV from GCS"
            >> beam.io.ReadFromText(known_args.input)

            | "Parse Orders"
            >> beam.ParDo(ParseOrders())

            | "Write to BigQuery"
            >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema=table_schema,
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                ),
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                ),
            )
        )


if __name__ == "__main__":
    run()
