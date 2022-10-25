#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd
import tempfile

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    logger.info("Downloading the input artifact..")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)

    logger.info("Cleaning up data..")

    # filtering data based on min and max price
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    # replace NaNs with empty strings
    df['last_review'].fillna(value='', inplace=True)
    df['reviews_per_month'].fillna(value='', inplace=True)

    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    with tempfile.NamedTemporaryFile(mode='wb+') as fp:
        df.to_csv(fp.name, index=False)
        artifact = wandb.Artifact(
            name=args.output_artifact,
            type=args.output_type,
            description=args.output_description
        )
        artifact.add_file(fp.name)
        run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact",
        type=str,
        help="input csv file with the raw data",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="output csv file after the cleaning",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="name of the output data",
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="description about the output data",
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="minimum rent price that will be kept in the data",
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="maximum rent price that will be kept in the data",
        required=True
    )


    args = parser.parse_args()

    go(args)
