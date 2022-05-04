import csv
import os.path
import subprocess
from abc import ABC, abstractmethod
from io import StringIO
from pathlib import Path

import pymongo
import requests
import toml
import typer
from pymongo import IndexModel
from tqdm import tqdm
from typer import Typer
import pandas as pd
import numpy as np
from loguru import logger

app = Typer()


@app.command()
def split_manifest_to_segments(manifest_path: str = typer.Option(..., help='Path to manifest file'),
                               number_of_segments: int = typer.Option(...,
                                                                      help='Number of segments to split the data '
                                                                           'frame into. Number of concurrent '
                                                                           'downloads depends on it.'),
                               output_directory: str = typer.Option(...,
                                                                    help='Directory to output the resulting splits to')
                               ) -> str:
    base_name = Path(manifest_path).stem
    df = pd.read_csv(manifest_path, sep='\t')

    splits = np.array_split(df, number_of_segments)

    for i, df in enumerate(splits):
        df: pd.DataFrame
        df.to_csv(os.path.join(output_directory, f'{base_name}_{i}.txt'), sep='\t', index=False)

    return output_directory


@app.command()
def run_gdc_client_download_on_directory(
        manifests_directory: str = typer.Option(..., help='Directory containing manifest files'),
        number_of_concurrent_downloads: int = typer.Option(..., help='Number of simultaneous downloads to perform'),
        output_directory: str = typer.Option(..., help='Directory to dump the resulting files to'),
        manifests_regex_expression: str = typer.Option("*.txt",
                                                       help='Regex expression to select on specific manifest files')
):
    manifest_files = Path(manifests_directory).glob(manifests_regex_expression)
    commands = sorted(
        [f'gdc-client download -m {manifest_file} -d {output_directory} --debug' for manifest_file in manifest_files])

    for j in range(max(len(commands) // number_of_concurrent_downloads + 1, 1)):
        procs = [subprocess.Popen(i, shell=True) for i in
                 commands[
                 j * number_of_concurrent_downloads: min((j + 1) * number_of_concurrent_downloads, len(commands))
                 ]
                 ]
        for p in procs:
            p.wait()


class AbstractDatabaseInserter(ABC):
    def __init__(self, subject: str, base_dir: str, mongo_connection_string: str, db_name: str, col_name: str):
        self.subject = subject
        self.base_dir = base_dir
        self.mongo_connection_string = mongo_connection_string
        self.db_name = db_name

        self.info = self.request_file_info(data_type=subjects[self.subject])
        logger.debug(self.info.head())
        self.patient_file_map = self.make_patient_file_map(self.info, base_dir)
        client = pymongo.MongoClient(mongo_connection_string)
        logger.debug(client.server_info())
        db = client[db_name]
        self.col = db[col_name]

        self.col.drop()

        indexes = [IndexModel([('name', pymongo.ASCENDING)]),
                   IndexModel([('patient', pymongo.ASCENDING)]),
                   IndexModel([('sample', pymongo.ASCENDING)]),
                   IndexModel([('patient', pymongo.ASCENDING), ('name', pymongo.ASCENDING)]),
                   IndexModel([('sample', pymongo.ASCENDING), ('name', pymongo.ASCENDING)]),
                   IndexModel([('sample', pymongo.ASCENDING), ('patient', pymongo.ASCENDING)])
                   ]

        for patient, file_path in tqdm(self.patient_file_map.items()):
            self.insert_patient_data(patient=patient, file_path=file_path)

        self.col.create_indexes(indexes)

    @abstractmethod
    def insert_patient_data(self, patient: str, file_path: str):
        ...

    def request_file_info(self, data_type: str) -> pd.DataFrame:
        fields = [
            "file_name",
            "cases.submitter_id",
            "cases.samples.sample_type",
            "cases.project.project_id",
            "cases.project.primary_site",
        ]

        fields = ",".join(fields)

        files_endpt = "https://api.gdc.cancer.gov/files"

        filters = {
            "op": "and",
            "content": [
                {
                    "op": "in",
                    "content": {
                        "field": "files.experimental_strategy",
                        "value": [data_type]
                    }
                }
            ]
        }

        params = {
            "filters": filters,
            "fields": fields,
            "format": "TSV",
            "size": "200000"
        }

        response = requests.post(
            files_endpt,
            headers={"Content-Type": "application/json"},
            json=params)

        df = pd.read_csv(StringIO(response.content.decode("utf-8")), sep="\t")
        return df

    @staticmethod
    def make_patient_file_map(df, base_dir):
        return {row['cases.0.submitter_id']: os.path.join(
            base_dir, row.id, row.file_name)
            for _, row in df.iterrows()}


class mRNADatabaseInserter(AbstractDatabaseInserter):
    def insert_patient_data(self, patient: str, file_path: str):
        with open(file_path) as f:
            reader = csv.reader(f, delimiter='\t')
            data = list(reader)
        columns = data[1]
        samples = []
        for sample, row in enumerate(data[6:]):
            samples.append(
                {'name': row[1], 'value': row[-1], 'patient': patient,
                 'metadata': {columns[0]: row[0],
                              columns[2]: row[2],
                              columns[4]: row[4],
                              columns[5]: row[5],
                              columns[6]: row[6],
                              columns[7]: row[7],
                              }
                 }
            )

        self.col.insert_many(samples)

    def request_file_info(self, data_type) -> pd.DataFrame:
        df = super().request_file_info(data_type=data_type)
        df = df[
            df['cases.0.project.project_id'].str.startswith('TCGA')]
        df = df[
            df['file_name'].str.endswith('rna_seq.augmented_star_gene_counts.tsv')]
        df = df[
            df['cases.0.samples.0.sample_type'] == 'Primary Tumor']

        # When there is more than one file for a single patient just keep the first
        # (this is assuming they are just replicates and all similar)
        df = df[~df.duplicated(
            subset=['cases.0.submitter_id'], keep='first')]

        return df


class miRNADatabaseInserter(AbstractDatabaseInserter):
    def insert_patient_data(self, patient: str, file_path: str):
        with open(file_path) as f:
            reader = csv.reader(f, delimiter='\t')
            data = list(reader)
        columns = data[0]
        samples = []
        for sample, row in enumerate(data[6:]):
            samples.append(
                {'name': row[0], 'value': row[-2], 'patient': patient,
                 'metadata': {columns[1]: row[1],
                              columns[-1]: row[-1],
                              }
                 }
            )
        self.col.insert_many(samples)

    def request_file_info(self, data_type: str) -> pd.DataFrame:
        df = super(miRNADatabaseInserter, self).request_file_info(data_type=data_type)
        df = df[
            df['cases.0.project.project_id'].str.startswith('TCGA')]
        df = df[
            df['file_name'].str.endswith('mirbase21.mirnas.quantification.txt')]
        df = df[
            df['cases.0.samples.0.sample_type'] == 'Primary Tumor']

        # When there is more than one file for a single patient just keep the first
        # (this is assuming they are just replicates and all similar)
        df = df[~df.duplicated(
            subset=['cases.0.submitter_id'], keep='first')]

        return df


class DNAMethylationDatabaseInserter(AbstractDatabaseInserter):
    def insert_patient_data(self, patient: str, file_path: str):
        with open(file_path) as f:
            reader = csv.reader(f, delimiter='\t')
            data = list(reader)
        samples = []
        for sample, row in enumerate(data[6:]):
            samples.append(
                {'name': row[0], 'value': row[1], 'patient': patient}
            )

        self.col.insert_many(samples)

    def request_file_info(self, data_type: str) -> pd.DataFrame:
        df = super().request_file_info(data_type=data_type)
        df = df[
            df['cases.0.project.project_id'].str.startswith('TCGA')]
        df = df[
            df['file_name'].str.endswith('methylation_array.sesame.level3betas.txt')]
        df = df[
            df['cases.0.samples.0.sample_type'] == 'Primary Tumor']

        # When there is more than one file for a single patient just keep the first
        # (this is assuming they are just replicates and all similar)
        df = df[~df.duplicated(
            subset=['cases.0.submitter_id'], keep='first')]

        return df


subjects = dict(mRNA='RNA-Seq',
                DNAm='Methylation Array',
                miRNA='miRNA-Seq')
inserters = dict(mRNA=mRNADatabaseInserter,
                 miRNA=miRNADatabaseInserter,
                 DNAm=DNAMethylationDatabaseInserter
                 )


@app.command()
def insert_data(subject: str = typer.Option(...), base_dir: str = typer.Option(...),
                mongo_connection_string: str = typer.Option(...),
                db_name: str = typer.Option(...),
                col_name: str = typer.Option(None)
                ):
    col_name = col_name or subject
    inserter = inserters[subject]
    inserter(subject=subject,
             base_dir=base_dir,
             mongo_connection_string=mongo_connection_string,
             db_name=db_name,
             col_name=col_name)


if __name__ == '__main__':
    app()
