import csv
import os.path
import subprocess
from abc import ABC, abstractmethod
from io import StringIO
from pathlib import Path
import json

import pymongo
import requests

import typer
from loguru import logger
from pymongo import IndexModel, MongoClient
from tqdm import tqdm
from typer import Typer
import pandas as pd
import numpy as np

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


@app.command()
def insert_data(
        subject: str = typer.Option(..., help='Omics data type, e.g.: ["mRNA", "DNAm", "miRNA"]',
                                    prompt_required=True),
        base_dir: str = typer.Option(...,
                                     help='Directory in which the downloaded files are located at. Should '
                                          'conform with the files downloaded from the GDC manifest',
                                     prompt_required=True),
        mongo_connection_string: str = typer.Option(...,
                                                    help='Connection string used to connect to MongoDB. Must '
                                                         'have read/write privileges on the database',
                                                    prompt_required=True),
        db_name: str = typer.Option(..., help='Database name being written to', prompt_required=True),
        override: bool = typer.Option(...,
                                      help='If True, the existing collection will be dropped and a new one will be '
                                           'written instead, otherwise, will attempt to draw all existing patients '
                                           'names and continue parsing only for missing patients'),
        col_name: str = typer.Option(None, help='Optional collection name. If not provided, "subject" will be used.')
):
    col_name = col_name or subject
    inserter: AbstractDatabaseInserter = inserters[subject]
    inserter(
        subject=subject,
        base_dir=base_dir,
        mongo_connection_string=mongo_connection_string,
        db_name=db_name,
        col_name=col_name,
        override=override
    )


class AbstractDatabaseInserter(ABC):
    def __init__(self,
                 subject: str,
                 base_dir: str,
                 mongo_connection_string: str,
                 db_name: str,
                 col_name: str,
                 override: bool = False):
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

        if override:
            self.col.drop()
        else:
            existing_patients = self.col.distinct('patient')
            self.patient_file_map = {key: value for key, value in self.patient_file_map.items() if
                                     key not in existing_patients}

        indexes = [IndexModel([('name', pymongo.ASCENDING)]),
                   IndexModel([('patient', pymongo.ASCENDING)]),
                   IndexModel([('sample', pymongo.ASCENDING)]),
                   IndexModel([('patient', pymongo.ASCENDING), ('name', pymongo.ASCENDING)]),
                   IndexModel([('sample', pymongo.ASCENDING), ('name', pymongo.ASCENDING)]),
                   IndexModel([('sample', pymongo.ASCENDING), ('patient', pymongo.ASCENDING)])
                   ]
        self.col.create_indexes(indexes)
        
        for patient, file_path in tqdm(self.patient_file_map.items()):
            if not Path(file_path).is_file():
                logger.error(f'Unable to insert files for {patient}:{file_path}')
                continue
            self.insert_patient_data(patient=patient, file_path=file_path)
        
        
        
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
                {'name': row[1], 'value': float(row[-1]), 'patient': patient,
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
                {'name': row[0], 'value': float(row[-2]), 'patient': patient,
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
    def __init__(self, subject: str, base_dir: str, mongo_connection_string: str, db_name: str, col_name: str, override: bool = False):
        self._genes = set(pd.read_csv(Path(__file__).parent.joinpath('../DNAm_genes.csv'))['gene'].tolist())
        super().__init__(subject, base_dir, mongo_connection_string, db_name, col_name, override)
        
        
    def insert_patient_data(self, patient: str, file_path: str):
        with open(file_path) as f:
            reader = csv.reader(f, delimiter='\t')
            data = list(reader)
        samples = []

        def convert_to_float(num: str):
            try:
                return float(num)
            except ValueError:
                if num == 'NA':
                    return None
                else:
                    raise ValueError

        for sample, row in enumerate(data[6:]):
            if row[0] not in self._genes:
                continue
            samples.append(
                {'name': row[0], 'value': convert_to_float(row[1]), 'patient': patient}
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


@app.command()
def generate_variance_table(mongo_connection_string: str = typer.Option(..., help='MongoDB connection string'),
                            db_name: str = typer.Option(..., help='Database name'),
                            col_name: str = typer.Option(..., help='Collection to compute variance on'),
                            output_path: str = typer.Option(None,
                                                            help="Path to output the resulting variance table. If "
                                                                 "None, will be printed to stdout and returned"),
                            override: bool = typer.Option(False,
                                                          help='If True, will override existing variance table with '
                                                               'the same name'),
                            names_file: str = typer.Option(None,
                                                           help='Path to a preprocessed name file. Used in cases '
                                                                'where it takes too long to fetch the features for '
                                                                'the collection')):
    def get_values_for_name(name: str):
        return pd.DataFrame(db[col_name].find({'name': name}))

    p = Path(output_path)
    if p.exists() and not override:
        df = pd.read_csv(p, sep='\t')
        logger.debug(df.head())
        return df
    p.mkdir(parents=True, exist_ok=True)

    with MongoClient(mongo_connection_string) as client:
        logger.debug(client.server_info())
        db = client[db_name]
        if names_file:
            with open(names_file) as f:
                names = f.read().split('\n')

        else:
            names = db[col_name].distinct('name')
        df = pd.DataFrame([{'name': name, 'Var': get_values_for_name(name=name).value.var()} for name in
                           tqdm(names)])

    df = df.set_index('name')
    df.index.name = None

    df.to_csv(p, sep='\t')
    logger.debug(df.head())
    return df


subjects = dict(mRNA='RNA-Seq',
                DNAm='Methylation Array',
                miRNA='miRNA-Seq')
inserters = dict(mRNA=mRNADatabaseInserter,
                 miRNA=miRNADatabaseInserter,
                 DNAm=DNAMethylationDatabaseInserter
                 )

class AbstractVarianceComputer(ABC):
    def __init__(self, base_dir: str, ext: str, output_path: str) -> None:
        self.files = list(self.get_files(base_dir, ext))
        
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.parse_variance()
        
    def get_files(self, base_dir: str, ext: str):
        base_dir = Path(base_dir)
        assert base_dir.exists()
        
        return base_dir.glob(f'**/*.{ext}')

    @staticmethod
    def tofloat(num):
        try:
            return float(num)
            
        except ValueError:
            return None
    
    @abstractmethod
    def parse_file(file) -> dict:
        ...
        
    def parse_variance(self):
        out = dict()
        
        for file in tqdm(self.files):
            parsed = self.parse_file(file)
            
            for key, values in parsed.items():
                if key not in out:
                    out[key] = dict(sum=0., ssum=0., count=0)
                out[key]['sum'] += values['sum']
                out[key]['ssum'] += values['ssum']
                out[key]['count'] += values['count']
        with open(self.output_path, 'w') as f:
            json.dump(out, f, indent=2)
    
    
            
        
class mRNAVarianceComputer(AbstractVarianceComputer):
    def parse_file(self, file) -> dict:
        with Path(file).open() as f:
            reader = csv.reader(f, delimiter='\t')
        
            data = list(reader)[6:]
        out = dict()
        for item in data:
            if item[0] not in out.keys():
                out[item[0]] = dict(sum=0., ssum=0., count=0)
                
            val = self.tofloat(item[-1])
            if val is not None:
                out[item[0]]['count'] += 1
                out[item[0]]['sum'] = val
                out[item[0]]['ssum'] = val ** 2
            
        return out
    
class DNAmVarianceComputer(AbstractVarianceComputer):
    def parse_file(self, file) -> dict:
        with Path(file).open() as f:
            reader = csv.reader(f, delimiter='\t')
            
            data = list(reader)
            
        out = dict()
        for item in data:
            if item[0] not in out.keys():
                out[item[0]] = dict(sum=0., ssum=0., count=0)
                
            val = self.tofloat(item[-1])
            if val is not None:
                out[item[0]]['count'] += 1
                out[item[0]]['sum'] = val
                out[item[0]]['ssum'] = val ** 2
            
        return out
    
variance_computers = dict(mRNA=mRNAVarianceComputer,
                          DNAm=DNAmVarianceComputer)

@app.command()
def compute_variance(subject: str = typer.Option(..., help='Omics data type, e.g.: ["mRNA", "DNAm", "miRNA"]',
                                    prompt_required=True),
                    base_dir: str = typer.Option(...,
                                     help='Directory in which the downloaded files are located at. Should '
                                          'conform with the files downloaded from the GDC manifest',
                                     prompt_required=True),
                    file_extension: str = typer.Option(..., 
                                                       help='Extension of the files that should be seeked'),
                    output_file: str = typer.Option(..., help='Pathway to output the variace compute file')
                    ):
    computer = variance_computers[subject](base_dir=base_dir, ext=file_extension, output_path=output_file).parse_variance()
    

if __name__ == '__main__':
    app()
