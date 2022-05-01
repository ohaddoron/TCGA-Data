import os.path
import subprocess
from pathlib import Path

import typer
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
        df.to_csv(os.path.join(output_directory, f'{base_name}_{i}.txt'))

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
        [f'gdc-client download -m {manifest_file} -d {output_directory}' for manifest_file in manifest_files])

    for j in range(max(len(commands) // number_of_concurrent_downloads + 1, 1)):
        procs = [subprocess.Popen(i, shell=True) for i in
                 commands[
                 j * number_of_concurrent_downloads: min((j + 1) * number_of_concurrent_downloads, len(commands))
                 ]
                 ]
        for p in procs:
            p.wait()


if __name__ == '__main__':
    app()
