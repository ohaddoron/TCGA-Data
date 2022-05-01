# TCGA Download Repo

This repo was created to ease the download of TCGA patients data from the GDC portal. This is designed and based
on [multisurv](https://github.com/luisvalesilva/multisurv/tree/master/data) but is updated to download the data as it is
now stored, true to May2022.

All data used in the study are from the The Cancer Genome Atlas (TCGA) program, which includes a rich body of imaging,
clinical, and molecular data from 11,315 cases of 33 different cancer
types ([Weinstein et al., Nat Genet 2013](https://www.nature.com/articles/ng.2764)). The data are made available by the
National Cancer Institute (NCI) Genomic Data Commons (GDC) information system, publicly accessible at
the [GDC Data Portal](https://portal.gdc.cancer.gov/).

## Manifest Downloading Scripts

In order to speed up

## Download

<p>
  •
  <a href="#clinical-data">Clinical data</a><br />
  •
  <a href="#gene-expression">Gene expression</a><br />
  •
  <a href="#mirna-expression">miRNA expression</a><br />
  •
  <a href="#dna-methylation">DNA methylation</a><br />
  •
  <a href="#copy-number-variation">Copy number variation</a><br />
  •
  <a href="#whole-slide-images-(wsi)">Whole-slide images (WSI)</a>
</p>

### Clinical Data

Downloading the clinical data is done using a simple R script.

To download the data, one must first install
the [`TCGAbiolinks`](https://bioconductor.org/packages/release/bioc/html/TCGAbiolinks.html) package.

Inside an R console run the following script:

   ```r
   # Install the TCGAbiolinks pacakge
   if (!require("BiocManager", quietly = TRUE))
    install.packages("BiocManager")

    BiocManager::install("TCGAbiolinks")

    # Download data for all TCGA projects

    project_ids <- stringr::str_subset(TCGAbiolinks::getGDCprojects()$project_id, 'TCGA')
    
    data <- list()
    
    for (project_id in project_ids) { data[[project_id]] <- TCGAbiolinks::GDCquery_clinic(project=project_id,
    type='clinical')
    }
    
    # Merge into single table
    
    # (the "disease" column identifies each original table)
    
    data <- do.call(dplyr::bind_rows, data)
    
    # Write to file
    
    output_path <- '/mnt/dataA/TCGA/raw/clinical_data.tsv' readr::write_tsv(data, output_path)

   ```

### GDC Client

All downloads from this segments and onward are done using
the [GDC Data Transfer Tool](https://gdc.cancer.gov/access-data/gdc-data-transfer-tool). It is highly recommended to use
the UI version of the tool and for it all to be run on a unix based machine. If only a Windows machine is available,
[WSL2](https://docs.microsoft.com/en-us/windows/wsl/install) is a fine option.

---
**NOTE**

Make sure to add the gdc-client tool to your path.

---

### Manifest Downloading Scheme

Once any manifest has been downloaded, please follow these instructions in order to download the actual data files in a
concurrent manner.

1. Split the manifest into multiple segments so these can be fed concurrently to the gdc-client:

    ```shell
    python scripts/utils.py split-manifest-to-segments --manifest-path=<...> --number-of-segments=<...> --output-directory=<...>
    ```
2. Concurrent download all the raw data to a specified directory

    ```shell
    python scripts/utils.py run-gdc-client-download-on-directory --manifests-directory=<...> --number-of-concurrent-downloads=<...> --output-directory=<...> --manifests-regex-expression=<...>
    ```

### Gene Expression

The data are provided either as read counts or FPKM/FPKM-UQ. FPKM is designed for within-sample gene comparisons and has
actually fallen out of favor since the normalized gene values it produces do not add up to one million exactly. In
practice, however, the deviation from one million is not dramatic and it often works well enough. Given that normalizing
such a large number of samples is challenging, here I will use the FPKM-UQ data.

The GeneExpression data used in the original Multisurv paper was since updated and the manifest for it can now be
downloaded [here](https://portal.gdc.cancer.gov/repository?filters=%7B%22op%22%3A%22and%22%2C%22content%22%3A%5B%7B%22op%22%3A%22in%22%2C%22content%22%3A%7B%22field%22%3A%22files.data_category%22%2C%22value%22%3A%5B%22transcriptome%20profiling%22%5D%7D%7D%2C%7B%22op%22%3A%22in%22%2C%22content%22%3A%7B%22field%22%3A%22files.data_type%22%2C%22value%22%3A%5B%22Gene%20Expression%20Quantification%22%5D%7D%7D%2C%7B%22op%22%3A%22in%22%2C%22content%22%3A%7B%22field%22%3A%22files.experimental_strategy%22%2C%22value%22%3A%5B%22RNA-Seq%22%5D%7D%7D%5D%7D)
.

Follow the [Manifest Downloading Scheme](#manifest-downloading-scheme) to download the data.

### DNA Methylation

The data are provided in tables of array results of the level of methylation at known CpG sites. They include unique ids
for the array probes and methylation Beta values, representing the ratio between the methylated array intensity and
total array intensity (falls between 0, lower levels of methylation, and 1, higher levels of methylation).

The DNA Methylation data used in the original Multisurv paper was since updated and the manifest for it can now be
downloaded [here](https://portal.gdc.cancer.gov/repository?filters=%7B%22op%22%3A%22and%22%2C%22content%22%3A%5B%7B%22op%22%3A%22in%22%2C%22content%22%3A%7B%22field%22%3A%22files.data_category%22%2C%22value%22%3A%5B%22dna%20methylation%22%5D%7D%7D%2C%7B%22op%22%3A%22in%22%2C%22content%22%3A%7B%22field%22%3A%22files.data_format%22%2C%22value%22%3A%5B%22txt%22%5D%7D%7D%5D%7D)

Follow the [Manifest Downloading Scheme](#manifest-downloading-scheme) to download the data.