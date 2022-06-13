if (!require("BiocManager", quietly = TRUE))
    install.packages("BiocManager")

BiocManager::install("TCGAbiolinks")

project_ids <- stringr::str_subset(TCGAbiolinks::getGDCprojects()$project_id, 'TCGA')

data <- list()

for (project_id in project_ids) {
    data[[project_id]] <- TCGAbiolinks::GDCquery_clinic(project=project_id, type='clinical')
}

# Merge into single table
# (the "disease" column identifies each original table)
data <- do.call(dplyr::bind_rows, data)

# Write to file
output_path <- '/mnt/dataA/TCGA/raw/clinical_data.tsv'
readr::write_tsv(data, output_path)