library(AzureStor)

# downlaod data from Azure blob
blob_key = readLines('./blob_key.txt')
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
cont = storage_container(BLOBENDPOINT, "jphd")
storage_download(cont, "pead_experiment.zip", overwrite=TRUE)
unzip(zipfile = "pead_experiment.zip", exdir = "experiment")
dir.create("experiment2")
dir_exp = list.files('./experiment/Users/Mislav/Documents/GitHub/PEAD/experiments',
                     full.names = TRUE)
file.copy(dir_exp, './experiment2', recursive = TRUE)
unlink('./experiment', recursive = TRUE)
file.rename("experiment2", "experiment")

