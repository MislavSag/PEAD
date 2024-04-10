library(AzureStor)

# downlaod data from Azure blob
blob_key = readLines('./blob_key.txt')
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
cont = storage_container(BLOBENDPOINT, "jphd")
# storage_download(cont, "pead-predictors-20240129.csv", overwrite=TRUE)
storage_download(cont, "pre-predictors-20240408.csv", overwrite=TRUE)
