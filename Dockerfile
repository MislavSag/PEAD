FROM rocker/r-base:4.1.1

# Install any necessary packages
RUN apt-get update && apt-get install -y \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev

# Install necessary R packages
RUN Rscript -e "install.packages('data.table', repos='http://cran.rstudio.com/')"
RUN Rscript -e "install.packages('gausscov', repos='http://cran.rstudio.com/')"
RUN Rscript -e "install.packages('paradox', repos='http://cran.rstudio.com/')"
RUN Rscript -e "install.packages('mlr3', repos='http://cran.rstudio.com/')"
RUN Rscript -e "install.packages('mlr3pipelines', repos='http://cran.rstudio.com/')"
RUN Rscript -e "install.packages('mlr3tuning', repos='http://cran.rstudio.com/')"
RUN Rscript -e "install.packages('mlr3mbo', repos='http://cran.rstudio.com/')"
RUN Rscript -e "install.packages('mlr3misc', repos='http://cran.rstudio.com/')"
RUN Rscript -e "install.packages('mlr3filters', repos='http://cran.rstudio.com/')"
RUN Rscript -e "install.packages('mlr3learners', repos='http://cran.rstudio.com/')"
RUN Rscript -e "install.packages('AzureStor', repos='http://cran.rstudio.com/')"
RUN Rscript -e "install.packages('ranger', repos='http://cran.rstudio.com/')"
RUN Rscript -e "install.packages('xgboost', repos='http://cran.rstudio.com/')"
RUN Rscript -e "install.packages('kknn', repos='http://cran.rstudio.com/')"

# Copy the R script into the container
COPY my-r-script.R /

# Run the R script when the container starts
CMD ["Rscript", "my-r-script.R"]
