options(warn = -1)
library(data.table)
library(gausscov)
library(paradox)
library(mlr3)
library(mlr3pipelines)
library(mlr3viz)
library(mlr3tuning)
library(mlr3misc)
library(future)
library(future.apply)
library(mlr3extralearners)
library(batchtools)
library(mlr3batchmark)
library(checkmate)
library(stringi)
library(R6)
library(brew)



# UTILS -------------------------------------------------------------------
# utils functions
dir = function(reg, what) {
  fs::path(fs::path_expand(reg$file.dir), what)
}
getResultFiles = function(reg, ids) {
  fs::path(dir(reg, "results"), sprintf("%i.rds", if (is.atomic(ids)) ids else ids$job.id))
}
waitForFile = function(fn, timeout = 0, must.work = TRUE) {
  if (timeout == 0 || fs::file_exists(fn))
    return(TRUE)
  "!DEBUG [waitForFile]: `fn` not found via 'file.exists()'"
  timeout = timeout + Sys.time()
  path = fs::path_dir(fn)
  repeat {
    Sys.sleep(0.5)
    if (basename(fn) %chin% list.files(path, all.files = TRUE))
      return(TRUE)
    if (Sys.time() > timeout) {
      if (must.work)
        stopf("Timeout while waiting for file '%s'",
              fn)
      return(FALSE)
    }
  }
}
writeRDS = function (object, file, compress = "gzip") {
  batchtools:::file_remove(file)
  saveRDS(object, file = file, version = 2L, compress = compress)
  waitForFile(file, 300)
  invisible(TRUE)
}
UpdateBuffer = R6Class(
  "UpdateBuffer",
  cloneable = FALSE,
  public = list(
    updates = NULL,
    next.update = NA_real_,
    initialize = function(ids) {
      self$updates = data.table(
        job.id = ids,
        started = NA_real_,
        done = NA_real_,
        error = NA_character_,
        mem.used = NA_real_,
        written = FALSE,
        key = "job.id"
      )
      self$next.update = Sys.time() + runif(1L, 60, 300)
    },

    add = function(i, x) {
      set(self$updates, i, names(x), x)
    },

    save = function(jc) {
      i = self$updates[!is.na(started) & (!written), which = TRUE]
      if (length(i) > 0L) {
        first.id = self$updates$job.id[i[1L]]
        writeRDS(
          self$updates[i,!"written"],
          file = fs::path(
            jc$file.dir,
            "updates",
            sprintf("%s-%i.rds", jc$job.hash, first.id)
          ),
          compress = jc$compress
        )
        set(self$updates, i, "written", TRUE)
      }
    },

    flush = function(jc) {
      now = Sys.time()
      if (now > self$next.update) {
        self$save(jc)
        self$next.update = now + runif(1L, 60, 300)
      }
    }

  )
)


# RUN JOB -----------------------------------------------------------------
# load registry
reg = loadRegistry("experiments")

# extract not  done ids
ids_not_done = findNotDone(reg=reg)

# create job collection
resources = list(ncpus = 4) # this shouldnt be important
jc = makeJobCollection(ids_not_done,
                       resources = resources,
                       reg = reg)

# extract integer
i = as.integer(Sys.getenv('PBS_ARRAY_INDEX'))
# i = 2L

# start buffer
buf = UpdateBuffer$new(jc$jobs$job.id)
update = list(started = batchtools:::ustamp(), done = NA_integer_, error = NA_character_, mem.used = NA_real_)

# get job
cat("Get Job \n")
# job = batchtools:::Job$new(
#   file.dir = jc$file.dir,
#   reader = batchtools:::RDSReader$new(FALSE),
#   id = jc$jobs[i]$job.id,
#   job.pars = jc$jobs[i]$job.pars[[1L]],
#   seed = 1 + jc$jobs[i]$job.id,
#   resources = jc$resources
# )
job = batchtools:::getJob(jc, i)
id =


cat("CHANGE JOB ID MANNUALY", nrow(jc$jobs), "!!!")

# execute job
cat("Execute Job")
gc(reset = TRUE)
update$started = batchtools:::ustamp()
result = execJob(job)

# save job
writeRDS(result, file = getResultFiles(jc, id), compress = jc$compress)

# memory usage
tryCatch({
  memory.mult = c(if (.Machine$sizeof.pointer == 4L) 28L else 56L, 8L)
  gc_info <- gc(verbose = FALSE)
  memory_used = sum(gc_info[, 1L] * memory.mult) / 1000000L
}, error = function(e) {
  memory_used <- 1000  # Set to NA or some default value in case of error
})

# updates
update$done = batchtools:::ustamp()
update$mem.used = memory_used
buf$add(i, update)
buf$flush(jc)
buf$save(jc)
