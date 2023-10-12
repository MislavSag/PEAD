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
library(fs)
library(R6)


# utils
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

# load registry
reg = loadRegistry("experiments")

# create job collection
resources = list(ncpus = 4) # this shouldnt be important
jc = makeJobCollection(resources = resources, reg = reg)

# extract integer
i = as.integer(Sys.getenv('PBS_ARRAY_INDEX'))
# i = 10L

# get job
job = batchtools:::getJob(jc, i)
id = job$id

# execute job
result = execJob(job)

# save ojb
writeRDS(result, file = getResultFiles(jc, id), compress = jc$compress)

# updates
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
buf = UpdateBuffer$new(jc$jobs$job.id)
update = list(started = batchtools:::ustamp(), done = NA_integer_, error = NA_character_, mem.used = NA_real_)
update$done = batchtools:::ustamp()
buf$add(i, update)
buf$flush(jc)
buf$save(jc)
