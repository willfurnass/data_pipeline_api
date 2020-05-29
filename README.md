# data_pipeline_api
API to access the data pipeline

# Current issues and questions
* Units thing seems ambitious - feels like we have to know quite a lot about what is stored where, and that it is easier to just do client side where you actually know.
* It isn't obvious that the version should be stored in the metadata file, rather than having a file per version.
* We should pick one of path (relative to root?) or location (relative to parameter), unless there is a compelling reason not to.
* Why is the warning stuff inside the metadata file - shouldn't that all be tracked by the DB?
* I'm not quite clear on how things tie together - feels like we should be just getting hashes for our inputs, and then recording them, and letting the DB figure it out?
* Are "components" only relevant for datasets? If not, how are components encoded in parameter files?
* I'm not clear how the attrs come into it with the dataset stuff.