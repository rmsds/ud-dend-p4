This repository contains my submission for the Data Lake Project (section 4) of the Udacity Data Engineering Nano Degree.

# Summary
In this project a set of logs and song information are loaded into staging tables and are then transformed and loaded into five final tables to allow for easier analytics.

This is implemented using Spark.

# Running this code
## On a local machine against a local (or remote) dataset
* Put the dataset under the local directory (for example: `data/` and `output-data/`)
* Edit `etl.py` to point to it (change `input_data` and `output_data` inside `def main()`).
  * to use/create a remote dataset change the corresponding variable to point to the bucket (see the next session for an example)
* Prepare a `dl.cfg` file with your AWS credentials (see `dl.cfg.orig` for a template)
* Start a `jupyter/pyspark-notebook`  
`docker run --name pyspark-dend --rm -p 8888:8888 -v "$PWD":/home/jovyan/work jupyter/pyspark-notebook`
* Install any missing required Python packages  
`docker exec pyspark-dend pip install -r requirements.txt`
* Execute the script  
`docker exec pyspark-dend python etl.py`


## Run on Amazon EMR/Notebook against a dataset on Amazon S3
### Create S3 bucket for output data
* Create a bucket and note the name (for example: _ud-dend-proj4-rs-output_)
  * Configure the bucket for **no public** access. The Amazon EMR Notebook will have access to any resources your account has access.
* Edit `etl.py` to point to your input and output data sets. The variables must start with `s3a://`. For example:
  * `input_data = 's3a://udadicty-dend/'`
    * note that you need to be in the **us-west-2** region to be able to use this bucket
    * **Warning**: running this script for all the input `song_data` in this dataset takes an long time, mostly due to how slow is is to write the _parquet_ tables to an Amazon S3 bucket.
  * `output_data = 's3a://ud-dend-proj4-rs-output/'`


## Create a Notebook with associated EMR cluster
* On the Amazon AWS console -> EMR -> Notebook, create a new Notebook with a new dedicated cluster, for example:
  * notebook name: dend-proj4
  * cluster name: dend-proj4-dend-cluster
  * cluster size: 3x m5.xlarge
  * ec2 pair (for debugging): spark-cluster
* Once connected to the notebook you can use the following to install any missing Python packages (in this example _boto3_): `sc.install_pypi_package("boto3")`

## Execute the script
* Copy and paste your code into the notebook, or upload the Python file

# Input Data 
These were the detected schemas for the input data.

# song_data DataFrame schema
```
root                                                                            
 |-- artist_id: string (nullable = true)
 |-- artist_latitude: double (nullable = true)
 |-- artist_location: string (nullable = true)
 |-- artist_longitude: double (nullable = true)
 |-- artist_name: string (nullable = true)
 |-- duration: double (nullable = true)
 |-- num_songs: long (nullable = true)
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- year: long (nullable = true)
```

# log_data DataFrame schema
```
root
 |-- artist: string (nullable = true)
 |-- auth: string (nullable = true)
 |-- firstName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- itemInSession: long (nullable = true)
 |-- lastName: string (nullable = true)
 |-- length: double (nullable = true)
 |-- level: string (nullable = true)
 |-- location: string (nullable = true)
 |-- method: string (nullable = true)
 |-- page: string (nullable = true)
 |-- registration: double (nullable = true)
 |-- sessionId: long (nullable = true)
 |-- song: string (nullable = true)
 |-- status: long (nullable = true)
 |-- ts: long (nullable = true)
 |-- userAgent: string (nullable = true)
 |-- userId: string (nullable = true)
```

