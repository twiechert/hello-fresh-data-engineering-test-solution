## Prerequisites ##

For better reproducibility, the spark application and tests are run in a docker container.
It is assumed that the evaluator has docker and docker-compose installled on their system.

Alternatively, the job can directly be run via pyspark locally (see run without docker) or packaged and submit onto a spark environment.


## Running the test routines (in a docker container)
This will build the required docker container and run the test case inside the container.

``` 
make docker-test 
``` 

## Running the main application routine
This will build the required docker container and run the main application inside the container.

``` 
make docker-run
``` 

## Sources & References
- UDF to turn string codes duration types into minutes:
https://stackoverflow.com/questions/69735290/apache-spark-parse-pt2h5m-duration-iso-8601-duration-in-minutes

- sample repository to package and run spark jobs and tests inside docker
https://www.confessionsofadataguy.com/introduction-to-unit-testing-with-pyspark/

## Run without docker

If you do not run/test the application inside a docker container, you can use pyspark directly.
Assuming, you use anaconda, create a new environment

```
conda install -c conda-forge pyspark
conda create -n pyspark_env
conda activate pyspark_env
pip3 install -r requirements.txt
```

Then, run the application

``` pyspark < main.py ```

Similiarly, to run the tests locally without docker


``` python3 -m pytest ```

Alternatively, you can package the app directly to be used elsewhere.
``` 
conda install conda-pack
conda-pack -f --ignore-missing-files --exclude lib/python3.1 -n pyspark_env
```
