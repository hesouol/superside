Superside Organizations ETL
============

Simple ETL app, that extract records from Crunchbase's `organizations` api and write them to partitioned files.
For API docs: [Crunchbase API docs](https://support.crunchbase.com/hc/en-us/articles/8327794377235-Crunchbase-Basic-API-FAQ)
 
Build & Run
------------
Build: The application is dockerized, so you will need [Docker](https://www.docker.com/get-started/) to wrap it up.
~~~bash
    $ docker build . --tag=superside
~~~

Run: For running `CB_API_KEY` must be set. 
Refer to the [Crunchbase API docs](https://support.crunchbase.com/hc/en-us/articles/115010466447-How-do-I-get-Basic-API-access) to get your basic key. 
~~~bash
    $ docker run --name superside -d -e CB_API_KEY=<user_api_key> --volume $(pwd)/context=org:/home/marvel/context=org superside
~~~

It's also possible to provide a date parameter, that will be used to filter just the records modified after a certain date. In that case add the `--processing-date` parameter.
~~~bash
    $ docker run --name superside -d -e CB_API_KEY=<user_api_key> --volume $(pwd)/context=org:/home/marvel/context=org superside --processing-date 2024-01-01
~~~

If you want to run the tests, run those in the root directory. Four tests must be collected.
~~~bash
    $ pip install -r requirements.txt
    $ pytest
~~~

About the Project
------------
```
.
├── Dockerfile
├── README.md
├── pytest.ini
├── requirements.txt
├── src
│ ├── __init__.py
│ ├── base_etl.py
│ └── superside_etl.py
└── tests
    ├── __init__.py
    ├── conftest.py
    ├── test_base_etl.py
    └── test_superside_etl.py
```
What is important here is that `base_etl.py` contains the `BaseETL` class, which is an abstract class with a `load()`
method that can be shared in case other apis need to be extracted as well. This class can be replaced with different 
destinations like Spark with S3, Snowflake, Redshift and so on. The default setup writes partitioned Parquet files.

How to Automate?
------------
If the idea is to have a containerized environment, the app is ready.
It could be easy to automate it with:
- Airflow [KubernetesPodOperator](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html#kubernetespodoperator)
- Kubernetes [CronJob](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/)

The records can be dumped daily filtering by the `processing-date` provided.

Known issues
------------
1. In the extraction the exception treatment should be improved, the api was raising a few different errors, but I didn't have enough time to jump into that.
2. This approach writes too many small files, the API was failing too often that's why I decided to keep collecting very small chunks, this can be fixed in the next layer of the lake, compacting the files overnight into larger ones. I decided to write them right after extraction to not overload memory in case of backfilling.
3. The api does not provide a `gt` (greater than) query filter, just `gte` (greater then or equal). The `gt` operator is present in the docs, but it doesn't work. This may cause duplicates between batches, but it can also be fixed in the next layers using `uuid` and `updated_at` fields.
4. I used ChatGPT to generate the `extract_location_value`, I found the approach quite clever so I kept it with minor adjustments.
5. I totally underestimated the effort necessary for this challenge. The crunchbase docs are not very helpful and it was a bit stressful to go over it and have everything working.
6. The project is a bit over engineered, I would like to show the possibilities when extracting data. Depending on the scenario the script could be simplified.
7. I used MacOS (M1) to build this project, I had issues in the past running MacOS projects on Linux, let me know if you have any issues and I can try to adjust.

Final Thoughts
------------
Definitely there is room for improvement in the project, I would be glad to jump into a call, so we can discuss that together.
Thank you for the opportunity! 