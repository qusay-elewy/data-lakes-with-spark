
# Project: Data Lakes

This project tests our knowledge with dealing with Spark in processing ETL tasks.
Amazon Web Services have been used to accomplish this goal. Source data is stored in JSON files on S3. Spark is used to read these files, extract the fields we are interested in, then load them into a star schema, in which the entities are saved as parquet files on S3.

## Dimenssional Data Model

For our data model, extracted data is loaded into a star schema, which is composed of one fact table and four dimenssion tables listed below:

>_Fact Table:_
   * songplays: Stores users activities and what they listen to on Sparkify.
>_Dimenssion Tables:_
   * users: Stores users information
   * songs: Stores songs information
   * artists: Stores artists information
   * time: Stores time information

This model allows Sparkify data team to get insigths on played songs as well as their users.
Aggregates and statistics such as most played songs, peek hours, the number of registered users and how many of them are paying for the service can be easily built using this model.


## How Does it Work?

To run this etl.py, the user needs to run command _Python etl.py_ in terminal. The ETL uses Spark to extract the data from the JSON files saved on our S3 bucket into a data frame, do the needed transofrmations, then load them into entities that are saved as Parquet files.

A user was created to complete this job; this user has Admin Access + Full Access to S3. Amazon Key Id and Amazon Secret Access Key were set before running the ETL, but then were removed after completing the project and before submission to keep them safe. 


## ETL Evaluation

ETL took about 2 hours to complete.


## Questions

The process took so long to complete, which doesn't make since to me since this is supposed to be a high-perfomance solution; the whole purpose of using applications like Spark is to parallelize data processing in order to save the overall processing time. Some claimed on the Knowledge portal that loading data from S3 takes time, 2 hours to load 10000 records is a bit too long! Any idea what's happening here?

Also, the entire process seem like a black box to me. Is there a way to access this data visually? Like accessing the stages, created schema, logs, ... while running this application? And where is this Spark cluster (I see no connection string in the code referring to it), maybe local?

---

