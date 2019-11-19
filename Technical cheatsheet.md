# Technical cheatsheet

## Reading the datasets

1. Connect to the cluster `ssh {username}@iccluster040.iccluster.epfl.ch`
2. Access the datasets in the HDFS (hadoop cluster)
   - list of datasets:`hadoop fs -ls /datasets`
   - wikipedia datasets: 
     - `hadoop fs -ls /datasets/wikidatawiki`
       - /datasets/wikidatawiki/wikidatawiki-20170301-pages-articles-multistream-index.txt
       - /datasets/wikidatawiki/wikidatawiki-20170301-pages-articles-multistream.xml
     - `hadoop fs -ls /datasets/enwiki-20191001`
       - /datasets/enwiki-20191001/enwiki-20191001-pages-articles-multistream.xml
3. To read (take a sneak peak) from a dataset to the cluster: `hadoop fs -cat /datasets/<file_name> | less`
4. To  read the first 100 lines `hadoop fs -cat /datasets/<file_name> | head -n 100`
5. To copy the output to your own space in the cluster: `hadoop fs -cat /datasets/enwiki-20191001/enwiki-20191001-pages-articles-multistream.xml | head -n 200 > ~/enwiki_200_lines.txt`

- To transfer files from local to cluster:
`hadoop fs -put {filename.txt}  `




## Copy files from/to the cluster

1. Open a terminal and execute the commands LOCALLY

2. copy files FROM local **TO cluster** (scp {source} {destination})
   `scp dataset.py {username}@iccluster040.iccluster.epfl.ch:~`

3. copy files **FROM cluster** TO local

   ` scp {username}@iccluster040.iccluster.epfl.ch:~/dataset.py ./`



## Run spark-python files in the cluster 

1. copy your python file to the cluster (as mentioned above)

   `scp dataset.py {username}@iccluster040.iccluster.epfl.ch:~`

2. connect to the cluster

   `ssh {username}@iccluster040.iccluster.epfl.ch`

3. run the command:

   ```
   spark-submit \
    --master yarn \
    --packages com.databricks:spark-xml_2.11:0.6.0 \
    --num-executors 50 \
    --executor-memory 4g \
    dataset.py 
   ```
   or to specify the workers
   ```
   spark-submit --master yarn --packages com.databricks:spark-xml_2.11:0.7.0 --num-executors 70 --executor-memory 6g filename.py
   ```

4. you can see the process of the job you submitted here: http://iccluster040.iccluster.epfl.ch:8088/cluster

   (click on the **ID** of your job)

## Run spark-python files locally

1. go to where the ada environment is installed (usually in Anaconda3/envs/ada) and run:

   ```
   ./bin/spark-submit \
     --master local \
     --packages com.databricks:spark-xml_2.11:0.6.0 \
     {absolute path to python file}   
   ```

-------------
## Visualization Libraries

- D3
- Vega (specifies graphics in JSON format)
- Vincent (Python-to-Vega translator)
- Bokeh
