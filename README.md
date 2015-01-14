Как запускалось:
hadoop jar twitter-1.jar twittercounter.Runner /data/twitter/twitter_rv.net twitter

Как доставались результаты:
hdfs dfs -cat twitter/top/part-r-00000 | head -50 > twitter_results/top50
hdfs dfs -get twitter/average/part-r-00000 twitter_results/average
hdfs dfs -get twitter/range/part-r-00000 twitter_results/range

Лог выполнения лежит в файле log.
