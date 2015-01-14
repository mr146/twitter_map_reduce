Как запускалось:

hadoop jar twitter-1.jar twittercounter.Runner /data/twitter/twitter_rv.net twitter

Как доставались результаты:

hdfs dfs -cat twitter/top/part-r-00000 | head -50 > twitter_results/top50

hdfs dfs -get twitter/average/part-r-00000 twitter_results/average

hdfs dfs -get twitter/range/part-r-00000 twitter_results/range


Лог выполнения лежит в файле log.


Суть: все запускалось в Runner'е, все 4 задания для map-reduce.

1 - подсчет количества подписчиков: {userId, followerId} -M-> {userId, [1]} -R-> {userId, count}. TwitterAggregator*

2 - подсчет топа: {userId, count} -M-> {-count, [userIds]} -R-> {userId, count}. Далее просто head -50. TwitterGetTop*

3 - подсчет среднего: {userId, count} -M-> {1, [counts]} -R-> {usersCount, average}. TwitterAverage*

4 - построение распределения: {userId, count} -M-> {log(count), [1]} -R-> {log(count), usersCount}. TwitterGetRanges*

По факту для все reducer'ы кроме первого на выход подают "прилизанную" строчку (почему бы и нет, подумал я).

Аггрегация заняла 10 минут, подсчет топа - 5 минут, подсчет среднего - 2 минуты, построение распределения - 2 минуты, чуть более подробно можно увидеть в логах.

Результаты кинул в twitter_results
