using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Consumer
{
    public class Consumer
    {
        public static ConsumerConfig consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "dotnet-first",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        
        public static ConsumerConfig consumerConfig1 = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "dotnet-second",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        public static List<string> topics = new List<string>();
        //Single Consumer with 3 parition,Single topic,single consumer group
        public static IConsumer<string, string> consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetLogHandler((_, message) =>
            Console.WriteLine($"Facility: {message.Facility}-{message.Level} Message: {message.Message}"))
            .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}. Is Fatal: {e.IsFatal}"))
            .Build();
        public static IConsumer<string, string> multipleConsumer;

        public static void Main(string[] args)
        {
            try
            {
                //Add Topics
                topics.Add("first");
                //topics.Add("second");

                /*
                Single Consumer with 3 parition,Single topic,single consumer group
                Each partition each thread
                Each offset each thread
                */

                fetchedRecords(consumer);

                /*
                Multiple Consumer with 3 parition,Multiple topics,single consumer group
                Each partition each thread
                Each offset each thread
                */

                //MultipleConsumer();

                /*
                Multiple Consumer with 3 parition,Single topic,Multiple consumer group
                Each partition each thread
                Each partition each thread
                Ideally Number of Consumer within a same group = number of partition in a topic
                */

                //MultipleConsumerGroup();

            }
            catch (Exception e)
            {
                Console.WriteLine($"Consume topic subscribe Error: {e.Message}");
            }
        }

        /*
               Multiple Consumer with 3 parition,Multiple topics,single consumer group
               Each partition each thread
               Each offset each thread
               */
        private static void MultipleConsumer()
        {
            Console.WriteLine(Thread.CurrentThread.Name + "Topic:MultipleConsumer");
            Thread thread;
            try
            {
                var consumerBuilder = new ConsumerBuilder<string, string>(consumerConfig);
                var consumer1 = consumerBuilder.Build();
                var consumer2 = consumerBuilder.Build();
                thread = new Thread(() => fetchedRecords(consumer1));
                thread.Name = String.Concat("Thread - ", consumer1.Name);
                thread.Start();

                thread = new Thread(() => fetchedRecords(consumer2));
                thread.Name = String.Concat("Thread - ", consumer2.Name);
                thread.Start();
            }

            catch (Exception e)
            {
                Console.WriteLine($"MultipleConsumer topic send Error: {e.Message}");
            }
        }

        /*
               Multiple Consumer with 3 parition,Single topic,Multiple consumer group (dotnet-first, dotnet-second)
               Each partition each thread
               Each partition each thread
               Ideally Number of Consumer within a same group = number of partition in a topic
               */
        private static void MultipleConsumerGroup()
        {
            Console.WriteLine(Thread.CurrentThread.Name + "Topic:MultipleConsumerGroup");
            Thread thread;
            try
            {
                var consumerBuilder1 = new ConsumerBuilder<string, string>(consumerConfig);
                var consumerBuilder2 = new ConsumerBuilder<string, string>(consumerConfig1);
                var consumer1 = consumerBuilder1.Build();
                var consumer2 = consumerBuilder2.Build();
                thread = new Thread(() => fetchedRecords(consumer1));
                thread.Name = String.Concat($"Thread - {consumerConfig.GroupId} => {consumer1.Name}");
                thread.Start();

                thread = new Thread(() => fetchedRecords(consumer2));
                thread.Name = String.Concat($"Thread - {consumerConfig1.GroupId} => {consumer2.Name}");
                thread.Start();
            }

            catch (Exception e)
            {
                Console.WriteLine($"MultipleConsumerGroup topic send Error: {e.Message}");
            }
        }

        /*
               Fetch records from consumer and their topic
               Each topic each thread for each consumer
               */
        private static void fetchedRecords(IConsumer<string, string> consumer)
        {
            Console.WriteLine(Thread.CurrentThread.Name + "consumer:fetchedRecords");
            Thread thread;
            try
            {
                topics.ForEach(topic =>
                {
                    consumer.Subscribe(topic);
                    thread = new Thread(() => HandleFetchedRecords(topic, consumer));
                    thread.Name = String.Concat($"Thread - {topic}:{consumer.Name}");
                    thread.Start();
                });
            }

            catch (Exception e)
            {
                Console.WriteLine($"Consume topic send Error: {e.Message}");
            }
        }

        /*
               Fetch records from consumer and their topic
               Each partition each thread
               Each partition each thread
               Ideally Number of Consumer within a same group = number of partition in a topic
               */

        private static void HandleFetchedRecords(string topic, IConsumer<string, string> consumer)
        {
            Console.WriteLine(Thread.CurrentThread.Name + "topic,consumer:HandleFetchedRecords");
            List<TopicPartition> partitionsToPause = new List<TopicPartition>();
            ConsumeResult<string, string> partitionRecords = new ConsumeResult<string, string>();
            List<TopicPartition> partitionsList = GetTopicPartitions(consumerConfig.BootstrapServers, topic);
            RunTask runTask = new RunTask();
            Thread thread;
            try
            {
                partitionsList.ForEach(partition =>
                {
                    WatermarkOffsets watermarkOffsets = consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(5));
                    thread = new Thread(() => runTask.RunAllTask(topic,partition, consumer, watermarkOffsets));
                    thread.Name = String.Concat("Thread - ", partition);
                    Console.WriteLine(thread.Name + " Started");
                    thread.Start();
                    partitionsToPause.Add(partition);
                });
            }
            catch (Exception e)
            {
                Console.WriteLine($"Consume partition send Error: {e.StackTrace}");
            }
        }

        /*
               Get partition from each topic
               */
        public static List<TopicPartition> GetTopicPartitions(string bootstrapServers, string topicValue)
        {
            var topicPartition = new List<TopicPartition>();
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                var meta = adminClient.GetMetadata(TimeSpan.FromSeconds(20));
                meta.Topics.ForEach(topic =>
                {
                    if (topic.Topic == topicValue)
                    {
                        foreach (PartitionMetadata partition in topic.Partitions)
                        {
                            topicPartition.Add(new TopicPartition(topic.Topic, partition.PartitionId));
                        }
                    }
                });
            }
            return topicPartition;
        }
    }
}
