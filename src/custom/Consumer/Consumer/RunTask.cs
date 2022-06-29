using System;
using Confluent.Kafka;
using System.Threading.Tasks;
using System.Threading;

namespace Consumer
{
    class RunTask
    {
        static readonly object _object = new object();
        
        private static CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private static CancellationToken token = cancellationTokenSource.Token;
        private static ConsumeResult<string, string> partitionRecords = new ConsumeResult<string, string>();
        public static IConsumer<string, string> consumer = new ConsumerBuilder<string, string>(Consumer.consumerConfig).Build();
        //TupleList<long, string> list = new TupleList<long, string>();


        /*
               Print the all records,offsets,consumers and their groups 
               Each offset each thread
               */
        private void Print(string topic, long log_first_offset, long log_end_offset, TopicPartition partitionRecord, IConsumer<string, string> consumer)
        {
            var watch = System.Diagnostics.Stopwatch.StartNew();
            for (long i = log_first_offset; i < log_end_offset;)
            {
                try
                {
                    //Process ---> Each Task for each offset
                    var task = Task.Run(() =>
                    {
                        consumer.Assign(new TopicPartitionOffset(topic, new Partition(partitionRecord.Partition), i));
                        partitionRecords = consumer.Consume(token);
                        //Console.WriteLine($"Received message at  {consumer.MemberId}:{consumer.Name} => {partitionRecords.Topic} {partitionRecords.Partition.Value}:{partitionRecords.Offset}");
                        Console.WriteLine($"Received message at {consumer.MemberId}:{consumer.Name} => {partitionRecords.Topic} {partitionRecords.Message.Value}:{partitionRecords.Offset}");
                        i++;
                    });
                    Task.WaitAll(task);


                    /*
                    Thread ---> Each thread for each offset
                    Thread thread = new Thread(() =>
                    {
                        consumer.Assign(new TopicPartitionOffset("first", new Partition(partitionRecord.Partition), i));
                        partitionRecords = consumer.Consume(token);
                        Console.WriteLine($"Received message at {partitionRecords.Partition.Value} {partitionRecords.Message.Value}:{partitionRecords.Offset}");
                        i++;
                    });
                    thread.Start();
                    thread.Join();
                    */
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Error in Offset loop: {e.Message}");
                }
            }
            watch.Stop();
            Console.WriteLine($"Loop 1 Execution Time: {watch.ElapsedMilliseconds} ms");
        }



        /*
               Thread execution for all partition
               Each partition each thread
               Each partition each thread
               Ideally Number of Consumer within a same group = number of partition in a topic
               */
        public void RunAllTask(string topic,TopicPartition partitionRecord, IConsumer<string, string> consumer, WatermarkOffsets watermarkOffsetsRecord)
        {
            Boolean IsLockTaken = false;
            Monitor.Enter(_object, ref IsLockTaken);
            Console.WriteLine(Thread.CurrentThread.Name + " Trying to enter into the critical section");
            long log_first_offset = 0;
            log_first_offset = watermarkOffsetsRecord.Low.Value;
            long log_end_offset = watermarkOffsetsRecord.High.Value - 1;
            Console.WriteLine(Thread.CurrentThread.Name + " Entered into the critical section");
            try
            {
                //Task ---> Each Task for each offset
                var task = Task.Run(() => Print(topic, log_first_offset, log_end_offset, partitionRecord, consumer));
                Task.WaitAll(task);

                //ThreadPool ---> Using Threadpool
                //ThreadPool.QueueUserWorkItem(Run);
                //Thread.CurrentThread.Join();

                //Thread ---> Using Threadpool
                //Thread thread = new Thread(() => Print(log_first_offset, log_end_offset, partitionRecord, consumer));
                //thread.Start();
                //thread.Join();

            }
            catch (Exception e)
            {
                Console.WriteLine($"Error in Partition : {e.Message}");
            }
            finally
            {
                Monitor.Exit(_object);
                Console.WriteLine(Thread.CurrentThread.Name + "Exited the critical section");
            }
        }
    }
}
