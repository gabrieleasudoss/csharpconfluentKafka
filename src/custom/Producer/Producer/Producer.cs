using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Producer
{
    public class Producer
    {
        public static async Task Main(string[] args)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                Acks = Acks.All,
            };
            string topicName = "first";
            await CreateTopicAsync(producerConfig.BootstrapServers, topicName);
            using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
            {
                while (true)
                {
                    var user = new User
                    {
                        Id = Guid.NewGuid().ToString(),
                        FirstName = Faker.Name.FirstName(),
                        LastName = Faker.Name.LastName()
                    };
                    try
                    {
                        var deliveryReport = await producer.ProduceAsync(topicName, new Message<string, string> { Value = JsonFormart(user) });
                        //var deliveryReport = await producer.ProduceAsync(topicName, new Message<string, string> { Value = XmlFormart(user) });
                        Console.WriteLine($"delivered to: {deliveryReport.TopicPartitionOffset} :=> {deliveryReport.Message.Value}");
                    }
                    catch (ProduceException<string, string> e)
                    {
                        Console.WriteLine($"Message Falied: {e.Message} [{e.Error.Code}]");
                    }
                }
                 
            }
        }
        private static string JsonFormart(Object user)
        {
            return JsonSerializer.Serialize(user);
        }
        private static string XmlFormart(Object user)
        {
            var writer = new StringWriter();
            var serializer = new XmlSerializer(typeof(User));
            serializer.Serialize(writer, user);
            string xml = writer.ToString();
            return xml;
        }
        private static async Task CreateTopicAsync(string bootstrapServers, string topicName)
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    await adminClient.CreateTopicsAsync(new TopicSpecification[] {
                        new TopicSpecification { Name = topicName, ReplicationFactor = 1, NumPartitions = 3 } });
                }
                catch (CreateTopicsException e)
                {
                    Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                }
            }
        }
    }
}
