using Azure.Messaging.EventHubs;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs.Producer;

namespace SendSampleData
{
    class Program
    {
        const string eventHubName = "test-hub";
        
        public static async Task Main(string[] args)
        {
            string? connectionString = Environment.GetEnvironmentVariable("EVENT_HUB_CONNECTION_STRING");

            if(connectionString == null)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Please set the EVENT_HUB_CONNECTION_STRING environment variable.");
                Console.ResetColor();
            } else {
                await EventHubIngestionAsync(connectionString);
            }

            
        }

        public static async Task EventHubIngestionAsync(string connectionString)
        {
            await using (var producerClient = new EventHubProducerClient(connectionString, eventHubName))
            {
                for(int counter=0; counter < 100; counter++)
                {
                    try
                    {
                        EventData eventData = CreateEventData("TenantA", counter);
                        EventData eventData2 = CreateEventData("TenantB", counter);

                        using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();
                        eventBatch.TryAdd(eventData);
                        eventBatch.TryAdd(eventData2);

                        await producerClient.SendAsync(eventBatch);
                    }
                    catch (Exception exception)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("{0} > Exception: {1}", DateTime.Now, exception.Message);
                        Console.ResetColor();
                    }
                }
            }
        }

        private static EventData CreateEventData(string Tenant, int metric)
        {
            string recordString = $"{{\"TimeStamp\": \"{DateTime.Now}\", \"Name\": \"{Tenant}\", \"Metric\": {metric}, \"Source\": \"EventHubMessage\"}}";
            EventData eventData = new EventData(Encoding.UTF8.GetBytes(recordString));
            Console.WriteLine($"sending message: {recordString}");
            eventData.Properties.Add("Table", Tenant);
            return eventData;
        }
    }
}
