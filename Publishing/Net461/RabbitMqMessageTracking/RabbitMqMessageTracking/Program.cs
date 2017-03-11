using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMqMessageTracking
{
    public class Order
    {
        public int OrderId { get; set; }
        public int ClientId { get; set; }
        public string ProductCode { get; set; }
        public string OfferCode { get; set; }
        public int Quantity { get; set; }
        public decimal UnitPrice { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            SetupNewOrders();

            while (true)
            {
                int messagesBefore = GetMessageCount();
                Console.WriteLine("");
                Console.WriteLine("The topic exchange: \"order\" with queue: \"order.new\" with binding key \"new\" has been created on your local RabbbitMq");
                Console.WriteLine("Enter a number of messages to publish. If any failures occur, 2 retries will be attempted with 1s between attempts");
                int number = int.Parse(Console.ReadLine());

                var orders = new List<Order>();
                var rand = new Random();
                for (int i = 0; i < number; i++)
                {
                    var order = new Order()
                    {
                        OrderId = i,
                        ClientId = rand.Next(1000000),
                        OfferCode = "badgers",
                        ProductCode = "HGDHGDF",
                        Quantity = 10,
                        UnitPrice = 9.99M
                    };

                    orders.Add(order);
                }

                Console.WriteLine("Enter an exchange: ");
                var exchange = Console.ReadLine();
                Console.WriteLine("Enter a routing key: ");
                var routingKey = Console.ReadLine();
                Console.WriteLine("Enter a message batch size: ");
                var messageBatchSize = int.Parse(Console.ReadLine());

                var sw = new Stopwatch();
                sw.Start();
                var bulkEventPublisher = new BulkMessagePublisher();
                var messageStatesTask = bulkEventPublisher.SendBatchWithRetryAsync(exchange, routingKey, orders, 2, 1000, messageBatchSize);
                messageStatesTask.Wait();
                var messageTracker = messageStatesTask.Result;

                sw.Stop();
                Console.WriteLine("Milliseconds elapsed: " + (int)sw.Elapsed.TotalMilliseconds);

                if (messageTracker.PublishingInterrupted)
                {
                    var maxSendCount = messageTracker.GetMessageStates().Max(x => x.SendCount);
                    Console.WriteLine("Publishing was interrupted, with " + (maxSendCount -1) + " retries made");
                    Console.WriteLine("Interruption reason: " + messageTracker.InterruptionReason);
                    Console.WriteLine("Number of republished messages: " + messageTracker.GetMessageStates()
                                                                           .Count(x => x.SendCount > 1));
                }

                int messagesAfter = GetMessageCount();
                int newMessagesInQueue = messagesAfter - messagesBefore;

                if (newMessagesInQueue > number)
                    Console.WriteLine((newMessagesInQueue - number) + " duplicate messages created!!!!!!");
                else
                    Console.WriteLine("No duplicate messages created");

                Console.WriteLine("");
                Console.WriteLine("Final message status counts:");
                var groupedByStatus = messageTracker.GetMessageStates().GroupBy(x => x.Status);
                foreach (var group in groupedByStatus)
                {
                    if (!string.IsNullOrEmpty(group.First().Description))
                        Console.WriteLine(group.Key + " " + group.Count() + " : " + group.First().Description);
                    else
                        Console.WriteLine(group.Key + " " + group.Count());
                }
            }
        }

        private static void SetupNewOrders()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare("order", "topic", true);
                    channel.QueueDeclare("order.new", true, false, false, new Dictionary<string, object>());
                    channel.QueueBind("order.new", "order", "new");
                }
            }
        }

        private static void DeleteNewOrders()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDelete("orders", false);
                    channel.QueueDelete("orders.new", false);
                }
            }
        }

        private static int GetMessageCount()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    var declareOk = channel.QueueDeclare("order.new", true, false, false, new Dictionary<string, object>());
                    return (int)declareOk.MessageCount;
                }
            }
        }
    }
}
