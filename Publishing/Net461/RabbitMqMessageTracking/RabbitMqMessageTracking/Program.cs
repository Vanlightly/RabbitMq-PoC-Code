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
            MainAsync().Wait();
        }

        static async Task MainAsync()
        {
            try
            {
                Console.WriteLine("This code is explained in my blog series on RabbitMq Publishing starting at: http://jack-vanlightly.com/blog/2017/3/11/sending-messages-in-bulk-and-tracking-delivery-status-rabbitmq-publishing-part-2");
                Console.WriteLine("Enter 2 for the code of Part 2");
                Console.WriteLine("Enter 3 for the code of Part 3");
                int part = int.Parse(Console.ReadLine());

                if (part == 2)
                    await Part2().ConfigureAwait(false);
                else if (part == 3)
                    await Part3().ConfigureAwait(false);
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.ToString());

            }
        }


        #region .: Part 2 :.

        private static async Task Part2()
        {
            SetupPart2();

            while (true)
            {
                int messagesBefore = GetMessageCountPart2();
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
                var messageTracker = await bulkEventPublisher.SendBatchWithRetryAsync(exchange, routingKey, orders, 2, 1000, messageBatchSize);
                
                sw.Stop();
                Console.WriteLine("Milliseconds elapsed: " + (int)sw.Elapsed.TotalMilliseconds);

                if (messageTracker.PublishingInterrupted)
                {
                    var maxSendCount = messageTracker.GetMessageStates().Max(x => x.SendCount);
                    Console.WriteLine("Publishing was interrupted, with " + (messageTracker.AttemptsMade - 1) + " retries made");
                    Console.WriteLine("Interruption reason: " + messageTracker.InterruptionReason);
                    Console.WriteLine("Number of republished messages: " + messageTracker.GetMessageStates()
                                                                           .Count(x => x.SendCount > 1));
                }

                int messagesAfter = GetMessageCountPart2();
                int newMessagesInQueue = messagesAfter - messagesBefore;
                int confirmedSuccessCount = messageTracker.GetMessageStates().Count(x => x.Status == SendStatus.Success);
                int unackedCount = messageTracker.GetMessageStates().Count(x => x.Status == SendStatus.PendingResponse);

                if (newMessagesInQueue > confirmedSuccessCount + unackedCount)
                    Console.WriteLine((newMessagesInQueue - (confirmedSuccessCount + unackedCount)) + " duplicate messages created!!!!!!");
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

        private static void SetupPart2()
        {
            DeletePart2();
            DeletePart3();

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

        private static void DeletePart2()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDelete("order", false);
                    channel.QueueDelete("order.new", false);
                }
            }
        }

        private static int GetMessageCountPart2()
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

        #endregion .: Part 2 :.


        #region .: Part 3 :.

        private static async Task Part3()
        {
            SetupPart3();

            while (true)
            {
                int messagesBefore = GetOrderQueueMessageCountPart3();
                int unroutableBefore = GetUnroutableOrderQueueMessageCountPart3();
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
                var messageTracker = await bulkEventPublisher.SendBatchWithRetryAsync(exchange, routingKey, orders, 2, 1000, messageBatchSize);
                
                sw.Stop();
                Console.WriteLine("Milliseconds elapsed: " + (int)sw.Elapsed.TotalMilliseconds);

                if (messageTracker.PublishingInterrupted)
                {
                    var maxSendCount = messageTracker.GetMessageStates().Max(x => x.SendCount);
                    Console.WriteLine("Publishing was interrupted, with " + (messageTracker.AttemptsMade - 1) + " retries made");
                    Console.WriteLine("Interruption reason: " + messageTracker.InterruptionReason);
                    Console.WriteLine("Number of republished messages: " + messageTracker.GetMessageStates()
                                                                           .Count(x => x.SendCount > 1));
                }

                int messagesAfter = GetOrderQueueMessageCountPart3();
                int unroutableAfter = GetUnroutableOrderQueueMessageCountPart3();

                int addedMessages = messagesAfter > messagesBefore ? messagesAfter - messagesBefore : 0;
                int unroutableMessages = unroutableAfter - unroutableBefore;
                Console.WriteLine(addedMessages + " added to order.new, " + unroutableMessages + " added to order.unroutable");

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

        private static void DeletePart3()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDelete("order", false);
                    channel.QueueDelete("order.new", false);
                    channel.ExchangeDelete("order.unroutable", false);
                    channel.QueueDelete("order.unroutable", false);
                }
            }
        }

        private static void SetupPart3()
        {
            DeletePart2();

            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare("order.unroutable", "headers", true);
                    channel.QueueDeclare("order.unroutable", true, false, false, null);
                    channel.QueueBind("order.unroutable", "order.unroutable", "");

                    var props = new Dictionary<string, object>();
                    props.Add("alternate-exchange", "order.unroutable");
                    channel.ExchangeDeclare("order", "topic", true, false, props);
                    channel.QueueDeclare("order.new", true, false, false, null);
                    channel.QueueBind("order.new", "order", "new");
  
                }
            }
        }

        private static int GetOrderQueueMessageCountPart3()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    var declareOk = channel.QueueDeclare("order.new", true, false, false, null);
                    return (int)declareOk.MessageCount;
                }
            }
        }

        private static int GetUnroutableOrderQueueMessageCountPart3()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    var declareOk = channel.QueueDeclare("order.unroutable", true, false, false, null);
                    return (int)declareOk.MessageCount;
                }
            }
        }

        #endregion .: Part 3 :.
    }
}
