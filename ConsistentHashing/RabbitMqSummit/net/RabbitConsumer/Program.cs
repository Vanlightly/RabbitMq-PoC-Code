using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Rebalanser.Core;
using Rebalanser.SqlServer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitConsumer
{
    class CmdArgumentException : Exception
    {
        public CmdArgumentException(string message)
            : base(message)
        {}
    }

    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                var builder = new ConfigurationBuilder().AddCommandLine(args);
                IConfigurationRoot configuration = builder.Build();

                string mode = GetMandatoryArg(configuration, "Mode");
                if (mode == "publish")
                {
                    var exchange = GetMandatoryArg(configuration, "Exchange");
                    var stateCount = int.Parse(GetMandatoryArg(configuration, "Keys"));
                    var messageCount = int.Parse(GetMandatoryArg(configuration, "Messages"));
                    PublishSequence(exchange, stateCount, messageCount);
                }
                else if (mode == "input")
                {
                    string consumerGroup = GetMandatoryArg(configuration, "Group");
                    string outputQueue = GetMandatoryArg(configuration, "OutQueue");
                    int minProcessingMs = 0;
                    int maxProcessingMs = 0;
                    if (args.Length == 4)
                    {
                        minProcessingMs = int.Parse(GetMandatoryArg(configuration, "MinMs"));
                        maxProcessingMs = int.Parse(GetMandatoryArg(configuration, "MaxMs"));
                    }
                    RunRebalanserAsync(consumerGroup, outputQueue, minProcessingMs, maxProcessingMs).Wait();
                }
                else if (mode == "output")
                {
                    string queue = args[1];
                    StartConsumingAndPrinting(GetMandatoryArg(configuration, "Queue"));
                }
                else
                {
                    Console.WriteLine("Unknown command");
                }
            }
            catch(CmdArgumentException ex)
            {
                Console.WriteLine(ex.Message);
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

        static string GetMandatoryArg(IConfiguration configuration, string argName)
        {
            var value = configuration[argName];
            if (string.IsNullOrEmpty(value))
                throw new CmdArgumentException($"No argument {argName}");

            return value;
        }

        private static List<ClientTask> clientTasks;

        private static void PublishSequence(string exchange, int stateCount, int messageCount)
        {
            var states = new string[] { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j" };
            var stateIndex = 0;
            var value = 1;

            try
            {
                var factory = new ConnectionFactory() { HostName = "localhost" };
                var connection = factory.CreateConnection();
                var channel = connection.CreateModel();

                try
                {
                    while(value <= messageCount)
                    {
                        var message = $"{states[stateIndex]}={value}";
                        var body = Encoding.UTF8.GetBytes(message);
                        channel.BasicPublish(exchange, stateIndex.ToString(), null, body);

                        stateIndex++;
                        if (stateIndex == stateCount)
                        {
                            stateIndex = 0;
                            value++;
                        }
                            
                    }
                }
                finally
                {
                    channel.Close();
                    connection.Dispose();
                }
            }
            catch (Exception ex)
            {
                LogError(ex.ToString());
            }

            LogInfo("Messages sent");
        }

        private static async Task RunRebalanserAsync(string consumerGroup, string outputQueue, int minProcessingMs, int maxProcessingMs)
        {
            Providers.Register(new SqlServerProvider("Server=(local);Database=RabbitMqScaling;Trusted_Connection=true;"));
            clientTasks = new List<ClientTask>();

            using (var context = new RebalanserContext())
            {
                context.OnAssignment += (sender, args) =>
                {
                    var queues = context.GetAssignedResources();
                    foreach (var queue in queues)
                    {
                        StartConsumingAndPublishing(queue, outputQueue, minProcessingMs, maxProcessingMs);
                    }
                };

                context.OnCancelAssignment += (sender, args) =>
                {
                    LogInfo("Consumer subscription cancelled");
                    StopAllConsumption();
                };

                context.OnError += (sender, args) =>
                {
                    LogInfo($"Error: {args.Message}, automatic recovery set to: {args.AutoRecoveryEnabled}, Exception: {args.Exception.Message}");
                };

                await context.StartAsync(consumerGroup, new ContextOptions() { AutoRecoveryOnError = true, RestartDelay = TimeSpan.FromSeconds(30) });

                Console.WriteLine("Press enter to shutdown");
                while (!Console.KeyAvailable)
                {
                    Thread.Sleep(100);
                }

                StopAllConsumption();
                Task.WaitAll(clientTasks.Select(x => x.Client).ToArray());
            }
        }

        private static void StartConsumingAndPublishing(string queueName, string outputQueue, int minProcessingMs, int maxProcessingMs)
        {
            LogInfo("Subscription started for queue: " + queueName);
            var cts = new CancellationTokenSource();
            var rand = new Random(Guid.NewGuid().GetHashCode());
            
            var task = Task.Factory.StartNew(() =>
            {
                try
                {
                    var factory = new ConnectionFactory() { HostName = "localhost" };
                    var connection = factory.CreateConnection();
                    var receiveChannel = connection.CreateModel();
                    var sendChannel = connection.CreateModel();
                    try
                    {
                        receiveChannel.BasicQos(0, 1, false);
                        var consumer = new EventingBasicConsumer(receiveChannel);
                        consumer.Received += (model, ea) =>
                        {
                            var body = ea.Body;
                            var message = Encoding.UTF8.GetString(body);
                            sendChannel.BasicPublish(exchange: "",
                                     routingKey: outputQueue,
                                     basicProperties: null,
                                     body: body);
                            receiveChannel.BasicAck(ea.DeliveryTag, false);
                            Console.WriteLine(message);

                            if (maxProcessingMs > 0)
                            {
                                var waitMs = rand.Next(minProcessingMs, maxProcessingMs);
                                Thread.Sleep(waitMs);
                            }
                        };

                        receiveChannel.BasicConsume(queue: queueName,
                                             autoAck: false,
                                             consumer: consumer);

                        while (!cts.Token.IsCancellationRequested)
                            Thread.Sleep(100);
                    }
                    finally
                    {
                        receiveChannel.Close();
                        sendChannel.Close();
                        connection.Dispose();
                    }
                }
                catch (Exception ex)
                {
                    LogError(ex.ToString());
                }

                if (cts.Token.IsCancellationRequested)
                {
                    //LogInfo("Cancellation signal received for " + queueName);
                }
                else
                    LogInfo("Consumer stopped for " + queueName);
            }, TaskCreationOptions.LongRunning);

            clientTasks.Add(new ClientTask() { Cts = cts, Client = task });
        }

        private static void StopAllConsumption()
        {
            foreach (var ct in clientTasks)
            {
                ct.Cts.Cancel();
            }
        }

        private static void StartConsumingAndPrinting(string queueName)
        {
            var cts = new CancellationTokenSource();

            try
            {
                var factory = new ConnectionFactory() { HostName = "localhost" };
                var connection = factory.CreateConnection();
                var receiveChannel = connection.CreateModel();

                var states = new Dictionary<string, int>();
                try
                {
                    var consumer = new EventingBasicConsumer(receiveChannel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        var parts = message.Split("=");
                        var key = parts[0];
                        var currValue = int.Parse(parts[1]);
                        
                        if (states.ContainsKey(key))
                        {
                            var lastValue = states[key];

                            if (lastValue + 1 < currValue)
                                Console.WriteLine($"{message} JUMP FORWARDS {currValue - lastValue}");
                            else if (currValue < lastValue)
                                Console.WriteLine($"{message} JUMP BACKWARDS {lastValue - currValue}");
                            else
                                Console.WriteLine(message);

                            states[key] = currValue;
                        }
                        else
                        {
                            if(currValue > 1)
                                Console.WriteLine($"{message} JUMP FORWARDS {currValue}");
                            else
                                Console.WriteLine(message);

                            states.Add(key, currValue);
                        }

                        receiveChannel.BasicAck(ea.DeliveryTag, false);
                    };

                    receiveChannel.BasicConsume(queue: queueName,
                                         autoAck: false,
                                         consumer: consumer);

                    while (!cts.Token.IsCancellationRequested)
                        Thread.Sleep(100);
                }
                finally
                {
                    receiveChannel.Close();
                    connection.Dispose();
                }
            }
            catch (Exception ex)
            {
                LogError(ex.ToString());
            }

            if (cts.Token.IsCancellationRequested)
                LogInfo("Cancellation signal received for " + queueName);
            else
                LogInfo("Consumer stopped for " + queueName);
        }

        private static void LogInfo(string text)
        {
            Console.WriteLine($"{DateTime.Now.ToString("hh:mm:ss,fff")}: INFO  : {text}");
        }

        private static void LogError(string text)
        {
            Console.WriteLine($"{DateTime.Now.ToString("hh:mm:ss,fff")}: ERROR  : {text}");
        }
    }
}
