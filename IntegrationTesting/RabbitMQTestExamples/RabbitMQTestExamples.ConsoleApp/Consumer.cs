using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQTestExamples.ConsoleApp
{
    public class Consumer
    {
        private IMessageProcessor _messageProcessor;
        private Task _consumerTask;

        public Consumer(IMessageProcessor messageProcessor)
        {
            _messageProcessor = messageProcessor;
        }

        public void Consume(CancellationToken token, string queueName)
        {
            _consumerTask = Task.Run(() =>
            {
                var factory = new ConnectionFactory() { HostName = "localhost" };
                using (var connection = factory.CreateConnection())
                {
                    using (var channel = connection.CreateModel())
                    {
                        channel.QueueDeclare(queue: queueName,
                                        durable: false,
                                        exclusive: false,
                                        autoDelete: false,
                                        arguments: null);

                        var consumer = new EventingBasicConsumer(channel);
                        consumer.Received += (model, ea) =>
                        {
                            var body = ea.Body;
                            var message = Encoding.UTF8.GetString(body);
                            _messageProcessor.ProcessMessage(message);
                        };
                        channel.BasicConsume(queue: queueName,
                                            autoAck: false,
                                                consumer: consumer);

                        while (!token.IsCancellationRequested)
                            Thread.Sleep(1000);
                    }
                }
            });
        }

        public void WaitForCompletion()
        {
            _consumerTask.Wait();
        }

    }
}
