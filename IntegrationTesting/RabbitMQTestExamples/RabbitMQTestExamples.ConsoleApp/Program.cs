using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace RabbitMQTestExamples.ConsoleApp
{
    public class RealProcessor : IMessageProcessor
    {
        public void ProcessMessage(string message)
        {
            Console.WriteLine(message);
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine("Hello World!");

                var cts = new CancellationTokenSource();
                var processor = new RealProcessor();
                var consumer = new Consumer(processor);
                consumer.Consume(cts.Token, "queueX");

                var producer = new Publisher();
                producer.Publish("queueX", "hello");

                Console.WriteLine("Press any key to shutdown");
                Console.ReadKey();
                cts.Cancel();
                consumer.WaitForCompletion();
                Console.WriteLine("Shutdown");
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Fatal error: {ex}");
            }
        }

        
    }
}
