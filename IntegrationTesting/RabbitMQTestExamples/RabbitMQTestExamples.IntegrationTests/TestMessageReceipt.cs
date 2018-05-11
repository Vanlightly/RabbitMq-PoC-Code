using RabbitMQTestExamples.ConsoleApp;
using RabbitMQTestExamples.IntegrationTests.Helpers;
using System;
using System.Threading;
using Xunit;

namespace RabbitMQTestExamples.IntegrationTests
{
    public class TestMessageReceipt
    {
        [Fact]
        public void If_SendMessageToQueue_ThenConsumerReceiv4es()
        {
            // ARRANGE
            QueueDestroyer.DeleteQueue("queueX", "/");
            var cts = new CancellationTokenSource();
            var fake = new FakeProcessor();
            var myMicroservice = new Consumer(fake);

            // ACT
            myMicroservice.Consume(cts.Token, "queueX");

            var producer = new TestPublisher();
            producer.Publish("queueX", "hello");

            Thread.Sleep(1000);
            cts.Cancel();

            // ASSERT
            Assert.Equal(1, fake.Messages.Count);
            Assert.Equal("hello", fake.Messages[0]);
        }
    }
}
