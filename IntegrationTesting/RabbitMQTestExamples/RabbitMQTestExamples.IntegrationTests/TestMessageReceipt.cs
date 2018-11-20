using RabbitMQTestExamples.ConsoleApp;
using RabbitMQTestExamples.IntegrationTests.Helpers;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace RabbitMQTestExamples.IntegrationTests
{
    public class TestMessageReceipt
    {
        [Fact]
        public async Task ConnectionKillerExample()
        {
            ConnectionKiller.Initialize("localhost:15672", "guest", "guest");

            // ARRANGE
            QueueDestroyer.DeleteQueue("queueX", "/");
            var cts = new CancellationTokenSource();
            var fake = new FakeProcessor();
            var myMicroservice = new Consumer(fake);

            // ACT
            myMicroservice.Consume(cts.Token, "queueX");

            // put a break point here and go a look at the connections in the management ui.

            Thread.Sleep(1000);
            var connections = await ConnectionKiller.GetConnectionNamesAsync(10000);
            await ConnectionKiller.ForceCloseConnectionsAsync(connections);

            // Now go back to the management console and you will see the connection state changes to closed

            // To make a test out of this, create a consumer that has auto recovery enabled and make a test that
            // ensures it continues to consume after multiple connection failures
        }

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
