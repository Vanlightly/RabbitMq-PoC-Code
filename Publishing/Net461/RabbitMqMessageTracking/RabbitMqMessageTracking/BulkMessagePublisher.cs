using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMqMessageTracking
{
    public class BulkMessagePublisher
    {
        public async Task<IMessageTracker<T>> SendMessagesAsync<T>(string exchange,
            string routingKey,
            List<T> messages,
            int messageBatchSize)
        {
            var messageTracker = new MessageTracker<T>(messages);

            try
            {
                await SendBatchAsync(exchange, routingKey, messageTracker.GetMessageStates(), messageTracker, messageBatchSize);
            }
            catch (Exception ex)
            {
                messageTracker.RegisterUnexpectedException(ex);
            }

            return messageTracker;
        }

        public async Task<IMessageTracker<T>> SendBatchWithRetryAsync<T>(string exchange,
            string routingKey,
            List<T> messages,
            byte retryLimit,
            short retryPeriodMs,
            int messageBatchSize)
        {
            var messageTracker = new MessageTracker<T>(messages);

            try
            {
                await SendBatchWithRetryAsync(exchange, routingKey, messageTracker.GetMessageStates(), messageTracker, retryLimit, retryPeriodMs, 1, messageBatchSize);
            }
            catch (Exception ex)
            {
                messageTracker.RegisterUnexpectedException(ex);
            }

            return messageTracker;
        }

        private async Task<IMessageTracker<T>> SendBatchWithRetryAsync<T>(string exchange,
            string routingKey,
            List<MessageState<T>> outgoingMessages,
            MessageTracker<T> messageTracker,
            byte retryLimit,
            short retryPeriodMs,
            byte attempt,
            int messageBatchSize)
        {
            Console.WriteLine("Making attempt #" + attempt);

            try
            {
                await SendBatchAsync(exchange, routingKey, outgoingMessages, messageTracker, messageBatchSize);
            }
            catch (Exception ex)
            {
                messageTracker.RegisterUnexpectedException(ex);
            }

            if (messageTracker.ShouldRetry() && (attempt - 1) <= retryLimit)
            {
                attempt++;

                Console.WriteLine("Will make attempt #" + attempt + " in " + retryPeriodMs + "ms");
                await Task.Delay(retryPeriodMs);

                // delivery tags are reset on a new channel so we need to get a cloned tracker with the non retryable payloads
                // we also get the payloads that can be retried and then do another batch send with just these
                var newMessageTracker = messageTracker.GetCloneWithWipedDeliveryTags();
                var retryablePayloads = messageTracker.GetRetryableMessages();

                return await SendBatchWithRetryAsync(exchange, routingKey, retryablePayloads, newMessageTracker, retryLimit, retryPeriodMs, attempt, messageBatchSize);
            }
            else
            {
                return messageTracker;
            }
        }

        private async Task SendBatchAsync<T>(string exchange,
            string routingKey,
            List<MessageState<T>> messageStates,
            MessageTracker<T> messageTracker,
            int messageBatchSize)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            factory.AutomaticRecoveryEnabled = false;

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ConfirmSelect();
                    channel.BasicAcks += (o, a) => AckCallback(a, messageTracker);
                    channel.BasicNacks += (o, a) => NackCallback(a, messageTracker);
                    channel.BasicReturn += (o, a) => ReturnedCallback(a, messageTracker);
                    channel.ModelShutdown += (o, a) => ModelShutdown(a, messageTracker);

                    int counter = 0;
                    foreach (var messageState in messageStates)
                    {
                        counter++;
                        // create the RabbitMq message from the MessagePayload (the Order class)
                        var messageJson = JsonConvert.SerializeObject(messageState.MessagePayload);
                        var body = Encoding.UTF8.GetBytes(messageJson);
                        var properties = channel.CreateBasicProperties();
                        properties.Persistent = true;
                        properties.MessageId = messageState.MessageId;
                        properties.Headers = new Dictionary<string, object>();
                       
                        if (messageState.SendCount > 0)
                            properties.Headers.Add("republished", true);

                        // get the next sequence number (delivery tag) and register it with this MessageState object
                        var deliveryTag = channel.NextPublishSeqNo;
                        messageTracker.SetDeliveryTag(deliveryTag, messageState);
                        messageState.Status = SendStatus.PendingResponse;
                        messageState.SendCount++;

                        // send the message
                        try
                        {
                            channel.BasicPublish(exchange: exchange,
                                                    routingKey: routingKey,
                                                    basicProperties: properties,
                                                    body: body,
                                                    mandatory: true);

                            if (counter % messageBatchSize == 0)
                                channel.WaitForConfirms(TimeSpan.FromMinutes(1));
                        }
                        catch (OperationInterruptedException ex)
                        {
                            if (ex.ShutdownReason.ReplyCode == 404)
                                messageTracker.SetStatus(messageState.MessageId, SendStatus.NoExchangeFound, ex.Message);
                            else
                                messageTracker.SetStatus(messageState.MessageId, SendStatus.Failed, ex.Message);
                        }
                        catch (Exception ex)
                        {
                            messageTracker.SetStatus(messageState.MessageId, SendStatus.Failed, ex.Message);
                        }

                        if (channel.IsClosed || messageTracker.PublishingInterrupted)
                            return;
                    }

                    await Task.Run(() => channel.WaitForConfirms(TimeSpan.FromMinutes(1)));
                }
            } // already disposed exception here
        }

        private void AckCallback<T>(BasicAckEventArgs ea, MessageTracker<T> messageTracker)
        {
            messageTracker.SetStatus(ea.DeliveryTag, ea.Multiple, SendStatus.Success);
        }

        private void NackCallback<T>(BasicNackEventArgs ea, MessageTracker<T> messageTracker)
        {
            messageTracker.SetStatus(ea.DeliveryTag, ea.Multiple, SendStatus.Failed);
        }

        private void ReturnedCallback<T>(BasicReturnEventArgs ea, MessageTracker<T> messageTracker)
        {
            messageTracker.SetStatus(ea.BasicProperties.MessageId,
                SendStatus.Unroutable,
                string.Format("Reply Code: {0} Reply Text: {1}", ea.ReplyCode, ea.ReplyText));
        }

        private void ModelShutdown<T>(ShutdownEventArgs ea, MessageTracker<T> messageTracker)
        {
            if (ea.ReplyCode != 200)
                messageTracker.RegisterChannelClosed("Reply Code: " + ea.ReplyCode + " Reply Text: " + ea.ReplyText);
        }
    }
}
