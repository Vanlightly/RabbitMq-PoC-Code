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
        /// <summary>
        /// Sends the messages and tracks the send status of each message. Any exceptions are controlled and added to the returned IMessageTracker
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="exchange">The exchange to send to</param>
        /// <param name="routingKey">The routing key, empty string is permitted</param>
        /// <param name="messages">A list of objects that will be converted to JSON and sent as individual messages</param>
        /// <param name="messageBatchSize">Publishing will publish this number of messages at a time and then pause and wait for confirmation of delivery. Once an acknowledgement
        /// of each message has been received, or a timeout is reache, the next batch is sent</param>
        /// <param name="safetyPeriod">Adds extra guarantees of correct message send status. Confirms can be received out of order. This means that once all
        /// messages have been sent the channel can be closed prematurely due to incorrect ordering of confirms. The safety period keeps the channel open for an extra period, just in case we
        /// receive more confirms. This safety period is not required when the messageBatchSize is 1</param>
        /// <returns>A message tracker that provides you with the delivery status (to the exchange and queues - not the consumer) information, including errors that may have occurred</returns>
        public async Task<IMessageTracker<T>> SendMessagesAsync<T>(string exchange,
            string routingKey,
            List<T> messages,
            int messageBatchSize,
            TimeSpan safetyPeriod)
        {
            var messageTracker = new MessageTracker<T>(messages);

            try
            {
                await SendBatchAsync(exchange, 
                    routingKey, 
                    messageTracker.GetMessageStates(), 
                    messageTracker, 
                    messageBatchSize,
                    safetyPeriod).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                messageTracker.RegisterUnexpectedException(ex);
            }

            return messageTracker;
        }

        /// <summary>
        /// Sends the messages and tracks the send status of each message. Any exceptions are controlled and added to the returned IMessageTracker.
        /// Additionally, this overload provides retries.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="exchange">The exchange to send to</param>
        /// <param name="routingKey">The routing key, empty string is permitted</param>
        /// <param name="messages">A list of objects that will be converted to JSON and sent as individual messages</param>
        /// <param name="retryLimit">The number of retries to perform. If you set it to 3 for example, then up to 4 attempts are made in total</param>
        /// <param name="retryPeriodMs">Milliseconds between each attempt</param>
        /// <param name="messageBatchSize">Publishing will publish this number of messages at a time and then pause and wait for confirmation of delivery. Once an acknowledgement
        /// of each message has been received, or a timeout is reache, the next batch is sent</param>
        /// <param name="safetyPeriod">Adds extra guarantees of correct message send status. Confirms can be received out of order. This means that once all
        /// messages have been sent the channel can be closed prematurely due to incorrect ordering of confirms. The safety period keeps the channel open for an extra period, just in case we
        /// receive more confirms. This safety period is not required when the messageBatchSize is 1</param>
        /// <returns>A message tracker that provides you with the delivery status (to the exchange and queues - not the consumer) information, including errors that may have occurred</returns>
        public async Task<IMessageTracker<T>> SendBatchWithRetryAsync<T>(string exchange,
            string routingKey,
            List<T> messages,
            byte retryLimit,
            short retryPeriodMs,
            int messageBatchSize,
            TimeSpan safetyPeriod)
        {
            var messageTracker = new MessageTracker<T>(messages);

            try
            {
                messageTracker = await SendBatchWithRetryAsync(exchange, 
                    routingKey, 
                    messageTracker.GetMessageStates(), 
                    messageTracker, 
                    retryLimit, 
                    retryPeriodMs, 
                    1, 
                    messageBatchSize,
                    safetyPeriod).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                messageTracker.RegisterUnexpectedException(ex);
            }

            return messageTracker;
        }

        private async Task<MessageTracker<T>> SendBatchWithRetryAsync<T>(string exchange,
            string routingKey,
            List<MessageState<T>> outgoingMessages,
            MessageTracker<T> messageTracker,
            byte retryLimit,
            short retryPeriodMs,
            byte attempt,
            int messageBatchSize,
            TimeSpan safetyPeriod)
        {
            Console.WriteLine("Making attempt #" + attempt);

            try
            {
                await SendBatchAsync(exchange,
                    routingKey, 
                    outgoingMessages, 
                    messageTracker, 
                    messageBatchSize,
                    safetyPeriod).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                messageTracker.RegisterUnexpectedException(ex);
            }
            finally
            {
                messageTracker.AssignStatuses();
            }

            if (messageTracker.ShouldRetry() && (attempt - 1) <= retryLimit)
            {
                attempt++;

                Console.WriteLine("Will make attempt #" + attempt + " in " + retryPeriodMs + "ms");
                await Task.Delay(retryPeriodMs).ConfigureAwait(false);

                // delivery tags are reset on a new channel so we need to get a cloned tracker with:
                // - an empty delivert tag dictionary
                // - acknowledgement flag set to false on all message states that we will retry
                // we also get the payloads that can be retried and then do another batch send with just these
                var newMessageTracker = messageTracker.GetCloneWithResetAcknowledgements();
                var retryablePayloads = messageTracker.GetRetryableMessages();

                return await SendBatchWithRetryAsync(exchange, 
                    routingKey, 
                    retryablePayloads, 
                    newMessageTracker, 
                    retryLimit, 
                    retryPeriodMs, 
                    attempt, 
                    messageBatchSize,
                    safetyPeriod).ConfigureAwait(false);
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
            int messageBatchSize,
            TimeSpan safetyPeriod)
        {
            messageTracker.AttemptsMade++;

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

                    channel.WaitForConfirms(TimeSpan.FromMinutes(1));

                    if (safetyPeriod.Ticks > 0)
                    {
                        // add extra buffer in case of out of order last confirm
                        await Task.Delay(safetyPeriod).ConfigureAwait(false);
                    }
                }
            } // already disposed exception here
        }

        private void AckCallback<T>(BasicAckEventArgs ea, MessageTracker<T> messageTracker)
        {
            messageTracker.SetStatus(ea.DeliveryTag, SendStatus.Success);
        }

        private void NackCallback<T>(BasicNackEventArgs ea, MessageTracker<T> messageTracker)
        {
            messageTracker.SetStatus(ea.DeliveryTag, SendStatus.Failed);
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
