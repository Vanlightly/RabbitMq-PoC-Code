using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMqMessageTracking
{
    public class MessageTracker<T> : IMessageTracker<T>
    {
        // To keep all messages to be sent
        private List<MessageState<T>> _resultsMaster;

        // For high performance access based on delivery tag (sequence number)
        private ConcurrentDictionary<ulong, MessageState<T>> _resultsByDeliveryTag;

        // For high performance access based on message id
        private ConcurrentDictionary<string, MessageState<T>> _resultsByMessageId;


        private int _attempt;
        private int _messageCount;
        private int _acknowledgedCount;
        private bool _channelClosed;
        private string _channelClosedReason;
        private Exception _unexpectedException;

        private ulong _lastAcknowledgedDeliveryTag;
        private object _syncObj = new object();

        public MessageTracker(List<T> payloads)
        {
            _resultsByDeliveryTag = new ConcurrentDictionary<ulong, MessageState<T>>();
            _resultsByMessageId = new ConcurrentDictionary<string, MessageState<T>>();
            _resultsMaster = new List<MessageState<T>>();

            foreach (var payload in payloads)
            {
                Interlocked.Increment(ref _messageCount);
                var outgoingMessage = new MessageState<T>(payload);
                _resultsByMessageId.TryAdd(outgoingMessage.MessageId, outgoingMessage);
                _resultsMaster.Add(outgoingMessage);
            }
        }

        private MessageTracker(ConcurrentDictionary<string, MessageState<T>> resultsByMessageId,
            List<MessageState<T>> results)
        {
            _resultsByDeliveryTag = new ConcurrentDictionary<ulong, MessageState<T>>();
            _resultsByMessageId = resultsByMessageId;
            _resultsMaster = results;
        }

        public MessageTracker<T> GetCloneWithWipedDeliveryTags()
        {
            // no need to keep messages that will not be retried in
            // the message id dictionary
            var resultsByMessageId = new ConcurrentDictionary<string, MessageState<T>>();
            foreach (var key in _resultsByMessageId.Keys)
            {
                var result = _resultsByMessageId[key];
                if (CannotBeRetried(result.Status))
                    resultsByMessageId.TryAdd(key, result);
            }

            return new MessageTracker<T>(resultsByMessageId, _resultsMaster);
        }

        public List<MessageState<T>> GetRetryableMessages()
        {
            return _resultsMaster.Where(x => !CannotBeRetried(x.Status)).ToList();
        }

        private bool CannotBeRetried(SendStatus status)
        {
            return status == SendStatus.Success || status == SendStatus.NoExchangeFound || status == SendStatus.Unroutable;
        }

        public void RegisterNewAttempt()
        {
            _attempt++;
        }

        public void SetDeliveryTag(ulong deliveryTag, MessageState<T> outgoingMessage)
        {
            _resultsByDeliveryTag.TryAdd(deliveryTag, outgoingMessage);
        }

        public void SetStatus(ulong deliveryTag, bool multiple, SendStatus status)
        {
            SetStatus(deliveryTag, multiple, status, "");
        }

        public void SetStatus(ulong deliveryTag, bool multiple, SendStatus status, string description)
        {
            lock (_syncObj)
            {
                if (multiple)
                {
                    for (ulong i = _lastAcknowledgedDeliveryTag + 1; i <= deliveryTag; i++)
                    {
                        Interlocked.Increment(ref _acknowledgedCount);
                        if (_resultsByDeliveryTag.ContainsKey(i))
                        {
                            var result = _resultsByDeliveryTag[i];
                            result.Status = status;
                            result.Description = description;
                        }
                    }
                }
                else
                {
                    Interlocked.Increment(ref _acknowledgedCount);

                    if (_resultsByDeliveryTag.ContainsKey(deliveryTag))
                    {
                        var result = _resultsByDeliveryTag[deliveryTag];
                        result.Status = status;
                        result.Description = description;
                    }
                }

                _lastAcknowledgedDeliveryTag = deliveryTag;
            }
        }

        public void SetStatus(string messageId, SendStatus status)
        {
            SetStatus(messageId, status, "");
        }

        public void SetStatus(string messageId, SendStatus status, string description)
        {
            Interlocked.Increment(ref _acknowledgedCount);

            if (_resultsByMessageId.ContainsKey(messageId))
            {
                var result = _resultsByMessageId[messageId];
                result.Status = status;
                result.Description = description;
            }
        }

        public void RegisterChannelClosed(string reason)
        {
            _channelClosed = true;
            _channelClosedReason = reason;
        }

        public void RegisterUnexpectedException(Exception exception)
        {
            _unexpectedException = exception;
            _channelClosed = true;
            _channelClosedReason = "Unexpected exception";
        }

        public bool ShouldRetry()
        {
            return !_resultsMaster.All(x => x.Status == SendStatus.Success || x.Status == SendStatus.NoExchangeFound || x.Status == SendStatus.Unroutable);
        }

        public List<MessageState<T>> GetMessageStates()
        {
            return _resultsMaster;
        }

        public bool AllMessagesAcknowledged()
        {
            return _messageCount == _acknowledgedCount;
        }

        public bool PublishingInterrupted
        {
            get { return _channelClosed || _unexpectedException != null; }
        }

        public string InterruptionReason
        {
            get { return _channelClosedReason; }
        }

        public Exception UnexpectedException
        {
            get { return _unexpectedException; }
        }
    }
}
