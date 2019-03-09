﻿using System;
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
        private List<MessageState<T>> _statesMaster;

        // For high performance access based on delivery tag (sequence number)
        private ConcurrentDictionary<ulong, MessageState<T>> _statesByDeliveryTag;

        // For high performance access based on message id
        private ConcurrentDictionary<string, MessageState<T>> _statesByMessageId;


        private int _attempt;
        private int _messageCount;
        private bool _channelClosed;
        private string _channelClosedReason;
        private Exception _unexpectedException;

        public MessageTracker(List<T> payloads)
        {
            _statesByDeliveryTag = new ConcurrentDictionary<ulong, MessageState<T>>();
            _statesByMessageId = new ConcurrentDictionary<string, MessageState<T>>();
            _statesMaster = new List<MessageState<T>>();

            foreach (var payload in payloads)
            {
                var outgoingMessage = new MessageState<T>(payload);
                _statesByMessageId.TryAdd(outgoingMessage.MessageId, outgoingMessage);
                _statesMaster.Add(outgoingMessage);
            }
        }

        private MessageTracker(ConcurrentDictionary<string, MessageState<T>> resultsByMessageId,
            List<MessageState<T>> results,
            int attemptsMade)
        {
            _statesByDeliveryTag = new ConcurrentDictionary<ulong, MessageState<T>>();
            _statesByMessageId = resultsByMessageId;
            _statesMaster = results;
            AttemptsMade = attemptsMade;
        }

        public MessageTracker<T> GetCloneWithResetAcknowledgements()
        {
            // no need to keep messages that will not be retried in
            // the message id dictionary
            var statesByMessageId = new ConcurrentDictionary<string, MessageState<T>>();
            foreach (var key in _statesByMessageId.Keys)
            {
                var result = _statesByMessageId[key];
                if (CanBeRetried(result.Status))
                {
                    result.Acknowledged = false;
                    statesByMessageId.TryAdd(key, result);
                }
            }
            
            // reset sequence numbers
            foreach (var message in _statesMaster)
                message.SequenceNumber = 0;

            return new MessageTracker<T>(statesByMessageId, _statesMaster, AttemptsMade);
        }

        public List<MessageState<T>> GetRetryableMessages()
        {
            return _statesMaster.Where(x => !CannotBeRetried(x.Status)).ToList();
        }

        private bool CanBeRetried(SendStatus status)
        {
            return !CannotBeRetried(status);
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
            outgoingMessage.SequenceNumber = deliveryTag;
            _statesByDeliveryTag.TryAdd(deliveryTag, outgoingMessage);
        }

        public void SetStatus(ulong deliveryTag, SendStatus status)
        {
            SetStatus(deliveryTag, status, "");
        }

        public void SetStatus(ulong deliveryTag, SendStatus status, string description)
        {
            var messageState = _statesByDeliveryTag[deliveryTag];
            SetSendStatus(messageState, status, description);
        }

        public void SetStatus(string messageId, SendStatus status)
        {
            SetStatus(messageId, status, "");
        }

        public void SetStatus(string messageId, SendStatus status, string description)
        {
            var messageState = _statesByMessageId[messageId];
            SetSendStatus(messageState, status, description);
        }

        public void SetMultipleStatus(ulong deliveryTag, SendStatus status)
        {
            var pendingResponse = _statesMaster.Where(x => x.SequenceNumber > 0
                                            && x.SequenceNumber <= deliveryTag
                                            && x.Status == SendStatus.PendingResponse);

            foreach (var pending in pendingResponse)
                SetStatus(pending.SequenceNumber, status);
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
            return !_statesMaster.All(x => x.Status == SendStatus.Success || x.Status == SendStatus.NoExchangeFound || x.Status == SendStatus.Unroutable);
        }

        public List<MessageState<T>> GetMessageStates()
        {
            return _statesMaster;
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

        public int AttemptsMade { get; set; }

        private void SetSendStatus(MessageState<T> messageState, SendStatus status, string description)
        {
            if (status == SendStatus.NoExchangeFound)
            {
                foreach (var state in _statesMaster)
                {
                    state.Status = status;
                    state.Acknowledged = true;
                }
            }
            // unroutable messages get a BasicReturn followed by a BasicAck, so we want to ignore that ack
            else if (messageState.Status != SendStatus.Unroutable)
            {
                messageState.Status = status;
                messageState.Description = description;
                messageState.Acknowledged = true;
            }
        }
    }
}
