using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.TransactionEvents;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.Domains.Nodes
{
    
    /// <summary>
    /// Representation of new Node Session.
    /// </summary>
    
    public class NodeSession : IDisposable
    {
        private readonly DbInstance _dbInstance;

        private readonly Queue<ITransactionEvent> _events = new();

        private readonly Dictionary<string, DbTable> _subscribedToTables = new();

        private bool _disposed;
        
        public string Location { get; }
        public string Id { get; private set; }
        
        public DateTime Created { get; } = DateTime.UtcNow;
        private static readonly TimeSpan PingTimeOut = TimeSpan.FromSeconds(3);
        
        public DateTime LastAccessed { get; private set; } 

        public NodeSession(string location, DbInstance dbInstance)
        {
            _dbInstance = dbInstance;
            Location = location;
            LastAccessed = DateTime.UtcNow;
        }


        private void InitNewSession(string sessionId)
        {

            if (_awaitingTask != null)
                SetTaskException(new Exception("Session is expired"));


            Id = sessionId;
            
            _events.Clear();
            _currentRequestId = -1;
            _subscribedToTables.Clear();


            var tables = _dbInstance.GetTables();

            foreach (var table in tables)
            {
                var @event = InitTableEvent.Create(
                    new TransactionEventAttributes(Location,
                        DataSynchronizationPeriod.Immediately,
                        new Dictionary<string, string>()),
                    table);
                _events.Enqueue(@event);
            }

        }

        private long _currentRequestId;
        private SyncGrpcResponse _eventInProcess;
        
        private TaskCompletionSource<SyncGrpcResponse> _awaitingTask;
        private DateTime _taskSetTime;
        private readonly object _lockObject = new();
        

        private ValueTask<SyncGrpcResponse> ProcessAsync(long requestId)
        {

            if (requestId == _currentRequestId)
            {
                if (_eventInProcess != null)
                    return new ValueTask<SyncGrpcResponse>(_eventInProcess);
            }
                

            if (requestId != _currentRequestId + 1)
                throw new Exception(
                    $"Next request Id must be greater the previous one by 1. Current request ID is {_currentRequestId}");

            //If processId +1 - previous one is done Ok. We reset current state
            _eventInProcess = null;
            _currentRequestId = requestId;


            if (_events.Count == 0)
            {
                _awaitingTask = new TaskCompletionSource<SyncGrpcResponse>();
                _taskSetTime = DateTime.UtcNow;
                return new ValueTask<SyncGrpcResponse>(_awaitingTask.Task);
            }

            var nextEvent = _events.Dequeue();
            _eventInProcess = nextEvent.ToSyncGrpcResponse();

            if (nextEvent is InitTableEvent initTableEvent)
                _subscribedToTables.Add(initTableEvent.TableName, initTableEvent.Table);
            
            return new ValueTask<SyncGrpcResponse>(_eventInProcess);
        }



        private void SetTaskResult(SyncGrpcResponse response)
        {
            var awaitingTask = _awaitingTask;
            _awaitingTask = null;
            awaitingTask.SetResult(response);
        }

        private void SetTaskException(Exception e)
        {
            var awaitingTask = _awaitingTask;
            _awaitingTask = null;
            awaitingTask.SetException(e);
        }
        

        public void NewEvent(ITransactionEvent @event)
        {
            lock (_lockObject)
            {
                if (!_subscribedToTables.ContainsKey(@event.TableName))
                    return;
                
                if (@event.Attributes.Location == Location)
                    return;
                
                _events.Enqueue(@event);
                
                if (_awaitingTask == null)
                    return;

                var nextEvent = _events.Dequeue();
                _eventInProcess = nextEvent.ToSyncGrpcResponse();
                SetTaskResult(_eventInProcess);
            }
        }


        public ValueTask<SyncGrpcResponse> ProcessAsync(string sessionId, long requestId)
        {
            LastAccessed = DateTime.UtcNow;
            
            lock (_lockObject)
            {
                if (_disposed)
                    throw new Exception("Session is exposed");

                if (Id != sessionId)
                    InitNewSession(sessionId);

                return ProcessAsync(requestId);
            }
        }


        public void SendPing()
        {
            lock (_lockObject)
            {
                if (_awaitingTask == null)
                    return;

                if (_taskSetTime - DateTime.UtcNow > PingTimeOut)
                    SetTaskResult(new SyncGrpcResponse());
            }

        }

        public void Dispose()
        {
            lock (_lockObject)
            {
                
                if (_disposed)
                    return;

                _disposed = true;

                SetTaskException(new Exception("Session expired"));
            }
        }
    }
}