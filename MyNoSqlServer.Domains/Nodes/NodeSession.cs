using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Rows;
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
        
        public bool Compress { get; private set; }

        public NodeSession(string location, DbInstance dbInstance)
        {
            _dbInstance = dbInstance;
            Location = location;
            LastAccessed = DateTime.UtcNow;
        }


        private void InitNewSession(string sessionId, bool compress)
        {
            
            Compress = compress;

            if (_awaitingTask != null)
                SetTaskException(new Exception("Now session is arrived. Old session is expired"));


            Id = sessionId;
            
            _events.Clear();
            _currentRequestId = -1;
            _subscribedToTables.Clear();


            var tables = _dbInstance.GetTables();

            
            var attrs = new TransactionEventAttributes(null,
                DataSynchronizationPeriod.Immediately,
                EventSource.Synchronization,
                new Dictionary<string, string>());
            
            foreach (var table in tables)
            {
                table.GetReadAccess(readAccess =>
                {
                    var initTableEvent = FirstInitTableEvent.Create(attrs, table, readAccess.GetAllRows());
                    _subscribedToTables.Add(table.Name, initTableEvent.Table);
                    _events.Enqueue(initTableEvent);
                });
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
                if (_eventInProcess == null)
                    throw new Exception(
                        "Debug it. It must be not null");

                return new ValueTask<SyncGrpcResponse>(_eventInProcess);
            }


            if (requestId != _currentRequestId + 1)
                throw new Exception(
                    $"Debug It. Next request Id must be greater the previous one by 1. Current request ID is {_currentRequestId}");

            //If processId +1 - previous one is done Ok. We reset current state
            //ToDo - Double Check it
            _eventInProcess = null;
            _currentRequestId = requestId;


            if (_events.Count == 0)
            {
                _awaitingTask = new TaskCompletionSource<SyncGrpcResponse>();
                _taskSetTime = DateTime.UtcNow;
                return new ValueTask<SyncGrpcResponse>(_awaitingTask.Task);
            }

            var nextEvent = _events.Dequeue();
            _eventInProcess = nextEvent.ToSyncGrpcResponse(Compress);
            
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
                
                if (@event.Attributes.HasLocation(Location))
                    return;
                
                _events.Enqueue(@event);
                
                if (_awaitingTask == null)
                    return;

                var nextEvent = _events.Dequeue();
                _eventInProcess = nextEvent.ToSyncGrpcResponse(Compress);
                SetTaskResult(_eventInProcess);
            }
        }


        public ValueTask<SyncGrpcResponse> ProcessAsync(string sessionId, long requestId, bool compress)
        {
            LastAccessed = DateTime.UtcNow;
  
            
            lock (_lockObject)
            {
                if (_disposed)
                    throw new Exception("Session is exposed");

                if (Id != sessionId)
                    InitNewSession(sessionId, compress);

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