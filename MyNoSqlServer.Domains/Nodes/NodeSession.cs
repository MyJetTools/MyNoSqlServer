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
        
        public string RemoteLocation { get; }
        public string MyLocation { get; }
        public string Id { get; private set; }
        
        public bool Compression { get; private set; }
        
        public DateTime Created { get; } = DateTime.UtcNow;
        private static readonly TimeSpan PingTimeOut = TimeSpan.FromSeconds(5);
        
        public DateTime LastAccessed { get; private set; } 
        
        
        public TimeSpan Latency { get; private set; }
        
        public DateTime SetResultMoment = DateTime.UtcNow;
        
        public NodeSession(string remoteLocation, string myLocation, DbInstance dbInstance)
        {
            _dbInstance = dbInstance;
            RemoteLocation = remoteLocation;
            LastAccessed = DateTime.UtcNow;
            MyLocation = myLocation;
        }


        private void InitNewSession(string sessionId)
        {

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
        private SyncTransactionGrpcModel _eventInProcess;
        
        private TaskCompletionSource<SyncTransactionGrpcModel> _awaitingTask;
        private readonly object _lockObject = new();
        

        private ValueTask<SyncTransactionGrpcModel> ProcessAsync(long requestId)
        {

            if (requestId == _currentRequestId)
            {
                if (_eventInProcess == null)
                    throw new Exception(
                        "Debug it. _eventInProcess must be not null");

                return new ValueTask<SyncTransactionGrpcModel>(_eventInProcess);
            }


            if (requestId != _currentRequestId + 1)
                throw new Exception(
                    $"Debug It. Next request Id must be greater the previous one by 1. Current request ID is {_currentRequestId}");

            //If processId +1 - previous one is done Ok. We reset current state
            //ToDo - Double Check it
            _eventInProcess = null;
            _currentRequestId = requestId;
            LastAccessed = DateTime.UtcNow;

            Latency = LastAccessed - SetResultMoment;


            if (_events.Count == 0)
            {
                _awaitingTask = new TaskCompletionSource<SyncTransactionGrpcModel>();
                return new ValueTask<SyncTransactionGrpcModel>(_awaitingTask.Task);
            }

            var nextEvent = _events.Dequeue();
            _eventInProcess = nextEvent.ToSyncTransactionGrpcModel(MyLocation);
            
            return new ValueTask<SyncTransactionGrpcModel>(_eventInProcess);
        }



        private void SetTaskResult(SyncTransactionGrpcModel response)
        {
            var awaitingTask = _awaitingTask;
            _awaitingTask = null;
            awaitingTask.SetResult(response);
            SetResultMoment = DateTime.UtcNow;
        }

        private void SetTaskException(Exception e)
        {
            if (_awaitingTask == null)
                return;
            
            var awaitingTask = _awaitingTask;
            _awaitingTask = null;
            awaitingTask.SetException(e);
            SetResultMoment = DateTime.UtcNow;
        }
        

        public void NewEvent(ITransactionEvent @event)
        {
            lock (_lockObject)
            {
                if (!_subscribedToTables.ContainsKey(@event.TableName))
                    return;
                
                if (@event.Attributes.HasLocation(RemoteLocation))
                    return;
                
                _events.Enqueue(@event);
                
                if (_awaitingTask == null)
                    return;

                var nextEvent = _events.Dequeue();
                _eventInProcess = nextEvent.ToSyncTransactionGrpcModel(MyLocation);
                SetTaskResult(_eventInProcess);
            }
        }


        public ValueTask<SyncTransactionGrpcModel> ProcessAsync(string sessionId, long requestId, bool compression)
        {
            LastAccessed = DateTime.UtcNow;
            Compression = compression;
            
            lock (_lockObject)
            {
                if (_disposed)
                    throw new Exception("Session is exposed");

                if (Id != sessionId)
                    InitNewSession(sessionId);

                return ProcessAsync(requestId);
            }
        }



        private static readonly SyncTransactionGrpcModel PingResponse = new ();
        public void SendPing()
        {
            lock (_lockObject)
            {
                if (_awaitingTask == null)
                    return;

                if (DateTime.UtcNow - LastAccessed > PingTimeOut)
                {
                    SetTaskResult(PingResponse);
                }
                    
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