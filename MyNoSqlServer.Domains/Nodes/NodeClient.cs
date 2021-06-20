using System;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.TransactionEvents;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.Domains.Nodes
{
    public class NodeClient
    {
        private readonly NodesSyncOperations _nodesSyncOperations;
        private readonly IMyNoSqlServerNodeSynchronizationGrpcService _grpcService;
        private readonly ISettingsLocation _settingsLocation;

        private readonly string _sessionId;

        public NodeClient(NodesSyncOperations nodesSyncOperations,
            IMyNoSqlServerNodeSynchronizationGrpcService grpcService, ISettingsLocation settingsLocation)
        {
            _nodesSyncOperations = nodesSyncOperations;
            _grpcService = grpcService;
            _settingsLocation = settingsLocation;
            _sessionId = Guid.NewGuid().ToString("N");
        }


        private async Task ThreadLoopAsync()
        {

            long requestId = 0;

            while (_working)
            {
                try
                {
                    var result = await _grpcService.SyncAsync(new SyncGrpcRequest
                    {
                        Location = _settingsLocation.Location,
                        RequestId = requestId,
                        SessionId = _sessionId
                    });

                    HandleSyncEvent(result);

                    requestId++;

                }
                catch (Exception e)
                {
                    Console.WriteLine("NodeClient: "+e);
                }
                
            }
            
        }


        private TransactionEventAttributes CreateTransactionEventAttribute()
        {
            throw new NotImplementedException();
        }


        private void HandleSyncEvent(SyncGrpcResponse syncGrpcResponse)
        {

            var transactionEvents = syncGrpcResponse.ToTransactionEvents(CreateTransactionEventAttribute);

            foreach (var transactionEvent in transactionEvents)
            {
                switch (transactionEvent)
                {
                    
                    case UpdateTableAttributesTransactionEvent updateTableAttributesTransactionEvent:
                        _nodesSyncOperations.SetTableAttributes(updateTableAttributesTransactionEvent);
                        break;
                    
                    case InitTableTransactionEvent initTableTransactionEvent:
                        _nodesSyncOperations.ReplaceTable(initTableTransactionEvent);
                        break;
                    
                    case InitPartitionsTransactionEvent initPartitionsTransactionEvent:
                        _nodesSyncOperations.ReplacePartitions(initPartitionsTransactionEvent);
                        break;

                    case UpdateRowsTransactionEvent updateRowsTransactionEvent:
                        _nodesSyncOperations.UpdateRows(updateRowsTransactionEvent);
                        break;

                    case DeleteRowsTransactionEvent deleteRowsTransactionEvent:
                        _nodesSyncOperations.DeleteRows(deleteRowsTransactionEvent);
                        break;
                }
                
            }


        }


        private bool _working;
        private Task _task;


        public void Start()
        {
            _working = true;
            _task = ThreadLoopAsync();

        }
    }
}