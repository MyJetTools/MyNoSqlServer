using System;
using MyNoSqlServer.Domains.TransactionEvents;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.Domains.Nodes
{
    public class SyncTransactionHandler
    {
        private readonly NodesSyncOperations _nodesSyncOperations;

        public SyncTransactionHandler(NodesSyncOperations nodesSyncOperations)
        {
            _nodesSyncOperations = nodesSyncOperations;
        }
        
        public void HandleTransaction(SyncTransactionGrpcModel syncTransactionGrpcModel, Func<TransactionEventAttributes> getTransactionEventAttributes)
        {
            var transactionEvents = syncTransactionGrpcModel.ToTransactionEvents(getTransactionEventAttributes);

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
    }
}