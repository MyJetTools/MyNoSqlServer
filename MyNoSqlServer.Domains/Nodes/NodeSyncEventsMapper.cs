using System;
using System.Linq;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.TransactionEvents;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.Domains.Nodes
{
    public static class NodeSyncEventsMapper
    {

        private static SyncGrpcHeader[] ToHeaders(this ITransactionEvent @event)
        {
            if (@event.Attributes.Headers == null)
                return Array.Empty<SyncGrpcHeader>();
            
            if (@event.Attributes.Headers.Count == 0)
                return Array.Empty<SyncGrpcHeader>();

            return @event.Attributes.Headers.Select(itm => new SyncGrpcHeader
            {
                Key = itm.Key,
                Value = itm.Value
            }).ToArray();
        }
        

        public static SyncGrpcResponse ToSyncGrpcResponse(this ITransactionEvent @event)
        {
            var result = new SyncGrpcResponse
            {
                TableName = @event.TableName,
                Headers = @event.ToHeaders()
            };
            
            switch(@event) 
            {
                case InitTableEvent initTableEvent:
                    result.InitTable = initTableEvent.Table.GetSnapshotAsArray();
                    return result;
                
            }

            throw new Exception($"Unsupported transaction event {@event.GetType()}");
        }
        
        
        
    }
}