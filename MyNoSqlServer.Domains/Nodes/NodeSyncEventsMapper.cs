using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.DataCompression;
using MyNoSqlServer.Domains.Db.Rows;
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
        

        public static SyncGrpcResponse ToSyncGrpcResponse(this ITransactionEvent @event, bool compress)
        {
            var result = new SyncGrpcResponse
            {
                TableName = @event.TableName,
                Headers = @event.ToHeaders()
            };
            
            switch(@event) 
            {
                case FirstInitTableEvent initTableEvent:
                    result.TableAttributes = new TableAttributeGrpcModel
                    {
                        Persist = initTableEvent.Table.Persist,
                        MaxPartitionsAmount = initTableEvent.Table.MaxPartitionsAmount,
                        TableName = initTableEvent.TableName,
                    };

                    result.InitTableData = compress
                        ? MyNoSqlServerDataCompression.ZipPayload(initTableEvent.TableData)
                        : initTableEvent.TableData;
                    return result;
                
                case InitTableTransactionEvent initTableTransactionEvent:
                    result.InitTableData = compress
                        ? MyNoSqlServerDataCompression.ZipPayload(initTableTransactionEvent.Snapshot.AsByteArray())
                        : initTableTransactionEvent.Snapshot.AsByteArray();
                    return result;
                
               case InitPartitionsTransactionEvent initPartitionsTransactionEvent:
                   result.InitPartitionData = compress
                       ? MyNoSqlServerDataCompression.ZipPayload(initPartitionsTransactionEvent.Partitions.AsByteArray())
                       : initPartitionsTransactionEvent.Partitions.AsByteArray();
                   return result;
               
               case SyncTableAttributes syncTableAttributes:
                   result.TableAttributes = new TableAttributeGrpcModel
                   {
                       Persist = syncTableAttributes.PersistTable,
                       MaxPartitionsAmount = syncTableAttributes.MaxPartitionsAmount,
                       TableName = syncTableAttributes.TableName,
                   };
                   return result;
               
               case UpdateRowsTransactionEvent updateRowsTransactionEvent:
                   result.UpdateRowsData = compress
                       ? MyNoSqlServerDataCompression.ZipPayload(updateRowsTransactionEvent.Rows.ToJsonArray().AsArray())
                       : updateRowsTransactionEvent.Rows.ToJsonArray().AsArray();
                   return result; 
               
                case DeleteRowsTransactionEvent deleteRowsTransactionEvent:
                    result.DeleteRows = GetDbRowsToDelete(deleteRowsTransactionEvent);
                    return result; 
                
            }

            throw new Exception($"Unsupported transaction event {@event.GetType()}");
        }

        private static Dictionary<string, string[]> GetDbRowsToDelete(this DeleteRowsTransactionEvent deleteRowsTransactionEvent)
        {
            return deleteRowsTransactionEvent.Rows.GroupBy(itm => itm.PartitionKey).ToDictionary(
                pk => pk.Key,
                rk => rk.Select(itm => itm.RowKey).ToArray());
        }  
        
    }
    
    

}