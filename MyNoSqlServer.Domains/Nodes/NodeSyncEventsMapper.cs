using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Common;
using MyNoSqlServer.DataCompression;
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



        public static IEnumerable<ITransactionEvent> ToTransactionEvents(this SyncGrpcResponse syncGrpcResponse, 
            Func<TransactionEventAttributes> getTransactionEventAttributes)
        {
            if (syncGrpcResponse.TableName == null)
                yield break;


            TransactionEventAttributes transactionEventAttributes = null;
            
            if (syncGrpcResponse.TableAttributes != null)
            {
                transactionEventAttributes = getTransactionEventAttributes();
                
                yield return new UpdateTableAttributesTransactionEvent
                {
                    PersistTable = syncGrpcResponse.TableAttributes.Persist,
                    MaxPartitionsAmount = syncGrpcResponse.TableAttributes.MaxPartitionsAmount,
                    Attributes = transactionEventAttributes,
                    TableName = syncGrpcResponse.TableName
                };
            }
            
            if (syncGrpcResponse.InitTableData != null)
            {
                transactionEventAttributes ??= getTransactionEventAttributes();

                var initTableTransactionEvent = new InitTableTransactionEvent
                {
                    TableName = syncGrpcResponse.TableName,
                    Attributes = transactionEventAttributes,
                    Snapshot = syncGrpcResponse.InitTableData.DeserializeProtobufPartitionsData()
                };
                yield return initTableTransactionEvent;
            }
            
            if (syncGrpcResponse.InitPartitionData != null)
            {
                transactionEventAttributes ??= getTransactionEventAttributes();

                var initTableTransactionEvent = new InitPartitionsTransactionEvent
                {
                    TableName = syncGrpcResponse.TableName,
                    Attributes = transactionEventAttributes,
                    Partitions = syncGrpcResponse.InitPartitionData.DeserializeProtobufPartitionsData()
                };
                yield return initTableTransactionEvent;
            }

            if (syncGrpcResponse.UpdateRowsData != null)
            {
                transactionEventAttributes ??= getTransactionEventAttributes();

                var initTableTransactionEvent = new UpdateRowsTransactionEvent
                {
                    TableName = syncGrpcResponse.TableName,
                    Attributes = transactionEventAttributes,
                    RowsByPartition = syncGrpcResponse.UpdateRowsData.DeserializeProtobufPartitionsData()
                };
                yield return initTableTransactionEvent;
            }
            
            if (syncGrpcResponse.DeleteRows != null)
            {
                transactionEventAttributes ??= getTransactionEventAttributes();

                var initTableTransactionEvent = new DeleteRowsTransactionEvent
                {
                    TableName = syncGrpcResponse.TableName,
                    Attributes = transactionEventAttributes,
                    Rows = syncGrpcResponse.DeleteRows.ToDictionary(
                        itm => itm.Key, 
                        itm => itm.Value.AsReadOnlyList())
                };
                
                yield return initTableTransactionEvent;
            }
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
                    result.TableAttributes = new TableAttributesGrpcData
                    {
                        Persist = initTableEvent.Table.Persist,
                        MaxPartitionsAmount = initTableEvent.Table.MaxPartitionsAmount,
                    };

                    result.InitTableData = compress
                        ? MyNoSqlServerDataCompression.ZipPayload(initTableEvent.TableData.SerializeProtobufPartitionsData())
                        : initTableEvent.TableData.SerializeProtobufPartitionsData();
                    return result;
                
                case InitTableTransactionEvent initTableTransactionEvent:
                    result.InitTableData = compress
                        ? MyNoSqlServerDataCompression.ZipPayload(initTableTransactionEvent.Snapshot.SerializeProtobufPartitionsData())
                        : initTableTransactionEvent.Snapshot.SerializeProtobufPartitionsData();
                    return result;
                
               case InitPartitionsTransactionEvent initPartitionsTransactionEvent:
                   result.InitPartitionData = compress
                       ? MyNoSqlServerDataCompression.ZipPayload(initPartitionsTransactionEvent.Partitions.SerializeProtobufPartitionsData())
                       : initPartitionsTransactionEvent.Partitions.SerializeProtobufPartitionsData();
                   return result;
               
               case UpdateTableAttributesTransactionEvent syncTableAttributes:
                   result.TableAttributes = new TableAttributesGrpcData
                   {
                       Persist = syncTableAttributes.PersistTable,
                       MaxPartitionsAmount = syncTableAttributes.MaxPartitionsAmount
                   };
                   return result;
               
               case UpdateRowsTransactionEvent updateRowsTransactionEvent:
                   result.UpdateRowsData = compress
                       ? MyNoSqlServerDataCompression.ZipPayload(updateRowsTransactionEvent.RowsByPartition.SerializeProtobufPartitionsData())
                       : updateRowsTransactionEvent.RowsByPartition.SerializeProtobufPartitionsData();
                   return result; 
               
                case DeleteRowsTransactionEvent deleteRowsTransactionEvent:
                    result.DeleteRows = deleteRowsTransactionEvent.Rows.ToDictionary( 
                        itm => itm.Key, 
                        itm => itm.Value.ToArray());
                    return result; 
                
            }

            throw new Exception($"Unsupported transaction event {@event.GetType()}");
        }



        
    }
    
    

}