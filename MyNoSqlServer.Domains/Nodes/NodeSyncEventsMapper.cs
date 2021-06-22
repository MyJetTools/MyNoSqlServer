using System;
using System.Collections.Generic;
using System.IO;
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



        public static IEnumerable<ITransactionEvent> ToTransactionEvents(this SyncTransactionGrpcModel syncGrpcResponse, 
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
            
            if (syncGrpcResponse.InitPartitionsData != null)
            {
                transactionEventAttributes ??= getTransactionEventAttributes();

                var initTableTransactionEvent = new InitPartitionsTransactionEvent
                {
                    TableName = syncGrpcResponse.TableName,
                    Attributes = transactionEventAttributes,
                    Partitions = syncGrpcResponse.InitPartitionsData.DeserializeProtobufPartitionsData()
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
        

        public static SyncTransactionGrpcModel ToSyncTransactionGrpcModel(this ITransactionEvent @event)
        {
            var result = new SyncTransactionGrpcModel
            {
                TableName = @event.TableName,
                Headers = @event.ToHeaders(),
                Locations = @event.Attributes.Locations
            };
            
            switch(@event) 
            {
                case FirstInitTableEvent initTableEvent:
                    result.TableAttributes = new TableAttributesGrpcData
                    {
                        Persist = initTableEvent.Table.Persist,
                        MaxPartitionsAmount = initTableEvent.Table.MaxPartitionsAmount,
                    };

                    result.InitPacket = true;
                    result.InitTableData = initTableEvent.TableData.ToPartitionDataGrpcModel().ToList();
                    return result;
                
                case InitTableTransactionEvent initTableTransactionEvent:
                    result.InitTableData = initTableTransactionEvent.Snapshot.ToPartitionDataGrpcModel().ToList();
                    return result;
                
               case InitPartitionsTransactionEvent initPartitionsTransactionEvent:
                   result.InitTableData = initPartitionsTransactionEvent.Partitions.ToPartitionDataGrpcModel().ToList();
                   return result;
               
               case UpdateTableAttributesTransactionEvent syncTableAttributes:
                   result.TableAttributes = new TableAttributesGrpcData
                   {
                       Persist = syncTableAttributes.PersistTable,
                       MaxPartitionsAmount = syncTableAttributes.MaxPartitionsAmount
                   };
                   return result;
               
               case UpdateRowsTransactionEvent updateRowsTransactionEvent:
                   result.InitTableData = updateRowsTransactionEvent.RowsByPartition.ToPartitionDataGrpcModel().ToList();
                   return result; 
               
                case DeleteRowsTransactionEvent deleteRowsTransactionEvent:
                    result.DeleteRows = deleteRowsTransactionEvent.Rows.ToDictionary( 
                        itm => itm.Key, 
                        itm => itm.Value.ToArray());
                    return result; 
                
            }

            throw new Exception($"Unsupported transaction event {@event.GetType()}");
        }


        public static PayloadWrapperGrpcModel ToProtobufWrapper(this object src, bool compressed)
        {
            var memoryStream = new MemoryStream();
            ProtoBuf.Serializer.Serialize(memoryStream, src);

            var result = memoryStream.ToArray();

            return new PayloadWrapperGrpcModel
            {
                Payload = compressed ? MyNoSqlServerDataCompression.ZipPayload(result) : result
            };
        }


        public static T Deserialize<T>(this PayloadWrapperGrpcModel src, bool compressed)
        {
            if (compressed)
            {
                var uncompressed = MyNoSqlServerDataCompression.UnZipPayload(src.Payload);
                return ProtoBuf.Serializer.Deserialize<T>(uncompressed.AsMemory());
            }
            
            return ProtoBuf.Serializer.Deserialize<T>(src.Payload.AsMemory());
        }



        
    }
    
    

}