using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.Grpc
{
    public enum TransactionType
    {
        CleanTable, CleanPartition, CleanRows, InsertOrReplacePartitions
    }

    public enum TransactionOperationResult
    {
        Ok, TransactionNotFound, TableNotFound
    }
    
    [DataContract]
    public class CleanTableTransactionActionGrpcModel : ICleanTableTransactionAction{
    
        [DataMember(Order = 1)]
        public string TableName { get; set; }

        public static CleanTableTransactionActionGrpcModel Create(ICleanTableTransactionAction src)
        {
            return new CleanTableTransactionActionGrpcModel
            {
                TableName = src.TableName
            };
        }
    }
    
    [DataContract]
    public class DeletePartitionsTransactionActionGrpcModel : IDeletePartitionsTransactionAction{
    
        [DataMember(Order = 1)]
        public string TableName { get; set; }
        
        [DataMember(Order = 2)]
        public string[] PartitionKeys { get; set; }
        
        public static DeletePartitionsTransactionActionGrpcModel Create(IDeletePartitionsTransactionAction src)
        {
            return new DeletePartitionsTransactionActionGrpcModel
            {
                TableName = src.TableName,
                PartitionKeys = src.PartitionKeys
            };
        }
        
    }

    [DataContract]
    public class DeleteRowsTransactionActionGrpcModel : IDeleteRowsTransactionAction
    {
        [DataMember(Order = 1)]
        public string TableName { get; set; }
        
        [DataMember(Order = 2)]
        public string PartitionKey { get; set; }
        
        [DataMember(Order = 3)]
        public string[] RowKeys { get; set; }
        
        public static DeleteRowsTransactionActionGrpcModel Create(IDeleteRowsTransactionAction src)
        {
            return new DeleteRowsTransactionActionGrpcModel
            {
                TableName = src.TableName,
                PartitionKey = src.PartitionKey,
                RowKeys = src.RowKeys ?? Array.Empty<string>()
            };
        }
    }
    
    [DataContract]
    public class InsertOrReplaceEntitiesTransactionActionGrpcModel : IInsertOrReplaceEntitiesTransactionAction
    {
        
        [DataMember(Order = 1)]
        public string TableName { get; set; }
        
        [DataMember(Order = 2)]
        public List<byte[]> Entities { get; set; }
        
        public static InsertOrReplaceEntitiesTransactionActionGrpcModel Create(IInsertOrReplaceEntitiesTransactionAction src)
        {
            return new InsertOrReplaceEntitiesTransactionActionGrpcModel
            {
                TableName = src.TableName,
                Entities =  src.Entities,
            };
        }
    }


    public static class GrpcTransactionsDeserializer
    {

        public static IEnumerable<byte[]> SerializeTransactions(IEnumerable<IDbTransactionAction> transactionActions)
        {
            foreach (var action in transactionActions)
            {
                IDbTransactionAction contract = action switch
                {
                    ICleanTableTransactionAction cleanTableAction =>
                        cleanTableAction as CleanTableTransactionActionGrpcModel ??
                        CleanTableTransactionActionGrpcModel.Create(cleanTableAction),
                    
                    IDeletePartitionsTransactionAction deletePartitionsAction =>
                        deletePartitionsAction as DeletePartitionsTransactionActionGrpcModel ??
                        DeletePartitionsTransactionActionGrpcModel.Create(deletePartitionsAction),
                    
                    IDeleteRowsTransactionAction deleteRowsAction =>
                        deleteRowsAction as DeleteRowsTransactionActionGrpcModel ??
                        DeleteRowsTransactionActionGrpcModel.Create(deleteRowsAction),
                    
                    IInsertOrReplaceEntitiesTransactionAction insertOrReplaceAction =>
                        insertOrReplaceAction as InsertOrReplaceEntitiesTransactionActionGrpcModel ??
                        InsertOrReplaceEntitiesTransactionActionGrpcModel.Create(insertOrReplaceAction),
                    
                    _ => null
                };

                if (contract != null)
                {
                    var mem = new MemoryStream();
                    ProtoBuf.Serializer.Serialize(mem,contract);
                    yield return mem.ToArray();
                }
                
            }
            
            
            
        }

        public static IEnumerable<IDbTransactionAction> ReadGrpcTransactions(this TransactionPayloadGrpcRequest model)
        {

            foreach (var grpcModel in model.Steps)
            {

                switch (grpcModel.TransactionType)
                {
                    case TransactionType.CleanTable:
                        yield return ProtoBuf.Serializer.Deserialize<CleanTableTransactionActionGrpcModel>(grpcModel.Payload.AsSpan());
                        break;

                    case TransactionType.CleanRows:
                        yield return ProtoBuf.Serializer.Deserialize<DeletePartitionsTransactionActionGrpcModel>(grpcModel.Payload.AsSpan());
                        break;
                        
                    case TransactionType.CleanPartition:
                        yield return ProtoBuf.Serializer.Deserialize<DeleteRowsTransactionActionGrpcModel>(grpcModel.Payload.AsSpan());
                        break;

                    case TransactionType.InsertOrReplacePartitions:
                        yield return ProtoBuf.Serializer.Deserialize<InsertOrReplaceEntitiesTransactionActionGrpcModel>(grpcModel.Payload.AsSpan());
                        break;
                }
                
            }
            
        }
        
    }
    
}