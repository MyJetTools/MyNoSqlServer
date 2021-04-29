using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.Grpc
{
    public enum TransactionType
    {
        CleanTable,
        DeletePartitions,
        DeleteRows,
        InsertOrReplacePartitions
    }

    public enum TransactionOperationResult
    {
        Ok,
        TransactionNotFound,
        TableNotFound
    }

    [DataContract]
    public class CleanTableTransactionActionGrpcModel : ICleanTableTransactionAction
    {

        [DataMember(Order = 1)] public string TableName { get; set; }

        public static CleanTableTransactionActionGrpcModel Create(ICleanTableTransactionAction src)
        {
            return new CleanTableTransactionActionGrpcModel
            {
                TableName = src.TableName
            };
        }
    }

    [DataContract]
    public class DeletePartitionsTransactionActionGrpcModel : IDeletePartitionsTransactionAction
    {

        [DataMember(Order = 1)] public string TableName { get; set; }

        [DataMember(Order = 2)] public string[] PartitionKeys { get; set; }

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
        [DataMember(Order = 1)] public string TableName { get; set; }

        [DataMember(Order = 2)] public string PartitionKey { get; set; }

        [DataMember(Order = 3)] public string[] RowKeys { get; set; }

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

        [DataMember(Order = 1)] public string TableName { get; set; }

        [DataMember(Order = 2)] public List<TableEntityTransportGrpcContract> Entities { get; set; }

        IEnumerable<(DbEntityType Type, byte[] Payload)> IInsertOrReplaceEntitiesTransactionAction.Entities =>
            Entities.Select(itm => (itm.ContentType.ToDbEntityContentType(), itm.Content));


        public static InsertOrReplaceEntitiesTransactionActionGrpcModel Create(
            IInsertOrReplaceEntitiesTransactionAction src)
        {
            return new InsertOrReplaceEntitiesTransactionActionGrpcModel
            {
                TableName = src.TableName,
                Entities = src.Entities
                    .Select(itm => new TableEntityTransportGrpcContract
                    {
                        ContentType = itm.Type.ToGrpcEntityContentType(),
                        Content = itm.Payload
                        
                    }).ToList()
            };
        }


    }


    public static class GrpcTransactionsSerializer
    {

        internal struct TypeAndPayload
        {
            public TransactionType Type { get; set; }

            public IDbTransactionAction Contract { get; set; }
        }

        public static (TransactionType type, byte[] payload) SerializeTransactionsToGrpc(
            this IDbTransactionAction transactionAction)
        {
            TypeAndPayload result = transactionAction switch
            {
                ICleanTableTransactionAction cleanTableAction =>
                    new TypeAndPayload
                    {
                        Type = TransactionType.CleanTable,
                        Contract = cleanTableAction as CleanTableTransactionActionGrpcModel ??
                                   CleanTableTransactionActionGrpcModel.Create(cleanTableAction)
                    },

                IDeletePartitionsTransactionAction deletePartitionsAction =>
                    new TypeAndPayload
                    {
                        Type = TransactionType.DeletePartitions,
                        Contract = deletePartitionsAction as DeletePartitionsTransactionActionGrpcModel ??
                                   DeletePartitionsTransactionActionGrpcModel.Create(deletePartitionsAction)
                    },


                IDeleteRowsTransactionAction deleteRowsAction =>
                    new TypeAndPayload
                    {
                        Type = TransactionType.DeleteRows,
                        Contract = deleteRowsAction as DeleteRowsTransactionActionGrpcModel ??
                                   DeleteRowsTransactionActionGrpcModel.Create(deleteRowsAction)
                    },


                IInsertOrReplaceEntitiesTransactionAction insertOrReplaceAction =>
                    new TypeAndPayload
                    {
                        Type = TransactionType.InsertOrReplacePartitions,
                        Contract = insertOrReplaceAction as InsertOrReplaceEntitiesTransactionActionGrpcModel ??
                                   InsertOrReplaceEntitiesTransactionActionGrpcModel.Create(insertOrReplaceAction)
                    },


                _ => new TypeAndPayload
                {
                    Type = default,
                    Contract = null
                }
            };

            if (result.Contract != null)
            {
                var mem = new MemoryStream();
                ProtoBuf.Serializer.Serialize(mem, result.Contract);
                return (result.Type, mem.ToArray());
            }

            throw new Exception("Unsupported IDbTransactionAction Type: " + transactionAction.GetType());
        }




        public static IEnumerable<IDbTransactionAction> ReadGrpcTransactions(this TransactionPayloadGrpcRequest model)
        {

            foreach (var grpcModel in model.Actions)
            {

                switch (grpcModel.TransactionType)
                {
                    case TransactionType.CleanTable:
                        yield return ProtoBuf.Serializer.Deserialize<CleanTableTransactionActionGrpcModel>(
                            grpcModel.Payload.AsSpan());
                        break;

                    case TransactionType.DeletePartitions:
                        yield return ProtoBuf.Serializer.Deserialize<DeletePartitionsTransactionActionGrpcModel>(
                            grpcModel.Payload.AsSpan());
                        break;

                    case TransactionType.DeleteRows:
                        yield return ProtoBuf.Serializer.Deserialize<DeleteRowsTransactionActionGrpcModel>(
                            grpcModel.Payload.AsSpan());
                        break;

                    case TransactionType.InsertOrReplacePartitions:
                        yield return ProtoBuf.Serializer.Deserialize<InsertOrReplaceEntitiesTransactionActionGrpcModel>(
                            grpcModel.Payload.AsSpan());
                        break;
                }

            }
        }

    }

}