using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Grpc;

namespace MyNoSqlServer.GrpcDataWriter
{
    public class MyNoSqlTransaction
    {
        private static readonly System.Diagnostics.ActivitySource Source = new("MyNoSql.TransactionsBuilder");

        private readonly IMyNoSqlTransportGrpcService _grpcService;
        private readonly Func<Type, string> _getTableName;

        private string _transactionId;

        private readonly List<TransactionActionGrpcModel> _transactionActionsGrpcModels = new ();
        //ToDo - we are going to use this parameter to calculate payload and separate operations
        private int _payloadSize = 0;

        public MyNoSqlTransaction(IMyNoSqlTransportGrpcService grpcService, Func<Type, string> getTableName)
        {
            _grpcService = grpcService;
            _getTableName = getTableName;
        }

        private void InsertTransactionAction(TransactionActionGrpcModel model)
        {
            _transactionActionsGrpcModels.Add(model);
            _payloadSize += model.Payload.Length;
        }

        private void InsertTransactionAction(IDbTransactionAction transaction)
        {

            var (transactionType, payload) = transaction.SerializeTransactionsToGrpc();


            var stepGrpcModel = new TransactionActionGrpcModel
            {
                TransactionType = transactionType,
                Payload = payload
            };

            InsertTransactionAction(stepGrpcModel);
        }

        public void CleanTable(string tableName)
        {
            var payloadModel = new CleanTableTransactionActionGrpcModel
            {
                TableName = tableName
            };
            
            InsertTransactionAction(payloadModel);
        }
        
        public void DeletePartitions(string tableName, string[] partitionKeys)
        {
            var payloadModel = new DeletePartitionsTransactionActionGrpcModel
            {
                TableName = tableName,
                PartitionKeys = partitionKeys
            };
            
            InsertTransactionAction(payloadModel);
        }
        
        public void DeleteRows(string tableName, string partitionKey, params string[] rowKeys)
        {
            var payloadModel = new DeleteRowsTransactionActionGrpcModel
            {
                TableName = tableName,
                PartitionKey = partitionKey,
                RowKeys = rowKeys
            };
            InsertTransactionAction(payloadModel);
        }

        public void InsertOrReplaceEntity<T>(T entity) where T:IMyNoSqlDbEntity, new()
        {
            InsertOrReplaceEntities(new[] {entity});
        }
        
        public void InsertOrReplaceEntities<T>(IEnumerable<T> entities) where T:IMyNoSqlDbEntity, new()
        {
            InsertOrReplaceEntitiesTransactionActionGrpcModel payloadModel = null;
            
            foreach (var entity in entities)
            {
                var tableName = _getTableName(entity.GetType());
                
                payloadModel ??= new InsertOrReplaceEntitiesTransactionActionGrpcModel
                {
                    TableName = tableName,
                    Entities = new List<TableEntityTransportGrpcContract>()
                };


                if (payloadModel.TableName != tableName)
                {
                    if (payloadModel.Entities.Count >0)
                        InsertTransactionAction(payloadModel);

                    payloadModel = new InsertOrReplaceEntitiesTransactionActionGrpcModel
                    {
                        TableName = tableName,
                        Entities = new List<TableEntityTransportGrpcContract>()
                    };

                    continue;
                }
 
                payloadModel.Entities.Add(entity.SerializeEntity());
            }
            
            if (payloadModel != null)
                InsertTransactionAction(payloadModel);
        }

        public async ValueTask SyncOperationsWithServerAsync()
        {
            var result = await _grpcService.PostTransactionActionsAsync(new TransactionPayloadGrpcRequest
            {
                TransactionId = _transactionId,
                Actions = _transactionActionsGrpcModels.ToArray(),
                Commit = false
            });

            _transactionId ??= result.Id;
        }


        public async ValueTask CommitAsync()
        {
            using var activity = Source.StartActivity("Commit.GrpcCall");

            await _grpcService.PostTransactionActionsAsync(new TransactionPayloadGrpcRequest
            {
                TransactionId = _transactionId,
                Actions = _transactionActionsGrpcModels.ToArray(),
                Commit = true
            });
        }
        
    }
}