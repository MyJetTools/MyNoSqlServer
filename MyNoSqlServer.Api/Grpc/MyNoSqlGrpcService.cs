using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Grpc;

namespace MyNoSqlServer.Api.Grpc
{
    public class MyNoSqlGrpcService : IMyNoSqlWriterGrpcService, IMyNoSqlWriterGrpcServiceLegacy
    {
        public ValueTask CreateTableIfNotExistsAsync(CreateTableIfNotExistsGrpcRequest request)
        {
            ServiceLocator.DbInstance.CreateTableIfNotExists(request.TableName, request.PersistTable);
            return new ValueTask();
        }

        public ValueTask SetTableAttributesAsync(SetTableAttributesGrpcRequest request)
        {
            if (request.MaxPartitionsAmount != null)
                ServiceLocator.DbInstance.SetMaxPartitionsAmount(request.TableName, request.MaxPartitionsAmount.Value);

            return new ValueTask();
        }

        public IAsyncEnumerable<TableEntityTransportGrpcContract> GetRowsAsync(GetEntitiesGrpcRequest request)
        {
            var table = ServiceLocator.DbInstance.GetTable(request.TableName);

            IEnumerable<TableEntityTransportGrpcContract> result;

            if (request.PartitionKey == null && request.RowKey == null)
            {
                result = table.GetAllRecords(request.Limit, request.Skip).Select(itm => itm.ToTransportContract());
            }
            else
            if (request.PartitionKey != null && request.RowKey == null)
            {
                result = table.GetRecords(request.PartitionKey, request.Limit, request.Skip).Select(itm => itm.ToTransportContract());
            }
            else
            if (request.PartitionKey == null && request.RowKey != null)
            {
                result = table.GetRecordsByRowKey(request.RowKey, request.Limit, request.Skip).Select(itm => itm.ToTransportContract());
            }
            else
            {
                var dbRow = table.GetEntity(request.PartitionKey, request.RowKey);
                return new[]{dbRow.ToTransportContract()}.ToAsyncEnumerable();
            }

            return result.ToAsyncEnumerable();
        }

        public ValueTask<GetDbRowGrpcResponse> GetRowAsync(GetEntityGrpcRequest request)
        {   
            var table = ServiceLocator.DbInstance.TryGetTable(request.TableName);

            if (table == null)
            {
                var resultTableNotFound = new GetDbRowGrpcResponse
                {
                    Response = MyNoSqlResponse.TableNotFound
                };
                return new ValueTask<GetDbRowGrpcResponse>(resultTableNotFound);
            }
            
            var dbRow = table.GetEntity(request.PartitionKey, request.RowKey);

            var result = dbRow == null
                ? new GetDbRowGrpcResponse
                {
                    Response = MyNoSqlResponse.DbRowNotFound
                }
                : new GetDbRowGrpcResponse
                {
                    Response = MyNoSqlResponse.Ok,
                    Entity = dbRow.ToTransportContract()
                };


            return new ValueTask<GetDbRowGrpcResponse>(result);
        }

        public ValueTask<TransactionGrpcResponse> PostTransactionActionsAsync(TransactionPayloadGrpcRequest request)
        {
        
            ServiceLocator.Logger.WriteInfo("PostTransactionActionsAsync", Newtonsoft.Json.JsonConvert.SerializeObject(request));

            try
            {
                var transactionSeq = request.TransactionId == null
                    ? ServiceLocator.PostTransactionsList.StartTransaction()
                    : ServiceLocator.PostTransactionsList.TryGet(request.TransactionId);

                if (request.Actions != null)
                {
                    var transactions = request.ReadGrpcTransactions().ToList();

                    var tables = new Dictionary<string, DbTable>();


                    foreach (var transaction in transactions)
                    { 
                        if (tables.ContainsKey(transaction.TableName))
                            continue;

                        var table = ServiceLocator.DbInstance.GetTable(transaction.TableName);
                        
                        tables.Add(transaction.TableName, table);
                    }
            
                    transactionSeq.PostTransactions(tables.Values, transactions);
                }

                if (request.Commit)
                {
                    ServiceLocator.DbOperations.ApplyTransactions(transactionSeq.Tables,
                        transactionSeq.GetTransactionsToExecute());
                }

                var result = new TransactionGrpcResponse
                {
                    Id = transactionSeq?.Id
                };

                return new ValueTask<TransactionGrpcResponse>(result);
            }
            catch (Exception e)
            {
                ServiceLocator.Logger.WriteError("GRPC.PostTransactionActionsAsync", e);
                throw;
            }

        }

        public ValueTask CancelTransactionAsync(CancelTransactionGrpcRequest request)
        {
            ServiceLocator.PostTransactionsList.TryDelete(request.Id);
            return new ValueTask();
        }
    }
}