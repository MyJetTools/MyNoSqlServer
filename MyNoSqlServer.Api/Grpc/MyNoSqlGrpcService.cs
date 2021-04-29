using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MyNoSqlServer.Grpc;

namespace MyNoSqlServer.Api.Grpc
{
    public class MyNoSqlGrpcService : IMyNoSqlTransportGrpcService
    {
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
            var transaction = request.TransactionId == null
                ? ServiceLocator.PostTransactionsList.StartTransaction()
                : ServiceLocator.PostTransactionsList.TryGet(request.TransactionId);


            var result = new TransactionGrpcResponse
            {
                Id = transaction?.Id
            };

            return new ValueTask<TransactionGrpcResponse>(result);
        }

        public ValueTask CancelTransactionAsync(CancelTransactionGrpcRequest request)
        {
            ServiceLocator.PostTransactionsList.TryDelete(request.Id);
            return new ValueTask();
        }
    }
}