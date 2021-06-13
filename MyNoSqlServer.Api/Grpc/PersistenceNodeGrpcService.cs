using System.Collections.Generic;
using System.Threading.Tasks;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Api.Nodes;
using MyNoSqlServer.Common;
using MyNoSqlServer.DataCompression;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.TransactionEvents;
using MyNoSqlServer.NodePersistence;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.Api.Grpc
{
    public class PersistenceNodeGrpcService : IMyNoSqlServerNodePersistenceGrpcService
    {
        
        private static TransactionEventAttributes GetGrpcRequestAttributes(string location)
        {
            return new TransactionEventAttributes
            {
                Location = location,
                SynchronizationPeriod = DataSynchronizationPeriod.Sec1
            };
        }
        
        public ValueTask PingAsync(PingGrpcRequest request)
        {
            NodesPool.GetOrCreateNode(request.Location);
            return new ValueTask();
        }



        public async ValueTask SaveTableSnapshotAsync(IAsyncEnumerable<PayloadWrapperGrpcModel> payloads)
        {
            var model = await payloads.MergePayloadAndDeserialize<SaveTableSnapshotGrpcModel>(false);
            ServiceLocator.DbOperations.ReplaceTable(model.TableName, model.PersistTable, model.Content.AsMyMemory(), 
                GetGrpcRequestAttributes(model.Location));
            
        }

        public async ValueTask SaveTableSnapshotCompressedAsync(IAsyncEnumerable<PayloadWrapperGrpcModel> payloads)
        {
            var model = await payloads.MergePayloadAndDeserialize<SaveTableSnapshotGrpcModel>(true);
            ServiceLocator.DbOperations.ReplaceTable(model.TableName, model.PersistTable, model.Content.AsMyMemory(), 
                GetGrpcRequestAttributes(model.Location));
        }

        public async ValueTask SavePartitionSnapshotAsync(IAsyncEnumerable<PayloadWrapperGrpcModel> payloads)
        {
            var model = await payloads.MergePayloadAndDeserialize<SavePartitionSnapshotGrpcModel>(false);
            ServiceLocator.DbOperations.ReplaceTable(model.TableName, model.PersistTable, model.Content.AsMyMemory(), 
                GetGrpcRequestAttributes(model.Location));
        }

        public async ValueTask SavePartitionCompressedSnapshotAsync(IAsyncEnumerable<PayloadWrapperGrpcModel> payloads)
        {
            var model = await payloads.MergePayloadAndDeserialize<SavePartitionSnapshotGrpcModel>(true);
            ServiceLocator.DbOperations.ReplaceTable(model.TableName, model.PersistTable, model.Content.AsMyMemory(), 
                GetGrpcRequestAttributes(model.Location));
        }

        public ValueTask DeleteTablePartitionsAsync(DeleteTablePartitionGrpcRequest request)
        {
            ServiceLocator.DbOperations.DeletePartitions(request.TableName, request.PartitionKeys, 
                GetGrpcRequestAttributes(request.Location));
            return new ValueTask();
        }

        public ValueTask SetTableAttributesAsync(SetTableAttributesGrpcRequest request)
        {
            ServiceLocator.DbOperations.SetTableAttributes(request.TableName, request.Persist, request.MaxPartitionsAmount, 
                GetGrpcRequestAttributes(request.Location));

            return new ValueTask();
        }

        public async IAsyncEnumerable<GetTableGrpcResponse> GetTablesAsync()
        {
            var tables = ServiceLocator.DbInstance.GetTables();

            foreach (var dbTable in tables)
            {
                yield return new GetTableGrpcResponse
                {
                    TableName = dbTable.Name,
                    Persist = dbTable.Persist,
                    MaxPartitionsAmount = dbTable.MaxPartitionsAmount
                };
            }
        }

        public async IAsyncEnumerable<TablePartitionGrpcModel> DownloadTableAsync(DownloadTableGrpcRequest request)
        {
            
            var tables = ServiceLocator.DbInstance.GetTable(request.TableName);
            
            
            var result = tables.GetReadAccess(readAccess =>
            {
                var snapshots = new Dictionary<string, byte[]>();
                foreach (var dbPartition in readAccess.GetAllPartitions())
                {
                    var rows = dbPartition.GetAllRows().ToJsonArray().AsArray();
                    
                    snapshots.Add(dbPartition.PartitionKey, rows);
                }

                return snapshots;
            });


            foreach (var (partitionKey, snapshot) in result)
            {
                yield return new TablePartitionGrpcModel
                {
                    PartitionKey = partitionKey,
                    Content = snapshot
                };
            }
            
        }

        public async IAsyncEnumerable<TablePartitionGrpcModel> DownloadTableCompressedAsync(DownloadTableGrpcRequest request)
        {
             
            var tables = ServiceLocator.DbInstance.GetTable(request.TableName);
            
            
            var result = tables.GetReadAccess(readAccess =>
            {
                var snapshots = new Dictionary<string, byte[]>();
                foreach (var dbPartition in readAccess.GetAllPartitions())
                {
                    var rows = dbPartition.GetAllRows().ToJsonArray().AsArray();
                    
                    snapshots.Add(dbPartition.PartitionKey, rows);
                }

                return snapshots;
            });


            foreach (var (partitionKey, snapshot) in result)
            {
                yield return new TablePartitionGrpcModel
                {
                    PartitionKey = partitionKey,
                    Content =  MyNoSqlServerDataCompression.ZipPayload(snapshot)
                };
            }
        }
    }
}