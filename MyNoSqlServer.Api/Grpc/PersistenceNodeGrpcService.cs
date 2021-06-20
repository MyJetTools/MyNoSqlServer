using System.Collections.Generic;
using System.Linq;
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
        private static readonly IReadOnlyDictionary<string, string> EmptyDictionary = new Dictionary<string, string>();

        private static IReadOnlyDictionary<string, string> ToDomainHeaders(IReadOnlyList<SyncGrpcHeader> headers)
        {
            if (headers == null)
                return EmptyDictionary;

            if (headers.Count == 0)
                return EmptyDictionary;

            return headers.ToDictionary(itm => itm.Key, itm => itm.Value);
        }
        
        private static TransactionEventAttributes GetGrpcRequestAttributes(List<string> locations, SyncGrpcHeader[] headers)
        {
            locations ??= new List<string>();
            locations.Add(Startup.Settings.Location);


            return new TransactionEventAttributes(locations, DataSynchronizationPeriod.Sec1, 
                EventSource.Synchronization, ToDomainHeaders(headers));
        }
        
        public ValueTask<PingGrpcResponse> PingAsync(PingGrpcRequest request)
        {
            NodesPool.GetOrCreateNode(request.Location);
            return new ValueTask<PingGrpcResponse>(new PingGrpcResponse
            {
                Location = Startup.Settings.Location
            });
        }



        public async ValueTask SaveTableSnapshotAsync(IAsyncEnumerable<PayloadWrapperGrpcModel> payloads)
        {
            var model = await payloads.MergePayloadAndDeserialize<WriteTableSnapshotGrpcModel>(false);

            var initTableTransactionEvent = new InitTableTransactionEvent
            {
                TableName = model.TableName,
                Snapshot = model.TablePartitions.ToDictionary(
                    itm => itm.PartitionKey,
                    snapshot => snapshot.Snapshot.ParseDbRowList()),
                Attributes = GetGrpcRequestAttributes(model.Locations, model.Headers)
            };
            
            ServiceLocator.NodesSyncOperations.ReplaceTable(initTableTransactionEvent);
            
        }

        public async ValueTask SaveTableSnapshotCompressedAsync(IAsyncEnumerable<PayloadWrapperGrpcModel> payloads)
        {
            var model = await payloads.MergePayloadAndDeserialize<WriteTableSnapshotGrpcModel>(false);

            var initTableTransactionEvent = new InitTableTransactionEvent
            {
                TableName = model.TableName,
                Snapshot = model.TablePartitions.ToDictionary(
                    itm => itm.PartitionKey,
                    snapshot => snapshot.Snapshot.ParseDbRowList()),
                Attributes = GetGrpcRequestAttributes(model.Locations, model.Headers)
            };
            
            ServiceLocator.NodesSyncOperations.ReplaceTable(initTableTransactionEvent);
        }

        public async ValueTask SavePartitionSnapshotAsync(IAsyncEnumerable<PayloadWrapperGrpcModel> payloads)
        {
            var model = await payloads.MergePayloadAndDeserialize<WritePartitionSnapshotGrpcModel>(false);

            var initPartitionsTransactionEvent = new InitPartitionsTransactionEvent
            {
                TableName = model.TableName,
                Partitions = model.PartitionsToBeInitialized.ToDictionary(
                    itm => itm.PartitionKey,
                    snapshot => snapshot.Snapshot.ParseDbRowList()),
                Attributes = GetGrpcRequestAttributes(model.Locations, model.Headers)
            };
            
            ServiceLocator.NodesSyncOperations.ReplacePartitions(initPartitionsTransactionEvent);
        }

        public async ValueTask SavePartitionCompressedSnapshotAsync(IAsyncEnumerable<PayloadWrapperGrpcModel> payloads)
        {
            var model = await payloads.MergePayloadAndDeserialize<WritePartitionSnapshotGrpcModel>(true);

            var initPartitionsTransactionEvent = new InitPartitionsTransactionEvent
            {
                TableName = model.TableName,
                Partitions = model.PartitionsToBeInitialized.ToDictionary(
                    itm => itm.PartitionKey,
                    snapshot => snapshot.Snapshot.ParseDbRowList()),
                Attributes = GetGrpcRequestAttributes(model.Locations, model.Headers)
            };
            
            ServiceLocator.NodesSyncOperations.ReplacePartitions(initPartitionsTransactionEvent);
        }

        public async ValueTask SaveRowsSnapshotAsync(IAsyncEnumerable<PayloadWrapperGrpcModel> payloads)
        {
            var model = await payloads.MergePayloadAndDeserialize<WriteRowsUpdateGrpcModel>(false);

            var updateRowsTransactionEvent = new UpdateRowsTransactionEvent
            {
                TableName = model.TableName,
                RowsByPartition = model.Rows.ToDictionary(
                    itm => itm.PartitionKey,
                    snapshot => snapshot.Snapshot.ParseDbRowList()),
                Attributes = GetGrpcRequestAttributes(model.Locations, model.Headers)
            };
            
            ServiceLocator.NodesSyncOperations.UpdateRows(updateRowsTransactionEvent);
        }

        public async ValueTask SaveRowsCompressedSnapshotAsync(IAsyncEnumerable<PayloadWrapperGrpcModel> payloads)
        {
            var model = await payloads.MergePayloadAndDeserialize<WriteRowsUpdateGrpcModel>(true);

            var updateRowsTransactionEvent = new UpdateRowsTransactionEvent
            {
                TableName = model.TableName,
                RowsByPartition = model.Rows.ToDictionary(
                    itm => itm.PartitionKey,
                    snapshot => snapshot.Snapshot.ParseDbRowList()),
                Attributes = GetGrpcRequestAttributes(model.Locations, model.Headers)
            };
            
            ServiceLocator.NodesSyncOperations.UpdateRows(updateRowsTransactionEvent);
        }

        public async ValueTask DeleteRowsAsync(IAsyncEnumerable<PayloadWrapperGrpcModel> payloads)
        {
            var model = await payloads.MergePayloadAndDeserialize<DeleteRowsGrpcRequest>(true);

            var deleteRowsTransactionEvent = new DeleteRowsTransactionEvent
            {
                TableName = model.TableName,
                Rows = model.RowsToDelete.ToDictionary(
                    itm => itm.PartitionKey,
                    rowKeys => rowKeys.RowKeys.AsReadOnlyList()),
                Attributes = GetGrpcRequestAttributes(model.Locations, model.Headers)
            };
            
            ServiceLocator.NodesSyncOperations.DeleteRows(deleteRowsTransactionEvent);
        }

        public ValueTask DeleteTablePartitionsAsync(DeleteTablePartitionGrpcRequest request)
        {
            ServiceLocator.DbOperations.DeletePartitions(request.TableName, request.PartitionKeys, 
                GetGrpcRequestAttributes(request.Locations, request.Headers));
            return new ValueTask();
        }

        public ValueTask SetTableAttributesAsync(SetTableAttributesGrpcRequest request)
        {
            var dbTable = ServiceLocator.DbInstance.GetTable(request.TableName);
            ServiceLocator.DbOperations.SetTableAttributes(dbTable, request.Persist, request.MaxPartitionsAmount, 
                GetGrpcRequestAttributes(request.Locations, request.Headers));

            return new ValueTask();
        }


        private static IEnumerable<ReadTableAttributeGrpcModel> GetTables()
        {
            var tables = ServiceLocator.DbInstance.GetTables();

            foreach (var dbTable in tables)
            {
                yield return new ReadTableAttributeGrpcModel
                {
                    TableName = dbTable.Name,
                    Attributes = new TableAttributesGrpcData
                    {
                        Persist =  dbTable.Persist,
                        MaxPartitionsAmount = dbTable.MaxPartitionsAmount
                    }
                };
            }
        }

        public IAsyncEnumerable<ReadTableAttributeGrpcModel> GetTablesAsync()
        {
            return GetTables().ToAsyncEnumerable();
        }


        private static IEnumerable<ReadTablePartitionGrpcModel> DownloadTable(DownloadTableGrpcRequest request)
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
                yield return new ReadTablePartitionGrpcModel
                {
                    PartitionKey = partitionKey,
                    Content = snapshot
                };
            }
        }

        public IAsyncEnumerable<ReadTablePartitionGrpcModel> DownloadTableAsync(DownloadTableGrpcRequest request)
        {
            return DownloadTable(request).ToAsyncEnumerable();
        }
        
        private static IEnumerable<ReadTablePartitionGrpcModel> DownloadCompressedTable(DownloadTableGrpcRequest request)
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
                yield return new ReadTablePartitionGrpcModel
                {
                    PartitionKey = partitionKey,
                    Content =  MyNoSqlServerDataCompression.ZipPayload(snapshot)
                };
            }
        }

        public IAsyncEnumerable<ReadTablePartitionGrpcModel> DownloadTableCompressedAsync(DownloadTableGrpcRequest request)
        {
            return DownloadCompressedTable(request).ToAsyncEnumerable();
        }
    }
}