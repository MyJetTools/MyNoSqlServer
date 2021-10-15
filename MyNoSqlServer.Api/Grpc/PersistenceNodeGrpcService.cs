using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Common;
using MyNoSqlServer.DataCompression;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Nodes;
using MyNoSqlServer.Domains.TransactionEvents;
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
            ServiceLocator.NodeSessionsList.GetOrCreate(request.Location, Startup.Settings.Location);
            return new ValueTask<PingGrpcResponse>(new PingGrpcResponse
            {
                Location = Startup.Settings.Location
            });
        }

        public async ValueTask SyncTransactionsAsync(IAsyncEnumerable<PayloadWrapperGrpcModel> payloads)
        {
            try
            {
                var model = await payloads.MergePayloadAndDeserialize<List<SyncTransactionGrpcModel>>(false);
                HandleGrpcResponse(model);
            }
            catch (Exception e)
            {
               ServiceLocator.AppLogs.WriteError(null, "PersistenceNodeGrpcService.SyncTransactionsAsync", "N/A", e);
                throw;
            }
       
        }

        public async ValueTask SyncTransactionsCompressedAsync(IAsyncEnumerable<PayloadWrapperGrpcModel> payloads)
        {
            try
            {
                var model = await payloads.MergePayloadAndDeserialize<List<SyncTransactionGrpcModel>>(true);
                HandleGrpcResponse(model);
            }
            catch (Exception e)
            {
                ServiceLocator.AppLogs.WriteError(null, "PersistenceNodeGrpcService.SyncTransactionsCompressedAsync", "N/A", e);
                throw;
            }

        }


        private static void HandleGrpcResponse(IReadOnlyList<SyncTransactionGrpcModel> events)
        {
            foreach (var transactionEvent in events)
            {
                ServiceLocator.SyncTransactionHandler.HandleTransaction(transactionEvent, 
                    ()=>GetGrpcRequestAttributes(transactionEvent.Locations, transactionEvent.Headers));
            }
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