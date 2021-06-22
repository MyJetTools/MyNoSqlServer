using System.Collections.Generic;
using MyNoSqlServer.DataCompression;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.Domains.Persistence.MasterNode
{
    /*
    public class MyNoSqlServerTableLoader : ITableLoader
    {
        private readonly IMyNoSqlServerNodePersistenceGrpcService _grpcService;
        private readonly IMyNoSqlNodePersistenceSettings _settings;
        public string TableName { get; }
        public bool Persist { get; }

        public MyNoSqlServerTableLoader(string tableName, bool persist, IMyNoSqlServerNodePersistenceGrpcService grpcService, IMyNoSqlNodePersistenceSettings settings)
        {
            _grpcService = grpcService;
            _settings = settings;
            TableName = tableName;
            Persist = persist;
        }
        
        
        public async IAsyncEnumerable<PartitionSnapshot> GetPartitionsAsync()
        {

            var request = new DownloadTableGrpcRequest()
            {
                TableName = TableName
            };

            if (_settings.CompressData)
            {
                await foreach (var partition in  _grpcService.DownloadTableCompressedAsync(request))
                {
                    yield return new PartitionSnapshot
                    {
                        PartitionKey = partition.PartitionKey,
                        Snapshot = MyNoSqlServerDataCompression.UnZipPayload(partition.Content)
                    };
                }
                
            }
            else
            {
                await foreach (var partition in  _grpcService.DownloadTableAsync(request))
                {
                    yield return new PartitionSnapshot
                    {
                        PartitionKey = partition.PartitionKey,
                        Snapshot = MyNoSqlServerDataCompression.UnZipPayload(partition.Content)
                    };
                }
            }
          
        }
    }
    */
}