using System.Collections.Generic;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.Domains.Persistence.MasterNode
{
    
    public class MyNoSqlServerNodePersistence : ITablesPersistenceReader 
    {
        private readonly IMyNoSqlServerNodePersistenceGrpcService _grpcService;
        private readonly IMyNoSqlNodePersistenceSettings _settings;


        public MyNoSqlServerNodePersistence(IMyNoSqlServerNodePersistenceGrpcService grpcService,
            IMyNoSqlNodePersistenceSettings settings)
        {
            _grpcService = grpcService;
            _settings = settings;
        }

        public async IAsyncEnumerable<ITableLoader> LoadTablesAsync()
        {
            await foreach (var table in _grpcService.GetTablesAsync())
            {
                yield return new MyNoSqlServerTableLoader(table.TableName, table.Attributes.Persist, _grpcService, _settings);
            }
        }


    }
}