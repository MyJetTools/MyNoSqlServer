using System;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Operations;


namespace MyNoSqlServer.Domains.GarbageCollection
{
    public class ExpiredEntitiesGarbageCollector
    {
        private readonly DbInstance _dbInstance;
        private readonly DbTableWriteOperations _dbTableWriteOperations;
        private readonly IMetricsWriter _metricsWriter;

        public ExpiredEntitiesGarbageCollector(DbInstance dbInstance, DbTableWriteOperations dbTableWriteOperations, 
            IMetricsWriter metricsWriter)
        {
            _dbInstance = dbInstance;
            _dbTableWriteOperations = dbTableWriteOperations;
            _metricsWriter = metricsWriter;
        }

        public async ValueTask DetectAndExpireAsync(DateTime nowTime)
        {
            var startTime = DateTime.UtcNow;

            var tables = _dbInstance.Tables;

            foreach (var dbTable in tables)
            {
                foreach (var (dbPartition, dbRows) in dbTable.GetExpiredEntities(nowTime))
                {
                    Console.WriteLine(
                        $"Expiring Partitions {dbTable.Name}/{dbPartition.PartitionKey}. Rows Amount: {dbRows.Count}");
                    await _dbTableWriteOperations.DeleteEntitiesAsync(dbTable, dbPartition, dbRows);
                }
            }

            _metricsWriter.WriteExpiredEntitiesGcDuration(DateTime.UtcNow - startTime);
        }
    }
}