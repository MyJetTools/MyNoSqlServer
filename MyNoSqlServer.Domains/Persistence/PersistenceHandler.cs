using System.Threading.Tasks;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.SnapshotSaver;

namespace MyNoSqlServer.Domains.Persistence
{
    public class PersistenceHandler
    {
        private readonly ISnapshotSaverScheduler _snapshotSaverScheduler;
        private readonly SnapshotSaverEngine _snapshotSaverEngine;

        public PersistenceHandler(ISnapshotSaverScheduler snapshotSaverScheduler, SnapshotSaverEngine snapshotSaverEngine)
        {
            _snapshotSaverScheduler = snapshotSaverScheduler;
            _snapshotSaverEngine = snapshotSaverEngine;
        }

        public ValueTask SynchronizeTableAsync(DbTable dbTable, DataSynchronizationPeriod period)
        {

            if (!dbTable.Persist)
                return new ValueTask();
            
            _snapshotSaverScheduler.SynchronizeTable(dbTable, period);
            
            return period == DataSynchronizationPeriod.Immediately 
                ? _snapshotSaverEngine.SynchronizeImmediatelyAsync(dbTable) 
                : new ValueTask();
        }

        public ValueTask SynchronizePartitionAsync(DbTable dbTable, string partitionKey,
            DataSynchronizationPeriod period)
        {
            
            if (!dbTable.Persist)
                return new ValueTask();
            
            _snapshotSaverScheduler.SynchronizePartition(dbTable, partitionKey, period);

            return period == DataSynchronizationPeriod.Immediately 
                ? _snapshotSaverEngine.SynchronizeImmediatelyAsync(dbTable) 
                : new ValueTask();
        }




    }
}