using System.Threading.Tasks;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.SnapshotSaver;

namespace MyNoSqlServer.Domains.Persistence
{
    public class PersistenceHandler
    {
        private readonly ISnapshotStorage _snapshotStorage;
        private readonly ISnapshotSaverScheduler _snapshotSaverScheduler;

        public PersistenceHandler(ISnapshotStorage snapshotStorage, ISnapshotSaverScheduler snapshotSaverScheduler)
        {
            _snapshotStorage = snapshotStorage;
            _snapshotSaverScheduler = snapshotSaverScheduler;
        }

        public ValueTask SynchronizeTableAsync(DbTable dbTable, DataSynchronizationPeriod period)
        {
            if (period == DataSynchronizationPeriod.Immediately)
                return _snapshotStorage.SaveTableSnapshotAsync(dbTable);

            _snapshotSaverScheduler.SynchronizeTable(dbTable, period);
            return new ValueTask();
        }

        public ValueTask SynchronizePartitionAsync(DbTable dbTable, DbPartition dbPartition,
            DataSynchronizationPeriod period)
        {

            if (period == DataSynchronizationPeriod.Immediately)
            {
                var partitionSnapshot = PartitionSnapshot.Create(dbTable, dbPartition.PartitionKey);
                return _snapshotStorage
                    .SavePartitionSnapshotAsync(partitionSnapshot);
            }

            _snapshotSaverScheduler.SynchronizePartition(dbTable, dbPartition.PartitionKey, period);
            return new ValueTask();
        }

        public ValueTask SynchronizeDeletePartitionAsync(DbTable dbTable,
            DbPartition dbPartition, DataSynchronizationPeriod period)
        {
            if (period == DataSynchronizationPeriod.Immediately)
                return _snapshotStorage
                    .DeleteTablePartitionAsync(dbTable.Name, dbPartition.PartitionKey);

            _snapshotSaverScheduler.SynchronizePartition(dbTable, dbPartition.PartitionKey, period);
            return new ValueTask();
        }
        
    }
}