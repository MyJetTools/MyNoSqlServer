using System.Threading.Tasks;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.DataSynchronization;
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

        public ValueTask<OperationResult> SynchronizePartitionAsync(DbTable dbTable, DbPartition partitionToSave,
            DataSynchronizationPeriod period)
        {

            if (period == DataSynchronizationPeriod.Immediately)
            {
                var partitionSnapshot = PartitionSnapshot.Create(dbTable, partitionToSave);
                return new ValueTask<OperationResult>(
                    _snapshotStorage
                        .SavePartitionSnapshotAsync(partitionSnapshot)
                        .AsTask()
                        .ContinueWith(itm => OperationResult.Ok));
            }

            _snapshotSaverScheduler.SynchronizePartition(dbTable, partitionToSave, period);
            return new ValueTask<OperationResult>(OperationResult.Ok);
        }

        public ValueTask<OperationResult> SynchronizeDeletePartitionAsync(DbTable dbTable,
            DbPartition dbPartition, DataSynchronizationPeriod period)
        {

            if (period == DataSynchronizationPeriod.Immediately)
                return new ValueTask<OperationResult>(_snapshotStorage
                    .DeleteTablePartitionAsync(dbTable.Name, dbPartition.PartitionKey)
                    .AsTask()
                    .ContinueWith(itm => OperationResult.Ok));

            _snapshotSaverScheduler.SynchronizePartition(dbTable, dbPartition, period);
            return new ValueTask<OperationResult>(OperationResult.Ok);
        }
    }
}