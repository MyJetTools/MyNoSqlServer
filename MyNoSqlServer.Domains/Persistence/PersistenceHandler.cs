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

        public void SynchronizeTable(DbTable dbTable, DataSynchronizationPeriod period)
        {

            if (!dbTable.Persist)
                return;
            
            _snapshotSaverScheduler.SynchronizeTable(dbTable, period);

            if (period == DataSynchronizationPeriod.Immediately)
                Task.Run(async () =>
                {
                    await _snapshotSaverEngine.SynchronizeImmediatelyAsync(dbTable);
                });

        }

        public void SynchronizePartition(DbTable dbTable, string partitionKey,
            DataSynchronizationPeriod period)
        {
            
            if (!dbTable.Persist)
                return;
            
            _snapshotSaverScheduler.SynchronizePartition(dbTable, partitionKey, period);

            if (period == DataSynchronizationPeriod.Immediately)
                Task.Run(async () =>
                {
                    await _snapshotSaverEngine.SynchronizeImmediatelyAsync(dbTable);
                });

        }




    }
}