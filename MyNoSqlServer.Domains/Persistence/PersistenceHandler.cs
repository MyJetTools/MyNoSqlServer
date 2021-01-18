using System;
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
        private readonly SnapshotSaverEngine _snapshotSaverEngine;

        public PersistenceHandler(ISnapshotStorage snapshotStorage, ISnapshotSaverScheduler snapshotSaverScheduler, 
            SnapshotSaverEngine snapshotSaverEngine)
        {
            _snapshotStorage = snapshotStorage;
            _snapshotSaverScheduler = snapshotSaverScheduler;
            _snapshotSaverEngine = snapshotSaverEngine;
        }
        
        public ValueTask SynchronizeCreateTableAsync(DbTable dbTable, DataSynchronizationPeriod period, 
            DateTime snapshotDateTime)
        {
            _snapshotSaverScheduler.SynchronizeCreateTable(dbTable, period, snapshotDateTime);
            
            return period == DataSynchronizationPeriod.Immediately 
                ? _snapshotStorage.SaveTableSnapshotAsync(dbTable) 
                : new ValueTask();
        }

        public ValueTask SynchronizeTableAsync(DbTable dbTable, DataSynchronizationPeriod period, 
            DateTime snapshotDateTime)
        {

            if (!dbTable.PersistTable)
                return new ValueTask();
            
            _snapshotSaverScheduler.SynchronizeTable(dbTable, period, snapshotDateTime);
            
            return period == DataSynchronizationPeriod.Immediately 
                ? _snapshotStorage.SaveTableSnapshotAsync(dbTable) 
                : new ValueTask();
        }

        public ValueTask SynchronizePartitionAsync(DbTable dbTable, DbPartition partition, 
            DataSynchronizationPeriod period, 
            DateTime snapshotDateTime)
        {
            if (!dbTable.PersistTable)
                return new ValueTask();
            
            _snapshotSaverScheduler.SynchronizePartition(dbTable, partition, period, snapshotDateTime);
            
            return period == DataSynchronizationPeriod.Immediately 
                ? _snapshotSaverEngine.SynchronizeAsync(dbTable.Name) 
                : new ValueTask();
        }

        public ValueTask SynchronizeDeletePartitionAsync(DbTable dbTable,
            DbPartition dbPartition, DataSynchronizationPeriod period, 
            DateTime snapshotDateTime)
        {
            if (!dbTable.PersistTable)
                return new ValueTask();
            
            _snapshotSaverScheduler.SynchronizeDeletePartition(dbTable, dbPartition, period, snapshotDateTime);
            
            return period == DataSynchronizationPeriod.Immediately 
                ? _snapshotSaverEngine.SynchronizeAsync(dbTable.Name) 
                : new ValueTask();
        }
        
        public ValueTask SynchronizeDeleteTableAsync(DbTable dbTable,
            DataSynchronizationPeriod period, 
            DateTime snapshotDateTime)
        {
            
            _snapshotSaverScheduler.SynchronizeDeleteTable(dbTable, period, snapshotDateTime);
            
            return period == DataSynchronizationPeriod.Immediately 
                ? _snapshotSaverEngine.SynchronizeAsync(dbTable.Name) 
                : new ValueTask();
        }
        
    }
}