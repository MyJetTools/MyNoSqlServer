using System;
using System.Collections.Generic;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.SnapshotSaver
{

    public interface IPersistTableEvent
    {
        DbTable Table { get; }
        DataSynchronizationPeriod Period { get; }
        public DateTime SnapshotDateTime { get; }
    }
    

    public interface ITableToSaveEventsQueue
    {
        public string Table { get; }

        public IPersistTableEvent Dequeue();
    }
    
    
    public interface ISnapshotSaverScheduler
    {
        
        void SynchronizeCreateTable(DbTable dbTable, DataSynchronizationPeriod period, DateTime snapshotDateTime);
        void SynchronizeTable(DbTable dbTable, DataSynchronizationPeriod period, DateTime snapshotDateTime);
        void SynchronizePartition(DbTable dbTable, DbPartition partition, DataSynchronizationPeriod period, DateTime snapshotDateTime);
        void SynchronizeDeletePartition(DbTable dbTable, DbPartition partition, 
            DataSynchronizationPeriod period, DateTime snapshotDateTime);
        void SynchronizeDeleteTable(DbTable dbTable, 
            DataSynchronizationPeriod period, DateTime snapshotDateTime);

        IReadOnlyList<ITableToSaveEventsQueue> GetEventsQueue();
        ITableToSaveEventsQueue TryGetEventsQueue(string tableName);


        int TasksToSyncCount();

    }
    
    public static class DataSynchronizationServiceDecorators
    {

        private static int GetSecondsAmount(this DataSynchronizationPeriod period)
        {
            switch (period)
            {
                
                case DataSynchronizationPeriod.Sec1:
                    return 1;
                
                case DataSynchronizationPeriod.Sec5:
                    return 5;

                case DataSynchronizationPeriod.Sec15:
                    return 15;

                case DataSynchronizationPeriod.Sec30:
                    return 30;

                case DataSynchronizationPeriod.Min1:
                    return 60;
                
            }

            return 0;
        }
        
        public static DateTime GetNextPeriod(this DateTime dateTime, DataSynchronizationPeriod period)
        {
            return dateTime.AddSeconds(period.GetSecondsAmount());
        }
        
    }
    
}