using System;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.SnapshotSaver
{
    public enum DataSynchronizationPeriod
    {
        Immediately, Sec1, Sec5, Sec15, Sec30, Min1
    }
    
    public interface ISyncTask
    {
        DateTime SyncDateTime { get; }
    }

    public interface ISnapshotSaverScheduler
    {
        void SynchronizePartition(DbTable dbTable, DbPartition partitionToSave, DataSynchronizationPeriod period);
        
        void SynchronizeTable(DbTable dbTable, DataSynchronizationPeriod period);
        
        void SynchronizeDeletePartition(string tableName, string partitionKey, DataSynchronizationPeriod period);

        ISyncTask GetTaskToSync(bool appIsShuttingDown);


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