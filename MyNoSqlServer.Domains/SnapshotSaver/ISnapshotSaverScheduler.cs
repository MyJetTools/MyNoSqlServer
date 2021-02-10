using System;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.SnapshotSaver
{
    public interface ISyncTask
    {
        DateTime SyncDateTime { get; set; }
        
        long Id { get; set; }

    }


    public interface ICreateTableSyncTask : ISyncTask
    {
        
    }

    public interface ISyncTableTask : ISyncTask
    {
        
    }

    public interface ISyncPartitionTask : ISyncTask
    {
        public string PartitionKey { get; }
    }
    
    public interface IDeletePartitionTask : ISyncTask
    {
        public string PartitionKey { get; }   
    }

    public interface ISnapshotSaverScheduler
    {

        void SynchronizeCreateTable(DbTable dbTable);
        
        void SynchronizePartition(DbTable dbTable, DbPartition partitionToSave, DataSynchronizationPeriod period);
        
        void SynchronizeTable(DbTable dbTable, DataSynchronizationPeriod period);
        
        void SynchronizeDeletePartition(DbTable dbTable, string partitionKey, DataSynchronizationPeriod period);

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