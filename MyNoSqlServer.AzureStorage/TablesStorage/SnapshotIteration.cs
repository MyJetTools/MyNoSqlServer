using System.Collections.Generic;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.AzureStorage.TablesStorage
{
    public class SnapshotIteration
    {
        
        public DbTable DbTable { get;  }

        public SnapshotIteration(DbTable dbTable)
        {
            DbTable = dbTable;
        }
        public bool SyncWholeTable { get; private set; }
        
        public bool HasSyncTableAttributes { get; private set; }
        
        public Dictionary<string, bool> PartitionsToSync { get; private set; }

        public void SyncTable()
        {
            SyncWholeTable = true;
        }
        
        public void SyncTableAttributes()
        {
            HasSyncTableAttributes = true;
        }

        public void SyncPartition(string partitionKey)
        {
            PartitionsToSync ??= new Dictionary<string, bool>();
            
            if (!PartitionsToSync.ContainsKey(partitionKey))
                PartitionsToSync.Add(partitionKey, true);
        }
        
        
    }
}