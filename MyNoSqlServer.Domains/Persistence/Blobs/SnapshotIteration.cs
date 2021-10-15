using System.Collections.Generic;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains.Persistence.Blobs
{
    public class SnapshotIteration
    {
        
 
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
        
        
        public static SnapshotIteration Create(List<ITransactionEvent> eventsByTable)
        {
            var snapshot = new SnapshotIteration();

            foreach (var transactionEvent in eventsByTable)
            {
                switch (transactionEvent)
                {
                    
                    case UpdateTableAttributesTransactionEvent:
                        snapshot.SyncTableAttributes();   
                        break;
                    
                    case InitTableTransactionEvent:
                        snapshot.SyncTable();
                        break;
                    
                    case InitPartitionsTransactionEvent initPartitionsTransactionEvent:
                        foreach (var (partitionKey, _) in initPartitionsTransactionEvent.Partitions)
                            snapshot.SyncPartition(partitionKey);    
                        break;

                    case UpdateRowsTransactionEvent updateRowsTransactionEvent:
                        foreach (var (partitionKey, _) in updateRowsTransactionEvent.RowsByPartition)
                            snapshot.SyncPartition(partitionKey);   
                        break;

                    case DeleteRowsTransactionEvent deleteRowsTransactionEvent:
                        foreach (var (partitionKey, _) in deleteRowsTransactionEvent.Rows)
                            snapshot.SyncPartition(partitionKey);   
                        break;
                }
            }

            return snapshot;
        }
        
        
    }
}