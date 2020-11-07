using System.Collections.Generic;
using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Tests.MockServices
{
    public class ReplicaSynchronizationServiceMock : IReplicaSynchronizationService
    {
        public void PublishInitTable(DbTable dbTable)
        {
            
        }

        public void PublishInitPartition(DbTable dbTable, string partitionKey)
        {
        }

        public void SynchronizeUpdate(DbTable dbTable, IReadOnlyList<DbRow> dbRow)
        {
        }

        public void SynchronizeDelete(DbTable dbTable, IReadOnlyList<DbRow> dbRows)
        {
        }
    }
}