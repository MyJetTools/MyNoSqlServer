using System.Collections.Generic;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.DataSynchronization
{
    public interface IReplicaSynchronizationService
    {
        void PublishInitTable(DbTable dbTable);
        void PublishInitPartition(DbTable dbTable, DbPartition partition);
        void SynchronizeUpdate(DbTable dbTable, IReadOnlyList<DbRow> dbRow);
        void SynchronizeDelete(DbTable dbTable, IReadOnlyList<DbRow> dbRows);
    }

}