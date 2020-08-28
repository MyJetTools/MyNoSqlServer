using System;
using System.Collections.Generic;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.ChangesBroadcasting
{
    public interface IChangesBroadcaster
    {
        
        IReadOnlyList<string> Tables { get; }
        
        string Name { get; }
        string Ip { get; }
        
        DateTime Created { get; }
        DateTime LastUpdate { get; }
        
        void PublishInitTable(DbTable dbTable);
        void PublishInitPartition(DbTable dbTable, DbPartition partition);
        
        void SynchronizeUpdate(DbTable dbTable, DbRow dbRow);
        void SynchronizeUpdate(DbTable dbTable, IReadOnlyList<DbRow> dbRows);
        
        void SynchronizeDelete(DbTable dbTable, IReadOnlyList<DbRow> dbRows);
        
        string Id { get; } 
    }

}