using System.Collections.Generic;
using MyNoSqlServer.Domains.Json;

namespace MyNoSqlServer.Domains.Db.Tables
{
    public interface IUpdateTransaction
    {

    }

    public interface ICleanTableTransaction : IUpdateTransaction
    {
        
    }

    public interface ICleanPartitionsTransaction: IUpdateTransaction
    {
        string[] Partitions { get;  }
    }

    public interface IDeleteRowsTransaction: IUpdateTransaction
    {
        string PartitionKey { get; set; }
        string[] RowKeys { get; set; }
    }
    
    public interface IInsertOrUpdateTransaction: IUpdateTransaction
    {
        List<DynamicEntity> Entities { get; set; }
    }
}