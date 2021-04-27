using System.Collections.Generic;
using MyNoSqlServer.Domains.Json;

namespace MyNoSqlServer.Domains.Transactions
{
    public interface IDbTransaction
    {

    }

    public interface ICleanTableTransaction : IDbTransaction
    {
        
    }

    public interface ICleanPartitionsTransaction: IDbTransaction
    {
        string[] Partitions { get;  }
    }

    public interface IDeleteRowsTransaction: IDbTransaction
    {
        string PartitionKey { get; set; }
        string[] RowKeys { get; set; }
    }
    
    public interface IInsertOrReplaceEntitiesTransaction: IDbTransaction
    {
        List<DynamicEntity> Entities { get; set; }
    }
}