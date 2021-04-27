using System.Collections.Generic;
using MyNoSqlServer.Domains.Json;

namespace MyNoSqlServer.Domains.Transactions
{
    public class BaseTransaction
    {
        public string Type { get; set; }
    }


    public class CleanTableTransaction : BaseTransaction, ICleanTableTransaction
    {
        public static string Id => "CleanTable";
    }

    public class CleanPartitionsTransaction : BaseTransaction, ICleanPartitionsTransaction
    {
        public static string Id => "CleanPartitions";
        public string[] Partitions { get; set; }
    }
    
    public class DeleteRowsTransaction : BaseTransaction, IDeleteRowsTransaction
    {
        public static string Id => "DeletePartitions";
        public string PartitionKey { get; set; }
        public string[] RowKeys { get; set; }
    }    
    
    public class InsertOrReplaceEntitiesTransaction : BaseTransaction, IInsertOrReplaceEntitiesTransaction
    {
        public static string Id => "InsertOrUpdate";


        public List<DynamicEntity> Entities { get; set; }
    }   
}