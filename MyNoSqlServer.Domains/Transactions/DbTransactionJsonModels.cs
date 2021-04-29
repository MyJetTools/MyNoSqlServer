using System.Collections.Generic;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.Domains.Transactions
{
    public class BaseTransaction
    {
        public string Type { get; set; }
    }


    public class CleanTableTransactionJsonModel : BaseTransaction, ICleanTableTransactionAction
    {
        public static string Id => "CleanTable";
        public string TableName { get; set; }
    }

    public class DeletePartitionsTransactionActionJsonModel : BaseTransaction, IDeletePartitionsTransactionAction
    {
        public static string Id => "CleanPartitions";
        public string TableName { get; set; }
        public string[] PartitionKeys { get; set; }
    }
    
    public class DeleteRowsTransactionJsonModel : BaseTransaction, IDeleteRowsTransactionAction
    {
        public static string Id => "DeletePartitions";
        public string TableName { get; set; }
        public string PartitionKey { get; set; }
        public string[] RowKeys { get; set; }
    }    
    
    public class InsertOrReplaceEntitiesTransactionJsonModel : BaseTransaction, IInsertOrReplaceEntitiesTransactionAction
    {
        public static string Id => "InsertOrUpdate";
        public string TableName { get; set; }

        public List<byte[]> Entities { get; set; }
    }   
}