using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using MyNoSqlServer.Api.Services;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Json;

namespace MyNoSqlServer.Api.Models
{
    public class StartTransactionResponse
    {
        public string TransactionId { get; set; }
    }

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
    
    public class InsertOrUpdateTransaction : BaseTransaction, IInsertOrUpdateTransaction
    {
        public static string Id => "InsertOrUpdate";


        public List<DynamicEntity> Entities { get; set; }
    }        
    

    public static class TransactionsDeserializer
    {
        public static IEnumerable<IUpdateTransaction> GetTransactions(IMyMemory memory)
        {
            foreach (var transactionItem in memory.SplitJsonArrayToObjects())
            {
                var json = Encoding.UTF8.GetString(transactionItem.Span);
                
                var baseTransaction = JsonSerializer.Deserialize<BaseTransaction>(json);

                if (baseTransaction == null)
                    throw new Exception("Can not deserialize transaction");

                if (baseTransaction.Type == CleanTableTransaction.Id)
                    yield return JsonSerializer.Deserialize<CleanTableTransaction>(json);

                if (baseTransaction.Type == CleanPartitionsTransaction.Id)
                    yield return JsonSerializer.Deserialize<CleanPartitionsTransaction>(json);

                if (baseTransaction.Type == DeleteRowsTransaction.Id)
                    yield return JsonSerializer.Deserialize<DeleteRowsTransaction>(json);

                if (baseTransaction.Type == InsertOrUpdateTransaction.Id)
                {
                    var result = new InsertOrUpdateTransaction
                    {
                        Type = baseTransaction.Type,
                        Entities = new List<DynamicEntity>()
                    };

                    foreach (var entity in transactionItem.SplitJsonArrayToObjects())
                    {
                        result.Entities.Add(entity.ParseDynamicEntity());
                    }

                    yield return result;
                }
            }

        } 
        
    }
}