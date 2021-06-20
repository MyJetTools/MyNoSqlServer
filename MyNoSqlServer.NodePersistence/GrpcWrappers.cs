using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.Domains.TransactionEvents;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.NodePersistence
{
    public static class GrpcWrappers
    {


        public static  PartitionDataGrpcModel[] DataRowsToGrpcContent(this IReadOnlyDictionary<string, IReadOnlyList<DbRow>> rowsContent)
        {
            return rowsContent.Select(itm => new PartitionDataGrpcModel
            {
                PartitionKey = itm.Key,
                Snapshot = itm.Value.ToJsonArray().AsArray()
            }).ToArray();
        }


        
        public static SyncGrpcHeader[] ToGrpcHeaders(this IReadOnlyDictionary<string, string> src)
        {
            if (src == null)
                return Array.Empty<SyncGrpcHeader>();
            
            
            if (src.Count == 0)
                return Array.Empty<SyncGrpcHeader>();

            return src.Select(itm => new SyncGrpcHeader
            {
                Key = itm.Key,
                Value = itm.Value
            }).ToArray();
        }

    }
}