using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.NodePersistence.Grpc;

namespace MyNoSqlServer.Domains.Nodes
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