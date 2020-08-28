using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AsyncAwaitUtils;
using MyNoSqlServer.Domains.ChangesBroadcasting;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Grpc.Contracts;

namespace MyNoSqlServer.Api.Grpc
{
    public class GrpcChangesBroadcaster : IChangesBroadcaster, IDisposable
    {

        public GrpcChangesBroadcaster(string name, string ip, IReadOnlyList<string> tables)
        {
            Id = Guid.NewGuid().ToString("N");
            Name = name;
            Ip = ip;
            Tables = tables;
        }

        
        private readonly AsyncQueue<ChangeGrpcResponseContract> _queue = new AsyncQueue<ChangeGrpcResponseContract>();
        
        
        public bool Connected { get; set; }
        public IReadOnlyList<string> Tables { get; }
        public string Name { get; }
        public string Ip { get; }
        public DateTime Created { get; } = DateTime.UtcNow;
        public DateTime LastUpdate { get; } = DateTime.UtcNow;

        public void PublishInitTable(DbTable dbTable)
        {
            var result = new ChangeGrpcResponseContract
            {
                TableName = dbTable.Name,
                InitTableData = dbTable.GetJsonArray()
            };
            
            _queue.Put(result);
        }

        public void PublishInitPartition(DbTable dbTable, DbPartition partition)
        {
            var result = new ChangeGrpcResponseContract
            {
              
                TableName = dbTable.Name,
                InitPartitionData = partition.GetAllRows().ToJsonArray().AsArray()
            };
            
            _queue.Put(result);
        }

        public void SynchronizeUpdate(DbTable dbTable, DbRow dbRow)
        {
            var result = new ChangeGrpcResponseContract
            {
              
                TableName = dbTable.Name,
                UpdateRowsData = new[]{dbRow}.ToJsonArray().AsArray()
            };
            
            _queue.Put(result);
        }

        public void SynchronizeUpdate(DbTable dbTable, IReadOnlyList<DbRow> dbRows)
        {
            var result = new ChangeGrpcResponseContract
            {
                TableName = dbTable.Name,
                UpdateRowsData = dbRows.ToJsonArray().AsArray()
            };
            
            _queue.Put(result);
        }

        public void SynchronizeDelete(DbTable dbTable, IReadOnlyList<DbRow> dbRows)
        {
            var result = new ChangeGrpcResponseContract
            {
                TableName = dbTable.Name,
                DeletedRows = dbRows
                    .GroupBy(dbRow => dbRow.PartitionKey)
                    .Select(group => new DeleteDbRowContract
                    {
                        PartitionKey = group.Key,
                        RowKeys = group.Select(itm => itm.RowKey).ToArray()
                    }).ToArray()
            };
            
            _queue.Put(result);
        }

        public Task<ChangeGrpcResponseContract> GetResponseAsync()
        {
            return _queue.GetAsync();
        }

        public string Id { get; }
        public void Dispose()
        {
            _queue.Dispose();
        }
    }
}