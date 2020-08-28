using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;

namespace MyNoSqlServer.Domains.ChangesBroadcasting
{

    public class TableSubscriber
    {

        private readonly Dictionary<string, IChangesBroadcaster> _broadcasters 
            = new Dictionary<string, IChangesBroadcaster>();

        public IReadOnlyList<IChangesBroadcaster> Subscribers { get; private set; } = Array.Empty<IChangesBroadcaster>();

        public void Subscribe(IChangesBroadcaster broadcaster)
        {
            _broadcasters.Add(broadcaster.Id, broadcaster);
            Subscribers = _broadcasters.Values.ToList();
        }

        public void Unsubscribe(IChangesBroadcaster broadcaster)
        {
            if (!_broadcasters.ContainsKey(broadcaster.Id))
                return;

            _broadcasters.Remove(broadcaster.Id);
            if (_broadcasters.Count == 0)
                Subscribers = Array.Empty<IChangesBroadcaster>();
            else
                Subscribers = _broadcasters.Values.ToList();
        }

    }
    
    public class ChangesSubscribers
    {
        private readonly DbInstance _dbInstance;

        private readonly Dictionary<string, TableSubscriber> _broadcasters 
            = new Dictionary<string, TableSubscriber>();

        private readonly object _lockObject = new object();

        public ChangesSubscribers(DbInstance dbInstance)
        {
            _dbInstance = dbInstance;
        }

        public void Subscribe(IEnumerable<string> tables, IChangesBroadcaster broadcaster)
        {
            var tablesInstances = tables.Select(tableName =>
            {
                var tableInstance = _dbInstance.TryGetTable(tableName);

                if (tableInstance == null)
                    throw new Exception($"Table {tableName} is not found");

                return tableInstance;
            }).ToList();
            
            lock (_lockObject)
            {
                foreach (var table in tablesInstances)
                {
                    if (!_broadcasters.ContainsKey(table.Name))
                        _broadcasters.Add(table.Name, new TableSubscriber());
                    
                    _broadcasters[table.Name].Subscribe(broadcaster);

                    broadcaster.PublishInitTable(table);
                }
            }
        }

        public void Unsubscribe(IEnumerable<string> tables, IChangesBroadcaster broadcaster)
        {
            lock (_lockObject)
            {
                foreach (var tableName in tables)
                {
                    if (_broadcasters.ContainsKey(tableName))
                        _broadcasters[tableName].Unsubscribe(broadcaster);
                }
            }
        }


        private void ApplyAction(DbTable table, Action<IChangesBroadcaster> broadcaster)
        {
            lock (_lockObject)
            {
                if (!_broadcasters.ContainsKey(table.Name))
                    return;

                foreach (var subscriber in _broadcasters[table.Name].Subscribers)
                    broadcaster(subscriber);
            }
        }

        public void InitTable(DbTable table)
        {
            ApplyAction(table, broadcaster => broadcaster.PublishInitTable(table));
        }
        
        public void InitPartition(DbTable table, DbPartition dbPartition)
        {
            ApplyAction(table, broadcaster => broadcaster.PublishInitPartition(table, dbPartition));
        }

        public void UpdateRow(DbTable table, DbRow dbRow)
        {
            ApplyAction(table, broadcaster => broadcaster.SynchronizeUpdate(table, dbRow));
        }

        public void UpdateRows(DbTable table, IReadOnlyList<DbRow> dbRows)
        {
            ApplyAction(table, broadcaster => broadcaster.SynchronizeUpdate(table, dbRows));
        }

        public void DeleteRows(DbTable table, IReadOnlyList<DbRow> dbRows)
        {
            ApplyAction(table, broadcaster => broadcaster.SynchronizeDelete(table, dbRows));
        }


        public IEnumerable<IChangesBroadcaster> GetAll()
        {
            var result = new Dictionary<string, IChangesBroadcaster>();
            lock (_lockObject)
            {
                foreach (var subscriber in _broadcasters.Values)
                {
                    foreach (var broadcaster in subscriber.Subscribers)
                    {
                        if (!result.ContainsKey(broadcaster.Id))
                            result.Add(broadcaster.Id, broadcaster);
                    }
                }
            }

            return result.Values;
        }
    }
    

}