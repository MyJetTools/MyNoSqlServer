using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains.Nodes
{
    public class NodeSessionsList
    {
        private readonly DbInstance _dbInstance;
        private readonly Dictionary<string, NodeSession> _sessionsByLocation = new();

        private IReadOnlyList<NodeSession> _sessions = Array.Empty<NodeSession>();

        private readonly object _lockObject = new();
        
        
        //ToDo - Put it to the Settings
        public static TimeSpan SessionTimeout = TimeSpan.FromSeconds(30);
        

        public NodeSessionsList(DbInstance dbInstance)
        {
            _dbInstance = dbInstance;
        }

        public NodeSession GetOrCreate(string location)
        {
            lock (_lockObject)
            {
                if (_sessionsByLocation.TryGetValue(location, out var result))
                    return result;

                result = new NodeSession(location, _dbInstance);

                _sessionsByLocation.Add(location, result);
                _sessions = _sessionsByLocation.Values.ToList();

                return result;
            }
        }


        public void NewEvent(ITransactionEvent @event)
        {
            foreach (var session in _sessions)
            {
                session.NewEvent(@event);
            }
        }

        public void SendPing()
        {
            foreach (var session in _sessions)
                session.SendPing();
        }

        private IReadOnlyList<NodeSession> GetSessionsToGc()
        {
            List<NodeSession> result = null;

            var now = DateTime.UtcNow;
            foreach (var session in _sessions)
            {

                if (now - session.LastAccessed > SessionTimeout)
                {
                    result ??= new List<NodeSession>();
                    result.Add(session);
                } 
            }

            return result;

        }


        public void Gc()
        {
            var sessionsToGc = GetSessionsToGc();
            
            if (sessionsToGc == null)
                return;

            var hasDisposed = false;

            lock (_lockObject)
            {
                foreach (var session in sessionsToGc)
                {
                    if (_sessionsByLocation.Remove(session.Location))
                        hasDisposed = true;
                    
                    session.Dispose();
                }

                if (hasDisposed)
                {
                    _sessions = _sessionsByLocation.Values.ToList();
                }
            }

        }

        public IReadOnlyList<NodeSession> GetAll()
        {
            return _sessions;
        }
    }
}