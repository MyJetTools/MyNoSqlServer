using System;
using System.Collections.Generic;

namespace MyNoSqlServer.Api.Nodes
{
    public static class NodesPool
    {
        private static readonly Dictionary<string, ConnectedNode> Nodes = new ();

        private static readonly object LockObject = new();


        public static ConnectedNode GetOrCreateNode(string location)
        {
            lock (LockObject)
            {
                var result = Nodes.TryGetValue(location, out var node) ?
                node : new ConnectedNode(location);
                
                result.LastAccess = DateTime.UtcNow;
                return result;

            }
            
        }

    }
    
}