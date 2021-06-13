using System;
using System.Collections.Generic;
using System.Threading;

namespace MyNoSqlServer.Api.Nodes
{
    public static class NodesPool
    {
        private static Dictionary<string, ConnectedNode> _nodes = new ();

        private static object _lockObject = new();


        public static ConnectedNode GetOrCreateNode(string location)
        {
            lock (_lockObject)
            {
                var result = _nodes.TryGetValue(location, out var node) ?
                node : new ConnectedNode(location);
                
                result.LastAccess = DateTime.UtcNow;
                return result;

            }
            
        }

    }
    
}