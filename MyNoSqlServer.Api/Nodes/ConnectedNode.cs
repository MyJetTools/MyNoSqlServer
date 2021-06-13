using System;

namespace MyNoSqlServer.Api.Nodes
{
    public class ConnectedNode
    {
        public string NodeName { get; }
        
        public DateTime LastAccess { get; internal set; }


        public ConnectedNode(string nodeName)
        {
            NodeName = nodeName;
            LastAccess = DateTime.UtcNow;
        }
    }
}