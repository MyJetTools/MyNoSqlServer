using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Api.Hubs
{
    public static class HubUtils
    {
        public static Task SendTableNotFoundAsync(this IClientProxy clientProxy, string corrId)
        {
            return clientProxy.SendAsync("TableNotFound", corrId);
        }

        public static Task SendRowNotFoundAsync(this IClientProxy clientProxy, string corrId)
        {
            return clientProxy.SendAsync("RowNotFound", corrId);
        }

        public static Task SendRowAsync(this IClientProxy clientProxy, string corrId, DbRow dbRow)
        {
            return clientProxy.SendAsync("Row", corrId, dbRow.Data);
        }
        
        public static Task SendEmptyRowsAsync(this IClientProxy clientProxy, string corrId)
        {
            return clientProxy.SendRowsAsync(corrId, Array.Empty<byte>());
        }

        public static Task SendRowsAsync(this IClientProxy clientProxy, string corrId, byte[] data)
        {
            return clientProxy.SendAsync("Rows", corrId, data);
        }


    }
}