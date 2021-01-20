using System;
using System.Linq;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Api.Tcp;

namespace MyNoSqlServer.Api.Controllers
{
    
    [ApiController]
    public class ApiController: Controller
    {

        private static readonly Lazy<object> Version = new Lazy<object>(() => new
        {
            Name = ServiceLocator.AppName,
            Version = ServiceLocator.AppVersion,
            ServiceLocator.StartedAt,
            ServiceLocator.Host,
            Environment = ServiceLocator.AspNetEnvironment
        });

        private static IActionResult _isAliveResult;
        
        [HttpGet("api/isalive")]
        public IActionResult IsAlive()
        {
            _isAliveResult ??= Json(Version.Value);
            return _isAliveResult;
        }

        [HttpGet("/api/status")]
        public IActionResult Status()
        {
            var connections = ServiceLocator.TcpServer.GetConnections();

            var dt = DateTime.UtcNow;

            var result = connections.Cast<ChangesTcpService>().Select(itm =>
                new
                {
                    name = itm.ContextName,
                    ip = itm.TcpClient.Client.RemoteEndPoint.ToString(),
                    tables = itm.Tables,
                    connectedTime = (dt - itm.SocketStatistic.ConnectionTime).ToString("g"),
                    lastIncomingTime = (dt -itm.SocketStatistic.LastReceiveTime).ToString("g"),
                    id = itm.Id
                }).OrderBy(iym => iym.id);
            
            return Json(result);
        }
        
    }
}