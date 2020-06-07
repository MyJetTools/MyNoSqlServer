using System;
using System.Linq;
using Microsoft.AspNetCore.Mvc;

namespace MyNoSqlServer.Api.Controllers
{
    
    [ApiController]
    public class ApiController: Controller
    {

        private static readonly Lazy<object> Version = new Lazy<object>(() => new
        {
            version = ServiceLocator.Version,
            app = "MyNoSqlServer"
        });

        private static IActionResult _versionResult;
        
        [HttpGet("api/isalive")]
        public IActionResult IsAlive()
        {
            _versionResult ??= Json(Version.Value);
            return _versionResult;
        }

        [HttpGet("/api/status")]
        public IActionResult Status()
        {
            var connections = ServiceLocator.TcpServer.GetConnections();


            var result = connections.Cast<ChangesTcpService>().Select(itm =>
                new
                {
                    name = itm.ContextName,
                    ip = itm.TcpClient.Client.RemoteEndPoint.ToString(),
                    tables = itm.Tables,
                    connectedTime = itm.SocketStatistic.ConnectionTime.ToString("s"),
                    lastIncomingTime = itm.SocketStatistic.LastReceiveTime.ToString("s"),
                    id = itm.Id
                });
            
            
            return Json(result);
        }
        
    }
}