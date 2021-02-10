using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Api.Hubs;
using MyNoSqlServer.Api.Models;

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

            var signalRConnections = ChangesHub.Connections.Get();
            
            var result = new List<UiModel>();

            var dt = DateTime.UtcNow;


            result.AddRange(connections.Cast<ChangesTcpService>().Select(UiModel.Create));
            
            
            result.AddRange(signalRConnections.Select(UiModel.Create));
            
            
            
            return Json(result.OrderBy(iym => iym.Id));
        }
        
    }
}