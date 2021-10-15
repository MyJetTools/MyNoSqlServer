using System;
using Microsoft.AspNetCore.Mvc;

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

        
    }
}