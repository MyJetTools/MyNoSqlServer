using System.Text;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Domains;

namespace MyNoSqlServer.Api.Controllers
{
    [ApiController]
    public class LogsController :Controller
    {

        [HttpGet("/Logs")]
        public string GetLogs()
        {
            var items = ServiceLocator.Logger.GetAll();

            var result = new StringBuilder();

            foreach (var item in items)
            {
                if (item.Level == LogLevel.Info)
                {
                    result.AppendLine(item.DateTime.ToString("s") + " INFO");
                    result.AppendLine("Process: "+item.Process);
                    result.AppendLine("Msg: "+item.Message);
                    result.AppendLine("------------------------------");
                }
                
                if (item.Level == LogLevel.Error)
                {
                    result.AppendLine(item.DateTime.ToString("s") + " ERROR");
                    result.AppendLine("Process: "+item.Process);
                    result.AppendLine("Msg: "+item.Message);
                    result.AppendLine("Stacktrace: ");
                    result.AppendLine(item.StackTrace);
                    result.AppendLine("------------------------------");
                }
            }

            return result.ToString();
        }
        
    }
}