using System.Text;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Domains.Logs;

namespace MyNoSqlServer.Api.Controllers
{
    [ApiController]
    public class LogsController :Controller
    {

        [HttpGet("/Logs")]
        public string GetLogs()
        {
            var items = ServiceLocator.AppLogs.GetAll();

            var result = new StringBuilder();

            foreach (var item in items)
            {
                if (item.LogType == LogType.Info)
                {
                    result.AppendLine(item.DateTime.ToString("s") + " INFO");
                    result.AppendLine("Process: "+item.Process);
                    result.AppendLine("Msg: "+item.Message);
                    result.AppendLine("------------------------------");
                }
                
                if (item.LogType == LogType.Error)
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
        
        [HttpGet("/Logs/{tableName}")]
        public string GetLogsByTable([FromQuery]string tableName)
        {
            var items = ServiceLocator.AppLogs.Get(tableName);

            var result = new StringBuilder();

            foreach (var item in items)
            {
                if (item.LogType == LogType.Info)
                {
                    result.AppendLine(item.DateTime.ToString("s") + " INFO");
                    result.AppendLine("Process: "+item.Process);
                    result.AppendLine("Msg: "+item.Message);
                    result.AppendLine("------------------------------");
                }
                
                if (item.LogType == LogType.Error)
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