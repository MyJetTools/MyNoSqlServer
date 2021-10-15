using System;
using System.Collections.Generic;
using System.Linq;

namespace MyNoSqlServer.Domains.Logs
{

    public enum LogType
    {
        Info, Error
    }

    public class LogItem
    {
        public LogType LogType { get; internal set; }
        
        public DateTime DateTime { get; } = DateTime.UtcNow;
        public string Table { get; internal set; }
        
        public string Process { get; internal set; }
        public string Context { get; internal set; }
        public string Message { get; internal set; }
        public string StackTrace { get; internal set; }
        
    }
    
    public class AppLogs
    {

        private readonly List<LogItem> _items = new ();

        private readonly object _lockObject = new();

        private void Gc()
        {

            while (_items.Count > 100)
            {
                _items.RemoveAt(0);
            }
            
        }

        private void WriteToConsole(LogItem logItem)
        {
            Console.WriteLine(logItem.Table != null
                ? $"{logItem.DateTime:s}: [{logItem.LogType}] Table:{logItem.Table}"
                : $"{logItem.DateTime:s}: [{logItem.LogType}]");

            Console.WriteLine($"{logItem.DateTime:s}: [{logItem.LogType}] Process:{logItem.Process}");
            
            Console.WriteLine($"Context: {logItem.Context}");
            
            if (logItem.Message != null)
                Console.WriteLine($"Message: {logItem.Message}");
            
            
            if (logItem.StackTrace != null)
                Console.WriteLine($"StackTrace: {logItem.StackTrace}");
            
            Console.WriteLine("------------------------------");
        }

        public void WriteInfo(string table, string process,  string context, string message)
        {

            var item = new LogItem
            {
                LogType = LogType.Info,
                Table = table,
                Message = message,
                Context = context,
                Process = process
            };
            
            lock (_lockObject)
            {
                _items.Add(item);
                WriteToConsole(item);
                Gc();
            }
            
        }
        
        public void WriteError(string table, string process, string context, Exception ex)
        {

            var item = new LogItem
            {
                LogType = LogType.Info,
                Table = table,
                Message = ex.Message,
                Context = context,
                Process = process,
                StackTrace = ex.StackTrace
            };
            
            lock (_lockObject)
            {
                _items.Add(item);
                WriteToConsole(item);
                Gc();
            }
            
        }

        public IReadOnlyList<LogItem> GetAll()
        {
            lock (_lockObject)
                return _items.ToList();
        }
        
        public IReadOnlyList<LogItem> Get(string tableName)
        {
            lock (_lockObject)
                return _items.Where(itm => itm.Table == tableName).ToList();
        }

    }
}