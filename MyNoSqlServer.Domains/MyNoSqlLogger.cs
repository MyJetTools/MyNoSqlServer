using System;
using System.Collections.Generic;
using System.Linq;

namespace MyNoSqlServer.Domains
{

    public enum LogLevel
    {
        Info, Error,
    }

    public class LogItem
    {
        public DateTimeOffset DateTime { get; set; } = DateTimeOffset.UtcNow;
        
        public LogLevel Level { get; set; }
        public string Process { get; set; }
        public string Message { get; set; }
        public string StackTrace { get; set; }
    }

    public class MyNoSqlLogger
    {
        private LinkedList<LogItem> _items = new LinkedList<LogItem>();


        public long SnapshotId { get; private set; }


        private void Add(LogItem item)
        {
            lock (_items)
            {
                SnapshotId++;
                _items.AddLast(item);

                while (_items.Count > 100)
                {
                    _items.RemoveFirst();
                }
            }
        }

        public void WriteInfo(string process, string message)
        {
            var item = new LogItem
            {
                Process = process,
                Message = message,
                Level = LogLevel.Info
            };

            Add(item);
        }

        public void WriteError(string process, Exception e)
        {
            var item = new LogItem
            {
                Process = process,
                Message = e.Message,
                Level = LogLevel.Error,
                StackTrace = e.StackTrace
            };

            Add(item);
        }


        public IReadOnlyList<LogItem> GetAll()
        {
            lock (_items)
            {
                return _items.ToList();
            }
        }

    }
}