using System;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.Domains.SnapshotSaver.Implementation
{
    public static class DequeueTimeCalculator
    {


        private static int GetSec(int sec, int secInterval)
        {

            for (var i = 0; i < 60; i += secInterval)
            {
                if (sec < i)
                    return i;
            }

            return 0;
        }


        //Todo - Write Unit Tests
        public static DateTime GetDequeueTime(this DateTime now, DataSynchronizationPeriod period)
        {
            switch (period)
            {
                case DataSynchronizationPeriod.Asap:
                    return now;
                
                case DataSynchronizationPeriod.Immediately:
                    return now;
                
                case DataSynchronizationPeriod.Sec1:
                    return new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second+1);
                
                
                case DataSynchronizationPeriod.Sec5:
                    var sec5 = GetSec(now.Second, 5);
                    var min5 = sec5 == 0 ? now.Minute+1 : now.Minute;
                    return new DateTime(now.Year, now.Month, now.Day, now.Hour, min5, sec5);
                
                
                case DataSynchronizationPeriod.Sec15:
                    var sec15 = GetSec(now.Second, 15);
                    var min15 = sec15 == 0 ? now.Minute+1 : now.Minute ;
                    return new DateTime(now.Year, now.Month, now.Day, now.Hour, min15, sec15);
                
                
                case DataSynchronizationPeriod.Sec30:
                    var sec30 = now.Second < 30 ? 30 : 0;
                    var min30 = sec30 == 0 ? now.Minute + 1 : now.Minute;
                    return new DateTime(now.Year, now.Month, now.Day, now.Hour, min30, sec30);
                
                case DataSynchronizationPeriod.Min1:
                    return new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute+1, 0);
                    
            }

            return now;
        }
        
    }
}