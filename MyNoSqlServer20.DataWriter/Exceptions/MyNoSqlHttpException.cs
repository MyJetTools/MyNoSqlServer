using System;

namespace MyNoSqlServer20.DataWriter.Exceptions
{
    public class MyNoSqlHttpException : Exception
    {
        public MyNoSqlHttpException(string message, Exception inner) : base(message, inner)
        {
        
        }
    }
}