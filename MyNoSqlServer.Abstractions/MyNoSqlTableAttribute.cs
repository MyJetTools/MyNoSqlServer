using System;

namespace MyNoSqlServer.Abstractions
{
    public class MyNoSqlTableAttribute : Attribute
    {

        public MyNoSqlTableAttribute(string tableName)
        {
            TableName = tableName;
        }
        
        public string TableName { get; }
    }
    
}