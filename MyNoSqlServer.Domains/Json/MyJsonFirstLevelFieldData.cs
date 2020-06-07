using System;
using System.Text;

namespace MyNoSqlServer.Domains.Json
{

    public interface IJsonFirstLine
    {
        ReadOnlyMemory<byte> Name { get; }
        ReadOnlyMemory<byte> Value { get; }
    }
    
    public class MyJsonFirstLevelFieldData :  IJsonFirstLine
    {
        public MyJsonFirstLevelFieldData(in ReadOnlyMemory<byte> name, in ReadOnlyMemory<byte> value)
        {
            _nameAsMemory = name;
            Value = value;
        }
        
        public MyJsonFirstLevelFieldData(string name, string value)
        {
            _name = name;
            _nameAsMemory = name.ToJsonReadOnlyMemory();
            Value = value.ToJsonReadOnlyMemory();
        }

        private string _name;

        public string Name => _name ??= _nameAsMemory.AsJsonString();



        private readonly ReadOnlyMemory<byte> _nameAsMemory;

        ReadOnlyMemory<byte> IJsonFirstLine.Name => _nameAsMemory;
        
        public ReadOnlyMemory<byte> Value { get; }

        public override string ToString()
        {
            return Name+":"+Encoding.ASCII.GetString(Value.Span);
        }
    }

}