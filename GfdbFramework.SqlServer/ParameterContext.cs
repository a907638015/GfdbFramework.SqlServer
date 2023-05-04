using GfdbFramework.Core;
using GfdbFramework.Interface;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Text;

namespace GfdbFramework.SqlServer
{
    /// <summary>
    /// Sql Server 数据库实现参数化操作的上下文对象类。
    /// </summary>
    internal class ParameterContext : IParameterContext
    {
        private readonly Type _StringType = typeof(string);
        private Dictionary<object, SqlParameter> _Params = new Dictionary<object, SqlParameter>();

        /// <summary>
        /// 使用一个标识上下文是否应当启用参数化操作的值初始化一个新的 <see cref="ParameterContext"/> 类对象。
        /// </summary>
        /// <param name="enableParametric">是否应当启用参数化操作。</param>
        internal ParameterContext(bool enableParametric)
        {
            EnableParametric = enableParametric;
        }

        /// <summary>
        /// 添加一个常量值到参数上下文中。
        /// </summary>
        /// <param name="value">待添加到参数上下文中的常量值。</param>
        /// <returns>添加到参数中后所使用的参数名。</returns>
        public string Add(object value)
        {
            if (value == null)
                value = DBNull.Value;

            if (EnableParametric)
            {
                if (_Params.TryGetValue(value, out SqlParameter parameter))
                {
                    if (parameter.ParameterName.StartsWith("@"))
                        return parameter.ParameterName;
                    else
                        return $"@{parameter.ParameterName}";
                }
                else
                {
                    int index = _Params.Count;

                    _Params.Add(value, new SqlParameter($"P{index}", value));

                    return $"@P{index}";
                }
            }
            else if (value == null)
            {
                return "null";
            }
            else if (value.GetType() == _StringType)
            {
                return $"'{((string)value).Replace("'", "''")}'";
            }
            else
            {
                return value.ToString();
            }
        }

        /// <summary>
        /// 释放当前上下文对象所占用的资源信息。
        /// </summary>
        public void Dispose()
        {
            _Params = null;
        }

        /// <summary>
        /// 将当前参数上下文对象转换成对应的参数集合。
        /// </summary>
        /// <returns>转换后对应的参数集合对象。</returns>
        public ReadOnlyList<DbParameter> ToList()
        {
            return new ReadOnlyList<DbParameter>(_Params.Values);
        }

        /// <summary>
        /// 获取一个值，该值指示当前上下文是否应当开启参数化操作。
        /// </summary>
        public bool EnableParametric { get; }
    }
}
