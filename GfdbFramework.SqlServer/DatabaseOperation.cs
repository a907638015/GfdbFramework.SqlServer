using GfdbFramework.Core;
using GfdbFramework.DataSource;
using GfdbFramework.Enum;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Text;

namespace GfdbFramework.SqlServer
{
    /// <summary>
    /// Sql Server 数据库操作类。
    /// </summary>
    public class DatabaseOperation : Interface.IDatabaseOperation
    {
        private readonly string _ConnectionString = null;
        private SqlConnection _Connection = null;
        private OpenedMode _OpenedMode = OpenedMode.Auto;
        private SqlCommand _Command = null;
        private SqlTransaction _Transaction = null;

        /// <summary>
        /// 使用指定的连接字符串初始化一个新的 <see cref="DatabaseOperation"/> 类实例。
        /// </summary>
        /// <param name="connectionString">数据库连接字符串。</param>
        public DatabaseOperation(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new Exception($"初始化一个 GfdbFramework.SqlServer.DatabaseOperation 对象时，参数 {nameof(connectionString)} 不能为空或纯空白字符串");

            _ConnectionString = connectionString;
        }

        /// <summary>
        /// 获取当前对象中的数据库连接打开方式。
        /// </summary>
        public OpenedMode OpenedMode
        {
            get
            {
                return _OpenedMode;
            }
        }

        /// <summary>
        /// 开启事务执行模式。
        /// </summary>
        public void BeginTransaction()
        {
            OpenConnection(OpenedMode.Transaction);

            _Transaction = _Connection.BeginTransaction();
        }

        /// <summary>
        /// 开启事务执行模式。
        /// </summary>
        /// <param name="level">事务级别。</param>
        public void BeginTransaction(IsolationLevel level)
        {
            OpenConnection(OpenedMode.Transaction);

            _Transaction = _Connection.BeginTransaction(level);
        }

        /// <summary>
        /// 关闭数据库的连接通道。
        /// </summary>
        /// <returns>关闭成功返回 true，否则返回 false。</returns>
        public bool CloseConnection()
        {
            return CloseConnection(OpenedMode.Manual);
        }

        /// <summary>
        /// 关闭数据库的连接通道（此方法不建议在框架外部调用，外部要手动关闭连接直接调用 <see cref="CloseConnection()"/> 方法即可，无需传递允许关闭的连接打开模式。
        /// </summary>
        /// <param name="openedMode">允许关闭的连接打开模式（当打开模式优先级等于低于允许的打开模式时都应该关闭）。</param>
        /// <returns>关闭成功返回 true，否则返回 false。</returns>
        public bool CloseConnection(OpenedMode openedMode)
        {
            if (_Connection != null)
            {
                if (openedMode >= _OpenedMode)
                {
                    if (_Connection.State != ConnectionState.Closed)
                        _Connection.Close();

                    _OpenedMode = OpenedMode.Auto;

                    return true;
                }

                return false;
            }

            return true;
        }

        /// <summary>
        /// 提交当前事务中的所有操作。
        /// </summary>
        public void CommitTransaction()
        {
            if (_Transaction != null)
                _Transaction.Commit();

            CloseConnection(OpenedMode.Transaction);

            _Transaction = null;
        }

        /// <summary>
        /// 释放当前数据库操作对象所占用的资源信息。
        /// </summary>
        public void Dispose()
        {
            _Transaction?.Rollback();
            _Transaction?.Dispose();
            _Command?.Dispose();
            _Connection?.Dispose();

            _Transaction = null;
            _Command = null;
            _Connection = null;
        }

        /// <summary>
        /// 执行指定的 Sql 语句并返回执行该语句所受影响的数据行数。
        /// </summary>
        /// <param name="sql">待执行的 Sql 语句。</param>
        /// <param name="sqlType">待执行语句的命令类型。</param>
        /// <param name="parameters">执行该命令语句所需的参数集合。</param>
        /// <param name="autoincrementValue">执行插入数据命令时最后一条插入语句所插入的自动增长字段值。</param>
        /// <returns>执行 <paramref name="sql"/> 参数对应语句所影响的数据行数。</returns>
        public int ExecuteNonQuery(string sql, CommandType sqlType, ReadOnlyList<DbParameter> parameters, out long autoincrementValue)
        {
            return ExecuteNonQuery(sql, sqlType, parameters, false, out autoincrementValue);
        }

        /// <summary>
        /// 执行指定的 Sql 语句并返回执行该语句所受影响的数据行数。
        /// </summary>
        /// <param name="sql">待执行的 Sql 语句。</param>
        /// <param name="sqlType">待执行语句的命令类型。</param>
        /// <param name="parameters">执行该命令语句所需的参数集合。</param>
        /// <returns>执行 <paramref name="sql"/> 参数对应语句所影响的数据行数。</returns>
        public int ExecuteNonQuery(string sql, CommandType sqlType, ReadOnlyList<DbParameter> parameters)
        {
            return ExecuteNonQuery(sql, sqlType, parameters, true, out _);
        }

        /// <summary>
        /// 执行指定的 Sql 语句并返回执行该语句所受影响的数据行数。
        /// </summary>
        /// <param name="sql">待执行的 Sql 语句。</param>
        /// <param name="sqlType">待执行语句的命令类型。</param>
        /// <param name="parameters">执行该命令语句所需的参数集合。</param>
        /// <param name="ignoreAutoincrementValue">是否忽略自增长字段的值。</param>
        /// <param name="autoincrementValue">执行插入数据命令时最后一条插入语句所插入的自动增长字段值。</param>
        /// <returns>执行 <paramref name="sql"/> 参数对应语句所影响的数据行数。</returns>
        private int ExecuteNonQuery(string sql, CommandType sqlType, ReadOnlyList<DbParameter> parameters, bool ignoreAutoincrementValue, out long autoincrementValue)
        {
            autoincrementValue = default;

            OpenConnection(OpenedMode.Auto);

            InitCommand(sql, sqlType, parameters);

            try
            {
                int result = _Command.ExecuteNonQuery();

                if (!ignoreAutoincrementValue)
                {
                    InitCommand("select @@identity", CommandType.Text, null);

                    object value = _Command.ExecuteScalar();

                    if (value == null || value == DBNull.Value)
                        throw new Exception($"在执行指定 Sql 语句后未能获取到自增字段的值，具体 Sql 语句为：{sql}");

                    if (value is int intValue)
                        autoincrementValue = intValue;
                    else if (value is long longValue)
                        autoincrementValue = longValue;
                    else if (value is short shortValue)
                        autoincrementValue = shortValue;
                    else if (value is byte byteValue)
                        autoincrementValue = byteValue;
                    else if (value is uint uintValue)
                        autoincrementValue = uintValue;
                    else if (value is ulong ulongValue)
                        autoincrementValue = (long)ulongValue;
                    else if (value is ushort ushortValue)
                        autoincrementValue = ushortValue;
                    else if (value is sbyte sbyteValue)
                        autoincrementValue = sbyteValue;
                    else if (value is decimal decimalValue)
                        autoincrementValue = (long)decimalValue;
                    else if (value is double doubleValue)
                        autoincrementValue = (long)doubleValue;
                    else if (value is float floatValue)
                        autoincrementValue = (long)floatValue;
                    else
                        throw new Exception(string.Format("在执行指定命令后获取到自增字段的值类型不正确，具体命令为：{0}，获取到的自增字段值为：{1}", sql, value));
                }

                return result;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                CloseConnection(OpenedMode.Auto);
            }
        }

        /// <summary>
        /// 执行指定命令语句并将结果集中每一行数据转交与处理函数处理。
        /// </summary>
        /// <param name="sql">待执行的 Sql 语句。</param>
        /// <param name="sqlType">待执行语句的命令类型。</param>
        /// <param name="parameters">执行该命令语句所需的参数集合。</param>
        /// <param name="readerHandler">处理结果集中每一行数据的处理函数（若该函数返回 false 则忽略后续的数据行不再回调此处理函数）。</param>
        public void ExecuteReader(string sql, CommandType sqlType, ReadOnlyList<DbParameter> parameters, Func<DbDataReader, bool> readerHandler)
        {
            OpenConnection(OpenedMode.Auto);

            InitCommand(sql, sqlType, parameters);

            try
            {
                SqlDataReader dataReader = _Command.ExecuteReader();

                while (dataReader.Read() && readerHandler(dataReader)) { }

                dataReader.Close();
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                CloseConnection(OpenedMode.Auto);
            }
        }

        /// <summary>
        /// 执行指定的 Sql 语句并返回结果集中的第一行第一列值返回。
        /// </summary>
        /// <param name="sql">待执行的 Sql 语句。</param>
        /// <param name="sqlType">待执行语句的命令类型。</param>
        /// <param name="parameters">执行该命令所需的参数集合。</param>
        /// <returns>执行 <paramref name="sql"/> 参数对应语句得到的结果集中第一行第一列的值。</returns>
        public object ExecuteScalar(string sql, CommandType sqlType, ReadOnlyList<DbParameter> parameters)
        {
            OpenConnection(OpenedMode.Auto);

            InitCommand(sql, sqlType, parameters);

            try
            {
                return _Command.ExecuteScalar();
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                CloseConnection(OpenedMode.Auto);
            }
        }

        /// <summary>
        /// 打开数据库的连接通道。
        /// </summary>
        /// <returns>打开成功返回 true，否则返回 false。</returns>
        public bool OpenConnection()
        {
            return OpenConnection(OpenedMode.Manual);
        }

        /// <summary>
        /// 打开数据库的连接通道（此方法不建议在框架外部调用，外部要手动打开连接直接调用 <see cref="OpenConnection()"/> 方法即可，无需传递连接打开方式）。
        /// </summary>
        /// <param name="openedMode">连接打开方式。</param>
        /// <returns>打开成功返回 true，否则返回 false。</returns>
        public bool OpenConnection(OpenedMode openedMode)
        {
            if (_Connection == null)
                _Connection = new SqlConnection(_ConnectionString);

            if (_Connection.State == ConnectionState.Closed)
                _Connection.Open();

            if (openedMode > _OpenedMode)
                _OpenedMode = openedMode;

            return true;
        }

        /// <summary>
        /// 回滚当前事务中的所有操作。
        /// </summary>
        public void RollbackTransaction()
        {
            if (_Transaction != null)
                _Transaction.Rollback();

            CloseConnection(OpenedMode.Transaction);

            _Transaction = null;
        }

        /// <summary>
        /// 回滚当前事务到指定保存点或回滚指定事务。
        /// </summary>
        /// <param name="pointName">要回滚的保存点名称或事务名称。</param>
        public void RollbackTransaction(string pointName)
        {
            if (_Transaction != null)
                _Transaction.Rollback(pointName);
        }

        /// <summary>
        /// 在当前事务模式下保存一个事务回滚点。
        /// </summary>
        /// <param name="pointName">回滚点名称</param>
        public void SaveTransaction(string pointName)
        {
            if (_Transaction != null)
                _Transaction.Save(pointName);
        }

        /// <summary>
        /// 初始化执行命令对象。
        /// </summary>
        /// <param name="commandText">待执行的命令语句。</param>
        /// <param name="commandType">待执行语句的命令类型。</param>
        /// <param name="parameters">执行该语句所需的参数集合。</param>
        private void InitCommand(string commandText, CommandType commandType, ReadOnlyList<DbParameter> parameters)
        {
            if (_Command == null)
                _Command = new SqlCommand(commandText, _Connection, _Transaction);
            else
                _Command.CommandText = commandText;

            _Command.CommandType = commandType;
            _Command.Parameters.Clear();

            if (parameters != null)
            {
                foreach (var item in parameters)
                {
                    _Command.Parameters.Add((SqlParameter)item);
                }
            }
        }
    }
}
