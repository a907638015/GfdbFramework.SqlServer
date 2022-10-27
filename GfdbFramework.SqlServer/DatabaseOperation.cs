using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using GfdbFramework.Core;
using GfdbFramework.DataSource;
using GfdbFramework.Enum;
using GfdbFramework.Interface;

namespace GfdbFramework.SqlServer
{
    /// <summary>
    /// Sql Server 数据库操作类。
    /// </summary>
    public class DatabaseOperation : IDatabaseOperation
    {
        private readonly string _ConnectionString = null;
        private SqlConnection _Connection = null;
        private ConnectionOpenedMode _OpenedMode = ConnectionOpenedMode.Auto;
        private SqlCommand _Command = null;
        private SqlTransaction _Transaction = null;

        /// <summary>
        /// 获取当前对象中的数据库连接打开方式。
        /// </summary>
        public ConnectionOpenedMode OpenedMode
        {
            get
            {
                return _OpenedMode;
            }
        }

        /// <summary>
        /// 使用指定的连接字符串初始化一个新的 <see cref="DatabaseOperation"/> 类实例。
        /// </summary>
        /// <param name="connectionString">数据库连接字符串。</param>
        public DatabaseOperation(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new Exception(string.Format("初始化一个 GfdbFramework.SqlServer.DatabaseOperation 对象时，参数 {0} 不能为空或纯空白字符串", nameof(connectionString)));

            _ConnectionString = connectionString;
        }

        /// <summary>
        /// 执行指定的命令语句并返回执行该语句所受影响的数据行数。
        /// </summary>
        /// <param name="commandText">待执行的命令语句。</param>
        /// <param name="commandType">待执行语句的命令类型。</param>
        /// <param name="parameters">执行该命令语句所需的参数集合。</param>
        /// <param name="ignoreAutoincrementValue">是否忽略自增长字段的值。</param>
        /// <param name="autoincrementValue">执行插入数据命令时插入自动增长字段的值。</param>
        /// <returns>执行 <paramref name="commandText"/> 参数对应语句所影响的数据行数。</returns>
        private int ExecuteNonQuery(string commandText, CommandType commandType, Interface.IReadOnlyList<DbParameter> parameters, bool ignoreAutoincrementValue, out long autoincrementValue)
        {
            autoincrementValue = default;

            InitCommand(commandText, commandType, parameters);

            OpenConnection(ConnectionOpenedMode.Auto);

            try
            {
                int result = _Command.ExecuteNonQuery();

                if (!ignoreAutoincrementValue)
                {
                    InitCommand("select @@identity", CommandType.Text, null);

                    object value = _Command.ExecuteScalar();

                    if (value == null || value == DBNull.Value)
                        throw new Exception(string.Format("在执行指定命令后未能获取到自增字段的值，具体命令为：{0}", commandText));

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
                        throw new Exception(string.Format("在执行指定命令后获取到自增字段的值类型不正确，具体命令为：{0}，获取到的自增字段值为：{1}", commandText, value));
                }

                return result;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                CloseConnection(ConnectionOpenedMode.Auto);
            }
        }

        /// <summary>
        /// 执行指定的命令语句并返回执行该语句所受影响的数据行数。
        /// </summary>
        /// <param name="commandText">待执行的命令语句。</param>
        /// <param name="commandType">待执行语句的命令类型。</param>
        /// <param name="parameters">执行该命令语句所需的参数集合。</param>
        /// <param name="autoincrementValue">执行插入数据命令时插入自动增长字段的值。</param>
        /// <returns>执行 <paramref name="commandText"/> 参数对应语句所影响的数据行数。</returns>
        public int ExecuteNonQuery(string commandText, CommandType commandType, Interface.IReadOnlyList<DbParameter> parameters, out long autoincrementValue)
        {
            return ExecuteNonQuery(commandText, commandType, parameters, false, out autoincrementValue);
        }

        /// <summary>
        /// 执行指定的命令语句并返回执行该语句所受影响的数据行数。
        /// </summary>
        /// <param name="commandText">待执行的命令语句。</param>
        /// <param name="commandType">待执行语句的命令类型。</param>
        /// <param name="parameters">执行该命令语句所需的参数集合。</param>
        /// <returns>执行 <paramref name="commandText"/> 参数对应语句所影响的数据行数。</returns>
        public int ExecuteNonQuery(string commandText, CommandType commandType, Interface.IReadOnlyList<DbParameter> parameters)
        {
            return ExecuteNonQuery(commandText, commandType, parameters, true, out _);
        }

        /// <summary>
        /// 执行指定的 Sql 语句并返回执行该语句所受影响的数据行数。
        /// </summary>
        /// <param name="sql">待执行的 Sql 语句。</param>
        /// <param name="parameters">执行该 Sql 语句所需的参数集合。</param>
        /// <returns>执行 <paramref name="sql"/> 参数对应 Sql 语句所影响的数据行数。</returns>
        public int ExecuteNonQuery(string sql, Interface.IReadOnlyList<DbParameter> parameters)
        {
            return ExecuteNonQuery(sql, CommandType.Text, parameters);
        }

        /// <summary>
        /// 执行指定的 Sql 语句并返回执行该语句所受影响的数据行数。
        /// </summary>
        /// <param name="sql">待执行的 Sql 语句。</param>
        /// <returns>执行 <paramref name="sql"/> 参数对应 Sql 语句所影响的数据行数。</returns>
        public int ExecuteNonQuery(string sql)
        {
            return ExecuteNonQuery(sql, null);
        }

        /// <summary>
        /// 执行指定的 Sql 语句并返回结果集中的第一行第一列值返回。
        /// </summary>
        /// <param name="sql">待执行的 Sql 语句。</param>
        /// <returns>执行该 Sql 得到的结果集中第一行第一列的值。</returns>
        public object ExecuteScalar(string sql)
        {
            return ExecuteScalar(sql, null);
        }

        /// <summary>
        /// 执行指定的 Sql 语句并返回结果集中的第一行第一列值返回。
        /// </summary>
        /// <param name="sql">待执行的 Sql 语句。</param>
        /// <param name="parameters">执行该 Sql 语句所需的参数集合。</param>
        /// <returns>执行该 Sql 得到的结果集中第一行第一列的值。</returns>
        public object ExecuteScalar(string sql, Interface.IReadOnlyList<DbParameter> parameters)
        {
            return ExecuteScalar(sql, CommandType.Text, parameters);
        }

        /// <summary>
        /// 执行指定命令语句并返回结果集中的第一行第一列值返回。
        /// </summary>
        /// <param name="commandText">待执行的命令语句。</param>
        /// <param name="commandType">待执行语句的命令类型。</param>
        /// <param name="parameters">执行该命令所需的参数集合。</param>
        /// <returns>执行该命令得到的结果集中第一行第一列的值。</returns>
        public object ExecuteScalar(string commandText, CommandType commandType, Interface.IReadOnlyList<DbParameter> parameters)
        {
            InitCommand(commandText, commandType, parameters);

            OpenConnection(ConnectionOpenedMode.Auto);

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
                CloseConnection(ConnectionOpenedMode.Auto);
            }
        }

        /// <summary>
        /// 执行指定的 Sql 语句并将结果集中每一行数据转交与处理函数处理。
        /// </summary>
        /// <param name="sql">待执行的 Sql 语句。</param>
        /// <param name="readerHandler">处理结果集中每一行数据的处理函数（若该函数返回 false 则忽略后续的数据行不再回调此处理函数）。</param>
        public void ExecuteReader(string sql, Func<DbDataReader, bool> readerHandler)
        {
            ExecuteReader(sql, null, readerHandler);
        }

        /// <summary>
        /// 执行指定的 Sql 语句并将结果集中每一行数据转交与处理函数处理。
        /// </summary>
        /// <param name="sql">待执行的 Sql 语句。</param>
        /// <param name="parameters">执行该 Sql 语句所需的参数集合。</param>
        /// <param name="readerHandler">处理结果集中每一行数据的处理函数（若该函数返回 false 则忽略后续的数据行不再回调此处理函数）。</param>
        public void ExecuteReader(string sql, Interface.IReadOnlyList<DbParameter> parameters, Func<DbDataReader, bool> readerHandler)
        {
            ExecuteReader(sql, CommandType.Text, parameters, readerHandler);
        }

        /// <summary>
        /// 执行指定命令语句并将结果集中每一行数据转交与处理函数处理。
        /// </summary>
        /// <param name="commandText">待执行的命令语句。</param>
        /// <param name="commandType">待执行语句的命令类型。</param>
        /// <param name="parameters">执行该命令语句所需的参数集合。</param>
        /// <param name="readerHandler">处理结果集中每一行数据的处理函数（若该函数返回 false 则忽略后续的数据行不再回调此处理函数）。</param>
        public void ExecuteReader(string commandText, CommandType commandType, Interface.IReadOnlyList<DbParameter> parameters, Func<DbDataReader, bool> readerHandler)
        {
            InitCommand(commandText, commandType, parameters);

            OpenConnection(ConnectionOpenedMode.Auto);

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
                CloseConnection(ConnectionOpenedMode.Auto);
            }
        }

        /// <summary>
        /// 创建数据库（该操作为独立操作，不受上下文控制，即不受事务、数据库开关连接等操作影响）。
        /// </summary>
        /// <param name="databaseInfo">待创建数据库的信息。</param>
        /// <returns>创建成功返回 true，否则返回 false。</returns>
        public bool CreateDatabase(DatabaseInfo databaseInfo)
        {
            if (databaseInfo == null)
                throw new ArgumentNullException(nameof(databaseInfo), "创建数据库时参数不能为 null");

            if (string.IsNullOrWhiteSpace(databaseInfo.Name))
                throw new ArgumentException("创建数据库时数据库名称不能为空");

            SqlConnection connection = GetMasterConnection();

            SqlCommand command = new SqlCommand(new SqlFactory().GenerateCreateDatabaseSql(databaseInfo), connection);

            connection.Open();

            command.ExecuteNonQuery();

            connection.Close();

            command.Dispose();

            return true;
        }

        /// <summary>
        /// 创建数据表。
        /// </summary>
        /// <param name="dataSource">带创建数据表对应的源信息。</param>
        /// <returns>创建成功返回 true，否则返回 false。</returns>
        public bool CreateTable(OriginalDataSource dataSource)
        {
            OpenConnection(ConnectionOpenedMode.Auto);

            InitCommand(new SqlFactory().GenerateCreateTableSql(dataSource), CommandType.Text, null);

            _Command.ExecuteNonQuery();

            CloseConnection(ConnectionOpenedMode.Auto);

            return true;
        }

        /// <summary>
        /// 校验指定的数据库是否存在。
        /// </summary>
        /// <param name="databaseName">需要确认是否存在的数据库名称。</param>
        /// <returns>若该数据库已存在则返回 true，否则返回 false。</returns>
        public bool ExistsDatabase(string databaseName)
        {
            SqlConnection connection = GetMasterConnection();

            SqlCommand command = new SqlCommand("select count(1) from master..sysdatabases where name = @databaseName", connection);

            command.Parameters.Add(new SqlParameter("databaseName", databaseName));

            connection.Open();

            bool result = (int)command.ExecuteScalar() == 1;

            connection.Close();

            command.Dispose();

            return result;
        }

        /// <summary>
        /// 校验指定的数据表是否存在。
        /// </summary>
        /// <param name="tableName">需要确认是否存在的数据表名称。</param>
        /// <returns>若该数据表已存在则返回 true，否则返回 false。</returns>
        public bool ExistsTable(string tableName)
        {
            object result = ExecuteScalar($"select object_id(N'@tableName', N'U')", new Realize.ReadOnlyList<DbParameter>(new SqlParameter("tableName", tableName)));

            return result != null && result != DBNull.Value;
        }

        /// <summary>
        /// 打开数据库的连接通道。
        /// </summary>
        /// <returns>打开成功返回 true，否则返回 false。</returns>
        public bool OpenConnection()
        {
            return OpenConnection(ConnectionOpenedMode.Manual);
        }

        /// <summary>
        /// 关闭数据库的连接通道。
        /// </summary>
        /// <returns>关闭成功返回 true，否则返回 false。</returns>
        public bool CloseConnection()
        {
            return CloseConnection(ConnectionOpenedMode.Manual);
        }

        /// <summary>
        /// 初始化执行命令对象。
        /// </summary>
        /// <param name="commandText">待执行的命令语句。</param>
        /// <param name="commandType">待执行语句的命令类型。</param>
        /// <param name="parameters">执行该语句所需的参数集合。</param>
        private void InitCommand(string commandText, CommandType commandType, Interface.IReadOnlyList<DbParameter> parameters)
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

        /// <summary>
        /// 打开数据库的连接通道。
        /// </summary>
        /// <param name="openedMode">连接打开方式。</param>
        /// <returns>打开成功返回 true，否则返回 false。</returns>
        public bool OpenConnection(ConnectionOpenedMode openedMode)
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
        /// 关闭数据库的连接通道。
        /// </summary>
        /// <param name="openedMode">允许关闭的连接打开模式。</param>
        /// <returns>关闭成功返回 true，否则返回 false。</returns>
        public bool CloseConnection(ConnectionOpenedMode openedMode)
        {
            if (_Connection != null)
            {
                if (openedMode >= _OpenedMode)
                {
                    if (_Connection.State == ConnectionState.Connecting || _Connection.State == ConnectionState.Executing)
                        _Connection.Close();

                    _OpenedMode = ConnectionOpenedMode.Auto;

                    return true;
                }

                return false;
            }

            return true;
        }

        /// <summary>
        /// 开启事务执行模式。
        /// </summary>
        public void BeginTransaction()
        {
            OpenConnection(ConnectionOpenedMode.Transaction);

            _Transaction = _Connection.BeginTransaction();
        }

        /// <summary>
        /// 开启事务执行模式。
        /// </summary>
        /// <param name="level">事务级别。</param>
        public void BeginTransaction(IsolationLevel level)
        {
            OpenConnection(ConnectionOpenedMode.Transaction);

            _Transaction = _Connection.BeginTransaction(level);
        }

        /// <summary>
        /// 回滚当前事务中的所有操作。
        /// </summary>
        public void RollbackTransaction()
        {
            if (_Transaction != null)
                _Transaction.Rollback();

            CloseConnection(ConnectionOpenedMode.Transaction);

            _Transaction = null;
        }

        /// <summary>
        /// 提交当前事务中的所有操作。
        /// </summary>
        public void CommitTransaction()
        {
            if (_Transaction != null)
                _Transaction.Commit();

            CloseConnection(ConnectionOpenedMode.Transaction);

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

            _Transaction = null;
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
        /// 释放当前操作对象所占用的资源信息。
        /// </summary>
        public void Dispose()
        {
            if (_Transaction != null)
                _Transaction.Dispose();

            if (_Command != null)
                _Command.Dispose();

            if (_Connection != null)
                _Connection.Dispose();

            _Transaction = null;
            _Command = null;
            _Connection = null;
        }

        /// <summary>
        /// 获取数据库名指向系统数据库的连接对象。
        /// </summary>
        /// <returns>指向系统 Master 数据库的连接对象。</returns>
        private SqlConnection GetMasterConnection()
        {
            string connectionString = new Regex(@"(?:((?:initial\s+catalog)|database))\s*=\s*\S+?(;|$)", RegexOptions.IgnoreCase).Replace(_ConnectionString, "$1=master$2");

            return new SqlConnection(connectionString);
        }

        /// <summary>
        /// 删除指定的数据表。
        /// </summary>
        /// <param name="tableName">待删除的数据表名称。</param>
        /// <returns>删除成功返回 true，否则返回 false。</returns>
        public bool DeleteTable(string tableName)
        {
            ExecuteNonQuery($"drop table {tableName}");

            return true;
        }
    }
}
