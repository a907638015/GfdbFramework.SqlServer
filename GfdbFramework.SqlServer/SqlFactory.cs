﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using GfdbFramework.Core;
using GfdbFramework.DataSource;
using GfdbFramework.Enum;
using GfdbFramework.Field;
using GfdbFramework.Interface;

namespace GfdbFramework.SqlServer
{
    /// <summary>
    /// SqlServer 数据库的 Sql 创建工厂类。
    /// </summary>
    public class SqlFactory : ISqlFactory
    {
        private const string _CASE_SENSITIVE_MARK = "collate Chinese_PRC_CI_AI";
        private const string _BOOL_TYPE_NAME = "System.Boolean";
        private static readonly string _DBFunCountMethodName = nameof(DBFun.Count);
        private static readonly string _DBFunMaxMethodName = nameof(DBFun.Max);
        private static readonly string _DBFunMinMethodName = nameof(DBFun.Min);
        private static readonly string _DBFunSumMethodName = nameof(DBFun.Sum);
        private static readonly string _DBFunAvgMethodName = nameof(DBFun.Avg);
        private static readonly string _DBFunSTDevMethodName = nameof(DBFun.STDev);
        private static readonly string _DBFunSTDevPMethodName = nameof(DBFun.STDevP);
        private static readonly string _DBFunVarMethodName = nameof(DBFun.Var);
        private static readonly string _DBFunVarPMethodName = nameof(DBFun.VarP);
        private static readonly string _DBFunNowTimeMethodName = nameof(DBFun.NowTime);
        private static readonly string _DBFunNewIDMethodName = nameof(DBFun.NewID);
        private static readonly Type _DBFunType = typeof(DBFun);

        /// <summary>
        /// 对指定的原始字段名、表名或视图名称进行编码。
        /// </summary>
        /// <param name="name">需要编码的原始字段名、表名或视图名称名称。</param>
        /// <param name="type">名称类型。</param>
        /// <returns>编码后的名称。</returns>
        public string EncodeName(string name, NameType type)
        {
            return $"[{name}]";
        }

        /// <summary>
        /// 使用指定的别名下标生成一个别名（必须保证不同下标生成的别名不同，相同下标生成的别名相同，且所生成的别名不得是数据库中的关键字）。
        /// </summary>
        /// <param name="aliasIndex">生成别名时的下标。</param>
        /// <param name="type">需要生成别名的名称类型。</param>
        /// <returns>使用指定别名下标生成好的别名。</returns>
        public string GenerateAlias(int aliasIndex, NameType type)
        {
            return type == NameType.Field ? $"F{aliasIndex}" : type == NameType.View ? $"V{aliasIndex}" : $"T{aliasIndex}";
        }

        /// <summary>
        /// 生成指定数据源所对应的 Sql 查询语句。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成查询 Sql 的数据源信息。</param>
        /// <param name="parameters">生成 Sql 所需使用的参数集合。</param>
        /// <returns>生成好的 Sql 查询语句。</returns>
        public string GenerateQuerySql(IDataContext dataContext, BasicDataSource dataSource, out Interface.IReadOnlyList<DbParameter> parameters)
        {
            return GenerateQuerySql(dataContext, dataSource, true, out parameters);
        }

        /// <summary>
        /// 生成指定数据源所对应的 Sql 查询语句。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成查询 Sql 的数据源信息。</param>
        /// <param name="useFieldAlias">生成的查询语句是否应当应用上字段别名。</param>
        /// <param name="parameters">生成 Sql 所需使用的参数集合。</param>
        /// <returns>生成好的 Sql 查询语句。</returns>
        public string GenerateQuerySql(IDataContext dataContext, BasicDataSource dataSource, bool useFieldAlias, out Interface.IReadOnlyList<DbParameter> parameters)
        {
            Dictionary<object, DbParameter> pars = new Dictionary<object, DbParameter>();

            string sql = GenerateQuerySql(dataContext, dataSource, dataSource.SelectField ?? dataSource.RootField, useFieldAlias, item =>
            {
                if (!pars.TryGetValue(item, out DbParameter dbParameter))
                {
                    dbParameter = new SqlParameter($"P{pars.Count}", item);

                    pars.Add(item, dbParameter);
                }

                return $"@{dbParameter.ParameterName}";
            });

            parameters = new Realize.ReadOnlyList<DbParameter>(pars.Values);

            return sql;
        }

        /// <summary>
        /// 生成从数据源中查询某一字段的 Sql 查询语句。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">查询字段所归属的数据源。</param>
        /// <param name="selectField">待生成 Sql 的查询字段。</param>
        /// <param name="useFieldAlias">生成的查询语句是否应当应用上字段别名。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的 Sql 查询语句。</returns>
        private string GenerateQuerySql(IDataContext dataContext, BasicDataSource dataSource, Field.Field selectField, bool useFieldAlias, Func<object, string> addParameter)
        {
            StringBuilder sqlFields = new StringBuilder();
            StringBuilder orderBy = new StringBuilder();
            StringBuilder groupBy = new StringBuilder();
            string sqlFrom = GenerateFromSql(dataContext, dataSource, false, addParameter);
            string where = string.Empty;
            string top = string.Empty;
            string distinct = dataSource.IsDistinctly ? "distinct " : string.Empty;
            string other = string.Empty;

            AppendSelectField(dataContext, dataSource, selectField, sqlFields, new HashSet<Field.Field>(), useFieldAlias, addParameter);
            dataSource.Where?.InitExpressionSQL(dataContext, dataSource, addParameter);

            if (dataSource.SortItems != null && dataSource.SortItems.Count > 0)
            {
                orderBy.Append(" order by ");

                foreach (var item in dataSource.SortItems)
                {
                    if (orderBy.Length > 10)
                        orderBy.Append(", ");

                    item.Field.InitExpressionSQL(dataContext, dataSource, addParameter);

                    if (item.Field.Type == FieldType.Subquery)
                        orderBy.AppendFormat("({0})", item.Field.ExpressionInfo.SQL);
                    else
                        orderBy.Append(item.Field.ExpressionInfo.SQL);

                    if (item.Type == SortType.Descending)
                        orderBy.Append(" desc");
                }
            }

            if (dataSource.GroupFields != null && dataSource.GroupFields.Count > 0)
            {
                groupBy.Append(" group by ");

                foreach (var item in dataSource.GroupFields)
                {
                    if (groupBy.Length > 10)
                        groupBy.Append(", ");

                    item.InitExpressionSQL(dataContext, dataSource, addParameter);

                    if (item.Type == FieldType.Subquery)
                        groupBy.AppendFormat("({0})", item.ExpressionInfo.SQL);
                    else
                        groupBy.Append(item.ExpressionInfo.SQL);
                }
            }

            if (dataSource.Where != null)
            {
                dataSource.Where.InitExpressionSQL(dataContext, dataSource, addParameter);

                where = $" where {dataSource.Where.BooleanInfo.SQL}";
            }

            if (dataSource.Limit != null && dataSource.Limit.HasValue)
            {
                if (dataSource.Limit.Value.Start == 0)
                {
                    top = $"top {dataSource.Limit.Value.Count} ";
                }
                else if (dataContext.BuildNumber >= 684)        //Sql Server 2012 及以上版本用 offset fetch next 方式
                {
                    other = $" offset {dataSource.Limit.Value.Start} rows fetch next {dataSource.Limit.Value.Count} rows only";
                }
                else if (dataContext.BuildNumber >= 611)        //Sql Server 2005 及以上版本用 row_number() over() 方式
                {
                    if (orderBy.Length < 1)
                        throw new Exception("低于 Sql Server 2012 版本的数据库在使用 Limit 功能时至少要设置有一个排序字段");

                    return $"select * from (select {distinct}row_number() over({orderBy.ToString().TrimStart()}) as RowNumber, {sqlFields} from {sqlFrom}{where}{groupBy}) as LimitTable where RowNumber between {dataSource.Limit.Value.Start + 1} and {dataSource.Limit.Value.Start + dataSource.Limit.Value.Count}";
                }
                else
                {
                    throw new Exception("低于 Sql Server 2005 版本的数据库不支持 Limit 功能，请改用 Contains 方法实现");
                }
            }

            return $"select {distinct}{top}{sqlFields} from {sqlFrom}{where}{groupBy}{orderBy}{other}";
        }

        /// <summary>
        /// 生成指定数据源在被用作 Select 查询中的 From 数据源时的 Sql。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">查询字段所归属的数据源。</param>
        /// <param name="forceQuery">若是原生数据源是否强制启用查询。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的 Sql 查询语句。</returns>
        private string GenerateFromSql(IDataContext dataContext, DataSource.DataSource dataSource, bool forceQuery, Func<object, string> addParameter)
        {
            if (dataSource.Type == DataSourceType.Table || dataSource.Type == DataSourceType.View)
            {
                OriginalDataSource originalDataSource = (OriginalDataSource)dataSource;

                if (forceQuery && originalDataSource.SelectField != null)
                    return $"({GenerateQuerySql(dataContext, originalDataSource, originalDataSource.SelectField, true, addParameter)}) as {originalDataSource.Alias}";
                else
                    return $"{originalDataSource.Name} as {originalDataSource.Alias}";
            }
            else if (dataSource.Type == DataSourceType.QueryResult)
            {
                ResultDataSource resultDataSource = (ResultDataSource)dataSource;

                if (forceQuery)
                    return $"({GenerateQuerySql(dataContext, resultDataSource, resultDataSource.SelectField ?? resultDataSource.RootField, true, addParameter)}) as {resultDataSource.Alias}";
                else
                    return GenerateFromSql(dataContext, resultDataSource.FromDataSource, true, addParameter);
            }
            else
            {
                JoinDataSource joinDataSource = (JoinDataSource)dataSource;

                string left = GenerateFromSql(dataContext, joinDataSource.Left, true, addParameter);
                string right = GenerateFromSql(dataContext, joinDataSource.Right, true, addParameter);

                if (joinDataSource.Type == DataSourceType.CrossJoin)
                {
                    return $"{left} cross join {right}";
                }
                else
                {
                    string joinType;

                    switch (joinDataSource.Type)
                    {
                        case DataSourceType.LeftJoin:
                            joinType = "left join";
                            break;
                        case DataSourceType.RightJoin:
                            joinType = "right join";
                            break;
                        case DataSourceType.FullJoin:
                            joinType = "full join";
                            break;
                        default:
                            joinType = "inner join";
                            break;
                    }

                    joinDataSource.On.InitExpressionSQL(dataContext, joinDataSource, addParameter);

                    return $"{left} {joinType} {right} on {joinDataSource.On.BooleanInfo.SQL}";
                }
            }
        }

        /// <summary>
        /// 追加待查询字段信息到字段集合中。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">查询字段所归属的数据源。</param>
        /// <param name="selectField">待追加的查询字段。</param>
        /// <param name="sqlFields">用于保存查询字段信息的字符串构造器。</param>
        /// <param name="appendedFields">已经追加过的字段集合。</param>
        /// <param name="useFieldAlias">生成的查询字段是否应当应用上字段别名。</param>
        /// <param name="addParameter">若查询字段需要添加参数时的添加方法。</param>
        private void AppendSelectField(IDataContext dataContext, DataSource.DataSource dataSource, Field.Field selectField, StringBuilder sqlFields, HashSet<Field.Field> appendedFields, bool useFieldAlias, Func<object, string> addParameter)
        {
            if (!appendedFields.Contains(selectField))
            {
                if (selectField.Type == FieldType.Object)
                {
                    ObjectField objectField = (ObjectField)selectField;

                    if (objectField.ConstructorInfo.Parameters != null && objectField.ConstructorInfo.Parameters.Count > 0)
                    {
                        foreach (var item in objectField.ConstructorInfo.Parameters)
                        {
                            AppendSelectField(dataContext, dataSource, item, sqlFields, appendedFields, useFieldAlias, addParameter);
                        }
                    }

                    if (objectField.Members != null && objectField.Members.Count > 0)
                    {
                        foreach (var item in objectField.Members)
                        {
                            AppendSelectField(dataContext, dataSource, item.Value.Field, sqlFields, appendedFields, useFieldAlias, addParameter);
                        }
                    }
                }
                else if (selectField.Type == FieldType.Collection)
                {
                    CollectionField collectionField = (CollectionField)selectField;

                    if (collectionField.ConstructorInfo.Parameters != null && collectionField.ConstructorInfo.Parameters.Count > 0)
                    {
                        foreach (var item in collectionField.ConstructorInfo.Parameters)
                        {
                            AppendSelectField(dataContext, dataSource, item, sqlFields, appendedFields, useFieldAlias, addParameter);
                        }
                    }

                    foreach (var item in collectionField)
                    {
                        AppendSelectField(dataContext, dataSource, item, sqlFields, appendedFields, useFieldAlias, addParameter);
                    }
                }
                else if (selectField is BasicField basicField)
                {
                    basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                    if (sqlFields.Length > 0)
                        sqlFields.Append(", ");

                    if (basicField.Type == FieldType.Subquery)
                        sqlFields.Append($"({basicField.ExpressionInfo.SQL})");
                    else
                        sqlFields.Append(basicField.ExpressionInfo.SQL);

                    if (useFieldAlias && !string.IsNullOrWhiteSpace(basicField.Alias))
                        sqlFields.Append($" as {basicField.Alias}");
                }

                appendedFields.Add(selectField);
            }
        }

        /// <summary>
        /// 初始化指定二元操作字段的 Sql 表示信息。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">待生成 Sql 表示信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的表示 Sql 信息。</returns>
        public ExpressionInfo InitBinaryField(IDataContext dataContext, DataSource.DataSource dataSource, BinaryField field, Func<object, string> addParameter)
        {
            string rightSql = null;
            string leftSql = null;
            string sql = null;

            if (field.DataType.FullName != _BOOL_TYPE_NAME)
            {
                field.Left.InitExpressionSQL(dataContext, dataSource, addParameter);
                field.Right.InitExpressionSQL(dataContext, dataSource, addParameter);

                leftSql = field.Left.Type == FieldType.Subquery || (field.OperationType != OperationType.Coalesce && field.OperationType != OperationType.Power && Helper.CheckIsPriority(field.OperationType == OperationType.LeftShift ? OperationType.Multiply : field.OperationType == OperationType.RightShift ? OperationType.Divide : field.OperationType, field.Left.ExpressionInfo.Type, false)) ? $"({field.Left.ExpressionInfo.SQL})" : field.Left.ExpressionInfo.SQL;
                rightSql = field.Right.Type == FieldType.Subquery || (field.OperationType != OperationType.Coalesce && field.OperationType != OperationType.Power && Helper.CheckIsPriority(field.OperationType == OperationType.LeftShift ? OperationType.Multiply : field.OperationType == OperationType.RightShift ? OperationType.Divide : field.OperationType, field.Right.ExpressionInfo.Type, true)) ? $"({field.Right.ExpressionInfo.SQL})" : field.Right.ExpressionInfo.SQL;
            }

            OperationType useType = field.OperationType;

            switch (field.OperationType)
            {

                case OperationType.Add:
                    sql = $"{leftSql} + {rightSql}";
                    break;
                case OperationType.And:
                    sql = $"{leftSql} & {rightSql}";
                    break;
                case OperationType.Divide:
                    sql = $"{leftSql} / {rightSql}";
                    break;
                case OperationType.Coalesce:
                    sql = $"isnull({leftSql}, {rightSql})";

                    useType = OperationType.Call;
                    break;
                case OperationType.ExclusiveOr:
                    sql = $"{leftSql} ^ {rightSql}";
                    break;
                case OperationType.GreaterThan:
                case OperationType.GreaterThanOrEqual:
                case OperationType.LessThan:
                case OperationType.LessThanOrEqual:
                case OperationType.Equal:
                case OperationType.NotEqual:
                case OperationType.NotIn:
                case OperationType.In:
                case OperationType.Like:
                case OperationType.NotLike:
                case OperationType.AndAlso:
                case OperationType.OrElse:
                    //大于等于Sql Server 2012时使用 iif 函数
                    if (dataContext.BuildNumber >= 684)
                    {
                        sql = $"iif({field.BooleanInfo.SQL}, convert(bit, 1), convert(bit, 0)";

                        useType = OperationType.Call;
                    }
                    else
                    {
                        sql = $"case when {field.BooleanInfo.SQL} then convert(bit, 1) else convert(bit, 0) end";

                        useType = OperationType.Default;
                    }
                    break;
                case OperationType.LeftShift:
                    sql = $"{leftSql} * power(2, {rightSql})";

                    useType = OperationType.Multiply;
                    break;
                case OperationType.Modulo:
                    sql = $"{leftSql} % {rightSql}";
                    break;
                case OperationType.Multiply:
                    sql = $"{leftSql} * {rightSql}";
                    break;
                case OperationType.Or:
                    sql = $"{leftSql} | {rightSql}";
                    break;
                case OperationType.Power:
                    sql = $"power({leftSql}, {rightSql})";

                    useType = OperationType.Call;
                    break;
                case OperationType.RightShift:
                    sql = $"{leftSql} / power(2, {rightSql})";

                    useType = OperationType.Divide;
                    break;
                case OperationType.Subtract:
                    sql = $"{leftSql} - {rightSql}";
                    break;
                case OperationType.ArrayIndex:
                    throw new Exception("Sql Server 不支持数组或集合类型的字段操作");
            }

            return new ExpressionInfo(sql, useType);
        }

        /// <summary>
        /// 初始化指定三元操作（条件操作）字段的 Sql 表示信息。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">待生成 Sql 表示信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的表示 Sql 信息。</returns>
        public ExpressionInfo InitConditionalField(IDataContext dataContext, DataSource.DataSource dataSource, ConditionalField field, Func<object, string> addParameter)
        {
            field.Test.InitExpressionSQL(dataContext, dataSource, addParameter);
            field.IfTrue.InitExpressionSQL(dataContext, dataSource, addParameter);
            field.IfFalse.InitExpressionSQL(dataContext, dataSource, addParameter);

            string testSql = field.Test.Type == FieldType.Subquery ? $"({field.Test.BooleanInfo.SQL})" : field.Test.BooleanInfo.SQL;
            string ifTrueSql = field.IfTrue.Type == FieldType.Subquery ? $"({field.IfTrue.ExpressionInfo.SQL})" : field.IfTrue.ExpressionInfo.SQL;
            string ifFalseSql = field.IfFalse.Type == FieldType.Subquery ? $"({field.IfFalse.ExpressionInfo.SQL})" : field.IfFalse.ExpressionInfo.SQL;

            //大于等于Sql Server 2012时使用 iif 函数
            if (dataContext.BuildNumber >= 684)
                return new ExpressionInfo($"iif({testSql}, ${ifTrueSql}, ${ifFalseSql})", OperationType.Call);
            else
                return new ExpressionInfo($"case when {testSql} then ${ifTrueSql} else ${ifFalseSql} end", OperationType.Default);
        }

        /// <summary>
        /// 初始化指定常量字段的 Sql 表示信息。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">待生成 Sql 表示信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的表示 Sql 信息。</returns>
        public ExpressionInfo InitConstantField(IDataContext dataContext, DataSource.DataSource dataSource, ConstantField field, Func<object, string> addParameter)
        {
            //只有基础数据类型才能作为参数传入 Sql Server
            if (Helper.CheckIsBasicType(field.DataType))
                return new ExpressionInfo(addParameter(field.Value), OperationType.Default);
            else if (field.Value is Queryable)
                throw new Exception("Sql Server 子查询不支持多行数据返回，若要使用子查询，可在 Queryable 对象最后调用一次 First() 函数或 Last() 函数将其限定只返回一行数据");
            else if (field.Value is MultipleJoin)
                throw new Exception("子查询不支持直接返回多表关联查询对象（MultipleJoin），若要使用子查询，可在 MultipleJoin 对象上调用一次 Select 函数并再次调用 First 方法即可");
            else
                throw new Exception("Sql Server 只支持基础数据类型的常量作为参数传入");
        }

        /// <summary>
        /// 初始化指定基础数据类型字段被直接用做 Where、On、Case 等条件判定时的 Sql 表示信息（如原始 Bit 类型字段直接用作 Where 条件时需要写成 Table.FieldName = 1）。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">需要生成 Where、On、Case 等条件判定表示 Sql 信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好用于在 Where、On、Case 等条件判定时的 Sql 表示信息。</returns>
        public ExpressionInfo InitFieldWhere(IDataContext dataContext, DataSource.DataSource dataSource, BasicField field, Func<object, string> addParameter)
        {
            if (field.Type == FieldType.Binary)
            {
                BinaryField binaryField = (BinaryField)field;

                //只有二元运算中的这几种操作可直接用于条件判定
                if (binaryField.OperationType == OperationType.Equal
                    || binaryField.OperationType == OperationType.NotEqual
                    || binaryField.OperationType == OperationType.LessThan
                    || binaryField.OperationType == OperationType.LessThanOrEqual
                    || binaryField.OperationType == OperationType.GreaterThan
                    || binaryField.OperationType == OperationType.GreaterThanOrEqual
                    || binaryField.OperationType == OperationType.AndAlso
                    || binaryField.OperationType == OperationType.OrElse
                    || binaryField.OperationType == OperationType.In
                    || binaryField.OperationType == OperationType.NotIn
                    || binaryField.OperationType == OperationType.Like
                    || binaryField.OperationType == OperationType.NotLike)
                {
                    binaryField.Left.InitExpressionSQL(dataContext, dataSource, addParameter);

                    if (binaryField.OperationType == OperationType.In || binaryField.OperationType == OperationType.NotIn)
                    {
                        string containType = binaryField.OperationType == OperationType.NotIn ? "not in" : "in";
                        string leftSql = binaryField.Left.Type == FieldType.Subquery || Helper.CheckIsPriority(binaryField.OperationType, binaryField.Left.ExpressionInfo.Type, false) ? $"({binaryField.Left.ExpressionInfo.SQL})" : binaryField.Left.ExpressionInfo.SQL;

                        if (binaryField.Left.DataType.FullName == "System.String" && dataContext.IsCaseSensitive)
                            leftSql = $"{leftSql} {_CASE_SENSITIVE_MARK}";

                        if (binaryField.Right.Type == FieldType.Subquery)
                        {
                            binaryField.Right.InitExpressionSQL(dataContext, dataSource, addParameter);

                            return new ExpressionInfo($"{leftSql} {containType} ({binaryField.Right.ExpressionInfo.SQL})", binaryField.OperationType);
                        }
                        //如果是常量字段且常量字段的值是 IEnumerable 类型
                        else if (binaryField.Right.Type == FieldType.Constant && ((binaryField.Right.DataType.IsArray && binaryField.Right.DataType.GetArrayRank() == 1 && Helper.CheckIsBasicType(binaryField.Right.DataType.GetElementType())) || (binaryField.Right.DataType.IsGenericType && binaryField.Right.DataType.GetGenericArguments().Length == 1)))
                        {
                            IEnumerable enumerable = (IEnumerable)((ConstantField)binaryField.Right).Value;

                            StringBuilder collection = new StringBuilder();

                            foreach (var item in enumerable)
                            {
                                if (collection.Length > 0)
                                    collection.Append(", ");

                                collection.Append(addParameter(item));
                            }

                            return new ExpressionInfo($"{leftSql} {containType} ({collection})", OperationType.Default);
                        }

                        throw new Exception("对用于子查询或 Where 条件的 Contains 方法，被调用对象 IEnumerable<T> 的成员（T）类型必须是基础数据类型");
                    }
                    else
                    {
                        binaryField.Right.InitExpressionSQL(dataContext, dataSource, addParameter);

                        if (binaryField.OperationType == OperationType.AndAlso || binaryField.OperationType == OperationType.OrElse)
                        {
                            string leftSql = binaryField.Left.Type == FieldType.Subquery || Helper.CheckIsPriority(binaryField.OperationType, binaryField.Left.BooleanInfo.Type, false) ? $"({binaryField.Left.BooleanInfo.SQL})" : binaryField.Left.BooleanInfo.SQL;
                            string rightSql = binaryField.Right.Type == FieldType.Subquery || Helper.CheckIsPriority(binaryField.OperationType, binaryField.Right.BooleanInfo.Type, true) ? $"({binaryField.Right.BooleanInfo.SQL})" : binaryField.Right.BooleanInfo.SQL;

                            if (binaryField.OperationType == OperationType.AndAlso)
                                return new ExpressionInfo($"{leftSql} and {rightSql}", OperationType.AndAlso);
                            else
                                return new ExpressionInfo($"{leftSql} or {rightSql}", OperationType.OrElse);
                        }
                        else
                        {
                            string leftSql = binaryField.Left.Type == FieldType.Subquery || Helper.CheckIsPriority(binaryField.OperationType, binaryField.Left.ExpressionInfo.Type, false) ? $"({binaryField.Left.ExpressionInfo.SQL})" : binaryField.Left.ExpressionInfo.SQL;
                            string rightSql = binaryField.Right.Type == FieldType.Subquery || Helper.CheckIsPriority(binaryField.OperationType, binaryField.Right.ExpressionInfo.Type, true) ? $"({binaryField.Right.ExpressionInfo.SQL})" : binaryField.Right.ExpressionInfo.SQL;

                            switch (binaryField.OperationType)
                            {
                                case OperationType.Equal:
                                    if (dataContext.IsCaseSensitive && binaryField.Left.DataType.FullName == "System.String" && binaryField.Right.DataType.FullName == "System.String")
                                        return new ExpressionInfo($"{leftSql} {_CASE_SENSITIVE_MARK} = {rightSql}", OperationType.Equal);

                                    return new ExpressionInfo($"{leftSql} = {rightSql}", OperationType.Equal);
                                case OperationType.NotEqual:
                                    if (dataContext.IsCaseSensitive && binaryField.Left.DataType.FullName == "System.String" && binaryField.Right.DataType.FullName == "System.String")
                                        return new ExpressionInfo($"{leftSql} {_CASE_SENSITIVE_MARK} != {rightSql}", OperationType.Equal);

                                    return new ExpressionInfo($"{leftSql} != {rightSql}", OperationType.NotEqual);
                                case OperationType.LessThan:
                                    return new ExpressionInfo($"{leftSql} < {rightSql}", OperationType.LessThan);
                                case OperationType.LessThanOrEqual:
                                    return new ExpressionInfo($"{leftSql} <= {rightSql}", OperationType.LessThanOrEqual);
                                case OperationType.GreaterThan:
                                    return new ExpressionInfo($"{leftSql} > {rightSql}", OperationType.GreaterThan);
                                case OperationType.GreaterThanOrEqual:
                                    return new ExpressionInfo($"{leftSql} >= {rightSql}", OperationType.GreaterThanOrEqual);
                                case OperationType.Like:
                                case OperationType.NotLike:
                                    if (dataContext.IsCaseSensitive)
                                        return new ExpressionInfo($"{leftSql} {_CASE_SENSITIVE_MARK} {(binaryField.OperationType == OperationType.Like ? "like" : "not like")} {rightSql}", OperationType.Like);
                                    else
                                        return new ExpressionInfo($"{leftSql} {(binaryField.OperationType == OperationType.Like ? "like" : "not like")} {rightSql}", OperationType.Like);
                            }
                        }
                    }
                }
            }
            else if (field.Type == FieldType.Unary && field.DataType.FullName == _BOOL_TYPE_NAME && ((UnaryField)field).OperationType == OperationType.Not)
            {
                UnaryField unaryField = (UnaryField)field;

                unaryField.Operand.InitExpressionSQL(dataContext, dataSource, unaryField.Operand.Type != FieldType.Original && unaryField.Operand.Type != FieldType.Quote, addParameter);

                string operandSql = unaryField.Operand.Type == FieldType.Subquery || (unaryField.Operand.Type != FieldType.Original && unaryField.Operand.Type != FieldType.Quote) || Helper.CheckIsPriority(OperationType.Equal, unaryField.Operand.ExpressionInfo.Type, false) ? $"({unaryField.Operand.ExpressionInfo.SQL})" : unaryField.Operand.ExpressionInfo.SQL;

                return new ExpressionInfo($"{operandSql} = 0", OperationType.Equal);
            }
            else if (field.Type == FieldType.Method)
            {
                MethodField methodField = (MethodField)field;

                //string 类型的 StartsWith 或 Contains 方法，不支持多参数的 StartsWith 或 Contains 方法
                if (methodField.ObjectField != null && methodField.ObjectField is BasicField basicField && (methodField.MethodInfo.Name == "StartsWith" || methodField.MethodInfo.Name == "Contains") && methodField.Parameters != null && methodField.Parameters.Count == 1)
                {
                    basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                    string objectSql = basicField.Type == FieldType.Subquery || Helper.CheckIsPriority(basicField.ExpressionInfo.Type, OperationType.Subtract, true) ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;
                    string searchString = methodField.Parameters[0].Type == FieldType.Subquery ? $"({((BasicField)methodField.Parameters[0]).ExpressionInfo.SQL})" : ((BasicField)methodField.Parameters[0]).ExpressionInfo.SQL;
                    string checkString = methodField.MethodInfo.Name == "StartsWith" ? "=" : ">=";

                    if (dataContext.IsCaseSensitive)
                        return new ExpressionInfo($"charIndex({searchString}, {objectSql} {_CASE_SENSITIVE_MARK}) {checkString} 1", OperationType.Call);
                    else
                        return new ExpressionInfo($"charIndex({searchString}, {objectSql}) {checkString} 1", OperationType.Call);
                }
            }

            field.InitExpressionSQL(dataContext, dataSource, false, addParameter);

            string fieldSql = field.Type == FieldType.Subquery || Helper.CheckIsPriority(OperationType.Equal, field.ExpressionInfo.Type, false) ? $"({field.ExpressionInfo.SQL})" : field.ExpressionInfo.SQL;

            return new ExpressionInfo($"{fieldSql} = 1", OperationType.Equal);
        }

        /// <summary>
        /// 初始化指定成员调用字段的 Sql 表示信息。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">待生成 Sql 表示信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的表示 Sql 信息。</returns>
        public ExpressionInfo InitMemberField(IDataContext dataContext, DataSource.DataSource dataSource, MemberField field, Func<object, string> addParameter)
        {
            if (field.ObjectField != null)
            {
                //若调用实例为 string 类型
                if (field.ObjectField.DataType.FullName == "System.String")
                {
                    if (field.ObjectField is BasicField basicField && field.MemberInfo.Name == "Length" && field.MemberInfo.MemberType == MemberTypes.Property)
                    {
                        basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                        string objectSql = field.Type == FieldType.Subquery ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;

                        return new ExpressionInfo($"len({objectSql})", OperationType.Call);
                    }
                }
            }

            throw new Exception($"未能将调用 {field.MemberInfo.DeclaringType.FullName} 类中的 {field.MemberInfo.Name} 成员字段转换成 Sql 表示信息");
        }

        /// <summary>
        /// 初始化指定方法调用字段的 Sql 表示信息。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">待生成 Sql 表示信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的表示 Sql 信息。</returns>
        public ExpressionInfo InitMethodField(IDataContext dataContext, DataSource.DataSource dataSource, MethodField field, Func<object, string> addParameter)
        {
            if (field.ObjectField != null)
            {
                //调用实例字段是基础数据字段
                if (field.ObjectField is BasicField basicField)
                {
                    //ToString 方法
                    if (field.MethodInfo.Name == "ToString")
                    {
                        basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                        string objectSql = basicField.Type == FieldType.Subquery ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;

                        return new ExpressionInfo($"convert(varchar, {objectSql})", OperationType.Call);
                    }
                    //若调用实例为 string 类型
                    else if (field.ObjectField.DataType.FullName == "System.String")
                    {
                        //IndexOf 方法
                        if (field.MethodInfo.Name == "IndexOf" && field.Parameters != null && (field.Parameters.Count == 1 || field.Parameters.Count == 2))
                        {
                            basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                            string objectSql = basicField.Type == FieldType.Subquery || Helper.CheckIsPriority(basicField.ExpressionInfo.Type, OperationType.Subtract, true) ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;
                            string searchString = field.Parameters[0].Type == FieldType.Subquery ? $"({((BasicField)field.Parameters[0]).ExpressionInfo.SQL})" : ((BasicField)field.Parameters[0]).ExpressionInfo.SQL;
                            string startIndex = field.Parameters.Count == 1 ? null : field.Parameters[1].Type == FieldType.Subquery ? $"({((BasicField)field.Parameters[1]).ExpressionInfo.SQL})" : ((BasicField)field.Parameters[1]).ExpressionInfo.SQL;

                            if (dataContext.IsCaseSensitive)
                                objectSql = $"{objectSql} {_CASE_SENSITIVE_MARK}";

                            //Sql Server 中的 charIndex 是从 1 开始，结果得手动减去 1，startIndex 得手动加上 1
                            if (field.Parameters.Count == 1)
                                return new ExpressionInfo($"charIndex({searchString}, {objectSql}) - 1", OperationType.Subtract);
                            else
                                return new ExpressionInfo($"charIndex({searchString}, {objectSql}, {startIndex} + 1) - 1", OperationType.Subtract);
                        }
                        ///Substring 方法
                        else if (field.MethodInfo.Name == "Substring" && field.Parameters != null && (field.Parameters.Count == 1 || field.Parameters.Count == 2))
                        {
                            BasicField startField = (BasicField)field.Parameters[0];

                            startField.InitExpressionSQL(dataContext, dataSource, addParameter);
                            basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                            string parameterStartSql = startField.Type == FieldType.Subquery || Helper.CheckIsPriority(OperationType.Add, startField.ExpressionInfo.Type, false) ? $"({startField.ExpressionInfo.SQL})" : startField.ExpressionInfo.SQL;
                            string parameterStringSql = basicField.Type == FieldType.Subquery ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;
                            string lengthSql;

                            if (field.Parameters.Count == 1)
                            {
                                string lengthParameter = basicField.Type == FieldType.Subquery ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;
                                string startSql = startField.Type == FieldType.Subquery || Helper.CheckIsPriority(OperationType.Subtract, startField.ExpressionInfo.Type, true) ? $"({startField.ExpressionInfo.SQL})" : startField.ExpressionInfo.SQL;

                                lengthSql = $"len({lengthParameter}) - {startSql}";
                            }
                            else
                            {
                                BasicField lengthField = (BasicField)field.Parameters[1];

                                lengthField.InitExpressionSQL(dataContext, dataSource, addParameter);

                                lengthSql = lengthField.Type == FieldType.Subquery ? $"({lengthField.ExpressionInfo.SQL})" : lengthField.ExpressionInfo.SQL;
                            }

                            return new ExpressionInfo($"substring({parameterStringSql}, {parameterStartSql} + 1, {lengthSql})", OperationType.Call);
                        }
                        //Trim、ToUpper 或 ToLower 方法
                        else if ((field.MethodInfo.Name == "Trim" || field.MethodInfo.Name == "ToUpper" || field.MethodInfo.Name == "ToLower") && (field.Parameters == null || field.Parameters.Count < 1))
                        {
                            basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                            string objectSql = basicField.Type == FieldType.Subquery ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;

                            string methodName = field.MethodInfo.Name == "Trim" ? "trim" : field.MethodInfo.Name == "ToLower" ? "lower" : "upper";

                            return new ExpressionInfo($"{methodName}({objectSql})", OperationType.Call);
                        }
                        //StartsWith 或 Contains 方法，不支持多参数的 StartsWith 或 Contains 方法
                        else if ((field.MethodInfo.Name == "StartsWith" || field.MethodInfo.Name == "Contains") && field.Parameters != null && field.Parameters.Count == 1)
                        {
                            if (dataContext.BuildNumber >= 684)
                                return new ExpressionInfo($"iif({field.BooleanInfo.SQL}, convert(bit, 1), convert(bit, 0))", OperationType.Call);
                            else
                                return new ExpressionInfo($"case when {field.BooleanInfo.SQL} then convert(bit, 1) else convert(bit, 0) end", OperationType.Default);
                        }
                        //Insert 方法
                        else if (field.MethodInfo.Name == "Insert" && field.Parameters != null && field.Parameters.Count == 2)
                        {
                            basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                            string objectSql = basicField.Type == FieldType.Subquery ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;
                            string index = field.Parameters[0].Type == FieldType.Subquery ? $"({((BasicField)field.Parameters[0]).ExpressionInfo.SQL})" : ((BasicField)field.Parameters[0]).ExpressionInfo.SQL;
                            string insertString = field.Parameters[1].Type == FieldType.Subquery ? $"({((BasicField)field.Parameters[1]).ExpressionInfo.SQL})" : ((BasicField)field.Parameters[1]).ExpressionInfo.SQL;

                            return new ExpressionInfo($"subString({objectSql}, 1, {index}) + {insertString} + subString({objectSql}, index + 1, len({objectSql}) - {index})", OperationType.Add);
                        }
                    }
                }
            }
            //若为静态方法且调用类型为 Convert 类型
            else if (field.MethodInfo.DeclaringType.FullName == "System.Convert")
            {
                //各种类型转换函数，如：ToInt32、ToDateTime 等
                if (field.MethodInfo.Name.StartsWith("To") && field.Parameters != null && field.Parameters.Count == 1 && field.Parameters[0] is BasicField basicField)
                {
                    string parameterSql = basicField.Type == FieldType.Subquery ? $"({basicField.ExpressionInfo.SQL})" : basicField.ExpressionInfo.SQL;

                    switch (field.MethodInfo.Name)
                    {
                        case "ToInt16":
                        case "ToInt32":
                        case "ToInt64":
                        case "ToBoolean":
                        case "ToByte":
                        case "ToSingle":
                        case "ToDouble":
                        case "ToDecimal":
                        case "ToDateTime":
                            return new ExpressionInfo($"convert({dataContext.NetTypeToDBType(field.DataType)}, {parameterSql})", OperationType.Call);
                    }
                }
            }
            //如果是 DBFun 类的函数
            else if (field.MethodInfo.ReflectedType.FullName == _DBFunType.FullName)
            {
                //DBFun.Count 方法
                if (field.MethodInfo.Name == _DBFunCountMethodName)
                {
                    if (field.Parameters == null || field.Parameters.Count < 1)
                    {
                        return new ExpressionInfo("count(1)", OperationType.Call);
                    }
                    else if (field.Parameters.Count == 1)
                    {
                        if (field.Parameters[0] is BasicField basicField)
                        {
                            basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                            return new ExpressionInfo($"count({basicField.ExpressionInfo.SQL})", OperationType.Call);
                        }
                    }
                }
                //其他聚合方法（Max、Min、STDev、STDevP、Sum、Avg、Var、VarP）
                else if (field.Parameters != null && field.Parameters.Count == 1 && (field.MethodInfo.Name == _DBFunMaxMethodName || field.MethodInfo.Name == _DBFunSTDevMethodName || field.MethodInfo.Name == _DBFunSTDevPMethodName || field.MethodInfo.Name == _DBFunMinMethodName || field.MethodInfo.Name == _DBFunSumMethodName || field.MethodInfo.Name == _DBFunAvgMethodName || field.MethodInfo.Name == _DBFunVarMethodName || field.MethodInfo.Name == _DBFunVarPMethodName) && field.Parameters[0] is BasicField basicField)
                {
                    basicField.InitExpressionSQL(dataContext, dataSource, addParameter);

                    string methodName;

                    if (field.MethodInfo.Name == _DBFunMaxMethodName)
                        methodName = "max";
                    else if (field.MethodInfo.Name == _DBFunMinMethodName)
                        methodName = "min";
                    else if (field.MethodInfo.Name == _DBFunSumMethodName)
                        methodName = "sum";
                    else if (field.MethodInfo.Name == _DBFunAvgMethodName)
                        methodName = "avg";
                    else if (field.MethodInfo.Name == _DBFunSTDevMethodName)
                        methodName = "stdev";
                    else if (field.MethodInfo.Name == _DBFunSTDevPMethodName)
                        methodName = "stdevp";
                    else if (field.MethodInfo.Name == _DBFunVarMethodName)
                        methodName = "var";
                    else if (field.MethodInfo.Name == _DBFunVarPMethodName)
                        methodName = "varp";
                    else
                        throw new Exception($"Sql Server 不支持 DBFun.{field.MethodInfo.Name} 函数");

                    return new ExpressionInfo($"{methodName}({basicField.ExpressionInfo.SQL})", OperationType.Call);
                }
                //DBFun.NowTime 函数
                else if ((field.Parameters == null || field.Parameters.Count < 1) && field.MethodInfo.Name == _DBFunNowTimeMethodName)
                {
                    return new ExpressionInfo("getDate()", OperationType.Call);
                }
                //DBFun.NewID 函数
                else if ((field.Parameters == null || field.Parameters.Count < 1) && field.MethodInfo.Name == _DBFunNewIDMethodName)
                {
                    return new ExpressionInfo("newID()", OperationType.Call);
                }
            }

            throw new Exception($"未能将调用 {field.MethodInfo.DeclaringType.FullName} 类中的 {field.MethodInfo.Name} 方法字段转换成 Sql 表示信息");
        }

        /// <summary>
        /// 初始化指定原始数据字段的 Sql 表示信息。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">待生成 Sql 表示信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的表示 Sql 信息。</returns>
        public ExpressionInfo InitOriginalField(IDataContext dataContext, DataSource.DataSource dataSource, OriginalField field, Func<object, string> addParameter)
        {
            return new ExpressionInfo($"{((BasicDataSource)dataSource).Alias}.{field.FieldName}", OperationType.Default);
        }

        /// <summary>
        /// 初始化指定引用字段的 Sql 表示信息。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">待生成 Sql 表示信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的表示 Sql 信息。</returns>
        public ExpressionInfo InitQuoteField(IDataContext dataContext, DataSource.DataSource dataSource, QuoteField field, Func<object, string> addParameter)
        {
            return new ExpressionInfo($"{field.UsingDataSource.Alias}.{field.UsingFieldName}", OperationType.Default);
        }

        /// <summary>
        /// 初始化指定子查询字段的 Sql 表示信息。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">待生成 Sql 表示信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的表示 Sql 信息。</returns>
        public ExpressionInfo InitSubqueryField(IDataContext dataContext, DataSource.DataSource dataSource, SubqueryField field, Func<object, string> addParameter)
        {
            return new ExpressionInfo(GenerateQuerySql(dataContext, field.QueryDataSource, field.QueryField, false, addParameter), OperationType.Default);
        }

        /// <summary>
        /// 初始化指定一元操作字段的 Sql 表示信息。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成 Sql 表示信息字段所归属的数据源信息。</param>
        /// <param name="field">待生成 Sql 表示信息的字段。</param>
        /// <param name="addParameter">添加 Sql 所需的参数方法（参数为需要添加的参数，返回值代表该参数的变量名）。</param>
        /// <returns>生成好的表示 Sql 信息。</returns>
        public ExpressionInfo InitUnaryField(IDataContext dataContext, DataSource.DataSource dataSource, UnaryField field, Func<object, string> addParameter)
        {
            field.Operand.InitExpressionSQL(dataContext, dataSource, addParameter);

            if (field.OperationType == OperationType.Not)
            {
                if (field.DataType.FullName == _BOOL_TYPE_NAME)
                {
                    string sql;
                    OperationType useType;

                    //大于等于Sql Server 2012时使用 iif 函数
                    if (dataContext.BuildNumber >= 684)
                    {
                        sql = $"iif({field.BooleanInfo.SQL}, convert(bit, 1), convert(bit, 0)";

                        useType = OperationType.Call;
                    }
                    else
                    {
                        sql = $"case when {field.BooleanInfo.SQL} then convert(bit, 1) else convert(bit, 0) end";

                        useType = OperationType.Default;
                    }

                    return new ExpressionInfo(sql, useType);
                }
                else
                {
                    string operandSql = field.Operand.Type == FieldType.Subquery || Helper.CheckIsPriority(OperationType.Not, field.Operand.ExpressionInfo.Type, true) ? $"({field.Operand.ExpressionInfo.SQL})" : field.Operand.ExpressionInfo.SQL;

                    return new ExpressionInfo($"~{operandSql}", OperationType.Not);
                }
            }
            else if (field.OperationType == OperationType.Negate)
            {
                string operandSql = field.Operand.Type == FieldType.Subquery || Helper.CheckIsPriority(OperationType.Negate, field.Operand.ExpressionInfo.Type, true) ? $"({field.Operand.ExpressionInfo.SQL})" : field.Operand.ExpressionInfo.SQL;

                return new ExpressionInfo($"-{operandSql}", OperationType.Negate);
            }
            else if (field.OperationType == OperationType.Convert)
            {
                string operandSql = field.Operand.Type == FieldType.Subquery || Helper.CheckIsPriority(OperationType.Call, field.Operand.ExpressionInfo.Type, false) ? $"({field.Operand.ExpressionInfo.SQL})" : field.Operand.ExpressionInfo.SQL;

                return new ExpressionInfo($"convert({dataContext.NetTypeToDBType(field.DataType)}, {operandSql})", OperationType.Call);
            }

            throw new Exception("未能将指定的一元操作字段转换成 Sql 表示信息");
        }

        /// <summary>
        /// 生成指定数据表的插入 Sql 语句。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成插入 Sql 语句的原始数据源对象。</param>
        /// <param name="fields">需要插入的字段集合。</param>
        /// <param name="values">需要插入字段对应的值集合。</param>
        /// <param name="parameters">执行生成 Sql 所需使用的参数集合。</param>
        /// <returns>生成好的插入 Sql 语句。</returns>
        public string GenerateInsertSql(IDataContext dataContext, OriginalDataSource dataSource, Interface.IReadOnlyList<OriginalField> fields, Interface.IReadOnlyList<BasicField> values, out Interface.IReadOnlyList<DbParameter> parameters)
        {
            StringBuilder insertFields = new StringBuilder();
            StringBuilder insertValues = new StringBuilder();
            Dictionary<object, DbParameter> pars = new Dictionary<object, DbParameter>();

            string addParameterFun(object item)
            {
                if (!pars.TryGetValue(item, out DbParameter dbParameter))
                {
                    dbParameter = new SqlParameter($"P{pars.Count}", item);

                    pars.Add(item, dbParameter);
                }

                return $"@{dbParameter.ParameterName}";
            };

            for (int i = 0; i < fields.Count; i++)
            {
                OriginalField field = fields[i];
                BasicField valueField = values[i];

                valueField.InitExpressionSQL(dataContext, dataSource, addParameterFun);

                if (i > 0)
                {
                    insertFields.Append(",");
                    insertValues.Append(",");
                }

                insertFields.Append(field.FieldName);

                if (valueField.Type == FieldType.Subquery)
                    insertValues.Append($"({values[i].ExpressionInfo.SQL})");
                else
                    insertValues.Append($"{values[i].ExpressionInfo.SQL}");
            }

            parameters = new Realize.ReadOnlyList<DbParameter>(pars.Values);

            return $"insert into {dataSource.Name}({insertFields}) values ({insertValues})";
        }

        /// <summary>
        /// 生成指定数据表的插入 Sql 语句。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="dataSource">待生成插入 Sql 语句的原始数据源对象。</param>
        /// <param name="fields">需要插入的字段集合。</param>
        /// <param name="entitys">需要插入的查询结果数据源。</param>
        /// <param name="parameters">执行生成 Sql 所需使用的参数集合。</param>
        /// <returns>生成好的插入 Sql 语句。</returns>
        public string GenerateInsertSql(IDataContext dataContext, OriginalDataSource dataSource, Interface.IReadOnlyList<OriginalField> fields, BasicDataSource entitys, out Interface.IReadOnlyList<DbParameter> parameters)
        {
            StringBuilder insertFields = new StringBuilder();

            foreach (var item in fields)
            {
                if (insertFields.Length > 0)
                    insertFields.Append(",");

                insertFields.Append(item.FieldName);
            }

            return $"insert into {dataSource.Name}({insertFields}) {GenerateQuerySql(dataContext, entitys, false, out parameters)}";
        }

        /// <summary>
        /// 生成指定数据表的数据删除 Sql 语句。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="deleteSources">待删除数据行的源数组。</param>
        /// <param name="fromSource">待删除数据行的来源数据。</param>
        /// <param name="where">删除时的条件限定字段信息。</param>
        /// <param name="parameters">执行生成 Sql 所需使用的参数集合。</param>
        /// <returns>生成好的数据删除 Sql 语句。</returns>
        public string GenerateDeleteSql(IDataContext dataContext, OriginalDataSource[] deleteSources, DataSource.DataSource fromSource, BasicField where, out Interface.IReadOnlyList<DbParameter> parameters)
        {
            if (where == null && deleteSources.Length == 1 && deleteSources[0] == fromSource)
            {
                parameters = null;

                return $"delete from {deleteSources[0].Name}";
            }
            else
            {
                if (deleteSources.Length > 1)
                    throw new Exception("Sql Server 不支持一次性删除多个数据表的数据");

                Dictionary<object, DbParameter> pars = new Dictionary<object, DbParameter>();

                StringBuilder deleteSql = new StringBuilder("delete ");

                for (int i = 0; i < deleteSources.Length; i++)
                {
                    if (i > 0)
                        deleteSql.Append(",");

                    deleteSql.Append(deleteSources[i].Alias);
                }

                string addParameterFun(object item)
                {
                    if (!pars.TryGetValue(item, out DbParameter dbParameter))
                    {
                        dbParameter = new SqlParameter($"P{pars.Count}", item);

                        pars.Add(item, dbParameter);
                    }

                    return $"@{dbParameter.ParameterName}";
                }

                deleteSql.Append(" from ");

                deleteSql.Append(GenerateFromSql(dataContext, fromSource, false, addParameterFun));

                if (where != null)
                {
                    where.InitExpressionSQL(dataContext, fromSource, addParameterFun);

                    deleteSql.Append(" where ");

                    deleteSql.Append(where.BooleanInfo.SQL);
                }

                parameters = pars.Count > 0 ? new Realize.ReadOnlyList<DbParameter>(pars.Values) : null;

                return deleteSql.ToString();
            }
        }

        /// <summary>
        /// 生成指定数据表的数据更新 Sql 语句。
        /// </summary>
        /// <param name="dataContext">数据操作上下文对象。</param>
        /// <param name="modifyFields">需要修改的字段信息集合。</param>
        /// <param name="dataSource">待修改数据的来源数据。</param>
        /// <param name="where">修改时的条件限定字段信息。</param>
        /// <param name="parameters">执行生成 Sql 所需使用的参数集合。</param>
        /// <returns>生成好的数据更新 Sql 语句。</returns>
        public string GenerateUpdateSql(IDataContext dataContext, Interface.IReadOnlyList<ModifyInfo> modifyFields, DataSource.DataSource dataSource, BasicField where, out Interface.IReadOnlyList<DbParameter> parameters)
        {
            string updateSource = null;
            StringBuilder setSql = new StringBuilder();

            Dictionary<object, DbParameter> pars = new Dictionary<object, DbParameter>();

            string addParameterFun(object item)
            {
                if (!pars.TryGetValue(item, out DbParameter dbParameter))
                {
                    dbParameter = new SqlParameter($"P{pars.Count}", item);

                    pars.Add(item, dbParameter);
                }

                return $"@{dbParameter.ParameterName}";
            };

            foreach (var item in modifyFields)
            {
                if (updateSource == null)
                    updateSource = item.DataSource.Alias;
                else if (updateSource != item.DataSource.Alias)
                    throw new Exception("Sql Server 不支持一次性修改多个数据表的数据");

                if (setSql.Length > 0)
                    setSql.Append(",");

                item.Field.InitExpressionSQL(dataContext, item.DataSource, addParameterFun);
                item.Value.InitExpressionSQL(dataContext, dataSource, addParameterFun);

                setSql.Append(item.Field.ExpressionInfo.SQL);
                setSql.Append("=");

                if (item.Value.Type == FieldType.Subquery)
                {
                    setSql.Append("(");
                    setSql.Append(item.Value.ExpressionInfo.SQL);
                    setSql.Append(")");
                }
                else
                {
                    setSql.Append(item.Value.ExpressionInfo.SQL);
                }
            }

            where?.InitExpressionSQL(dataContext, dataSource, addParameterFun);

            string result;

            if (where == null)
                result = $"update {updateSource} set {setSql} from {GenerateFromSql(dataContext, dataSource, false, addParameterFun)}";
            else
                result = $"update {updateSource} set {setSql} from {GenerateFromSql(dataContext, dataSource, false, addParameterFun)} where {where.BooleanInfo.SQL}";

            parameters = pars.Count > 0 ? new Realize.ReadOnlyList<DbParameter>(pars.Values) : null;

            return result;
        }

        /// <summary>
        /// 使用指定的数据库信息生成创建该数据库的 T-Sql 语句。
        /// </summary>
        /// <param name="databaseInfo">待生成创建 T-Sql 语句的数据库信息。</param>
        /// <returns>生成好用于创建该数据库的 T-Sql 语句。</returns>
        internal string GenerateCreateDatabaseSql(DatabaseInfo databaseInfo)
        {
            if (databaseInfo.Files == null || databaseInfo.Files.Count < 1)
            {
                string databasePath = Path.Combine(Path.GetDirectoryName(GetType().Assembly.Location), "Databases");

                databaseInfo.Files = new List<Core.FileInfo>()
                {
                    new Core.FileInfo()
                    {
                        Path = Path.Combine(databasePath, $"{databaseInfo.Name}.mdf"),
                        Type = FileType.Data
                    },
                    new Core.FileInfo()
                    {
                        Path = Path.Combine(databasePath, $"{databaseInfo.Name}_log.ldf"),
                        Type = FileType.Log
                    }
                };
            }

            string primaryInfo = null;
            string logInfo = null;

            foreach (var item in databaseInfo.Files)
            {
                if (string.IsNullOrWhiteSpace(item.Path))
                    throw new ArgumentException("创建数据库时保存数据库的文件路径不能为空");

                string filePath = Path.GetDirectoryName(item.Path);

                if (!Directory.Exists(filePath))
                    Directory.CreateDirectory(filePath);

                StringBuilder info = new StringBuilder($"name = {Path.GetFileNameWithoutExtension(item.Path)}, fileName = N'{item.Path}'");

                info.AppendFormat(", size = {0}MB", item.Size > 0 ? item.Size / 1024 : 2);
                info.AppendFormat(", fileGrowth = {0}{1}", item.Growth > 0 ? (item.GrowthMode == GrowthMode.Percentage ? (long)(item.Growth * 100) : (long)item.Growth / 1024) : 64, item.Growth > 0 && item.GrowthMode == GrowthMode.Percentage ? "%" : "MB");

                if (item.MaxSize > 0)
                    info.AppendFormat(", maxSize = {0}MB", item.MaxSize / 1024);

                if (item.Type == FileType.Data)
                    primaryInfo = info.ToString();
                else if (item.Type == FileType.Log)
                    logInfo = info.ToString();
            }

            if (primaryInfo == null)
                throw new ArgumentException("创建数据库时保存数据库的主文件信息不能为空");

            StringBuilder createSql = new StringBuilder($"create database {databaseInfo.Name} on primary ({primaryInfo})");

            if (logInfo != null)
                createSql.AppendFormat(" log on ({0})", logInfo);

            if (databaseInfo.ExtraInfo != null && databaseInfo.ExtraInfo is DatabaseExtraInfo extraInfo)
            {
                createSql.Append(Environment.NewLine);

                if (extraInfo.CompatibilityLevel >= 100)
                    createSql.AppendFormat("alter database {0} set compatibility_level = {1}{2}", databaseInfo.Name, extraInfo.CompatibilityLevel, Environment.NewLine);

                if (extraInfo.RecoveryMode != RecoveryMode.Default)
                    createSql.AppendFormat("alter database {0} set recovery {1}{2}", databaseInfo.Name, extraInfo.RecoveryMode == RecoveryMode.Full ? "full" : extraInfo.RecoveryMode == RecoveryMode.Simple ? "simple" : "bulk_logged", Environment.NewLine);

                if (!string.IsNullOrWhiteSpace(extraInfo.SortRule))
                    createSql.AppendFormat("alter database {0} set collate {1}{2}", databaseInfo.Name, extraInfo.SortRule, Environment.NewLine);

                if (!string.IsNullOrWhiteSpace(extraInfo.Owner))
                    createSql.AppendFormat("exec {0}.dbo.sp_changedbowner N'{1}'{2}", databaseInfo.Name, extraInfo.Owner, Environment.NewLine);
            }

            return createSql.ToString();
        }

        /// <summary>
        /// 使用指定的原始数据源信息生成创建该数据表或视图的 T-Sql 语句。
        /// </summary>
        /// <param name="dataSource">待生成创建 T-Sql 语句的数据源信息。</param>
        /// <returns>生成好用于创建该数据表或视图的 T-Sql 语句。</returns>
        internal string GenerateCreateTableSql(OriginalDataSource dataSource)
        {
            StringBuilder fields = new StringBuilder();
            StringBuilder uniqueConstraints = new StringBuilder();
            StringBuilder extendedPropertys = new StringBuilder();
            StringBuilder indices = new StringBuilder();

            foreach (var item in ((ObjectField)dataSource.RootField).Members)
            {
                OriginalField field = (OriginalField)item.Value.Field;

                if (fields.Length > 0)
                    fields.AppendFormat(",{0}", Environment.NewLine);

                fields.Append($"{field.FieldName} {field.FieldType}");

                if (field.IsPrimaryKey)
                {
                    fields.Append(" primary key");
                }
                else
                {
                    if (field.DefaultValue != null)
                    {
                        string valueType = field.DataType.FullName;

                        if (valueType == "System.Int32"
                            || valueType == "System.UInt32"
                            || valueType == "System.Int64"
                            || valueType == "System.UInt64"
                            || valueType == "System.Int16"
                            || valueType == "System.UInt16"
                            || valueType == "System.Byte"
                            || valueType == "System.SByte"
                            || valueType == "System.Boolean"
                            || field.DataType.IsEnum
                            || (new Regex(@"^\s*getdate\s*\(\s*\)\s*$", RegexOptions.IgnoreCase).IsMatch(field.DefaultValue.ToString()) && valueType == "System.DateTime")
                            || (new Regex(@"^\s*newid\s*\(\s*\)\s*$", RegexOptions.IgnoreCase).IsMatch(field.DefaultValue.ToString()) && valueType == "System.Guid"))
                        {
                            fields.AppendFormat(" default {0}", field.DefaultValue);
                        }
                        else
                        {
                            fields.AppendFormat(" default '{0}'", field.DefaultValue);
                        }
                    }

                    if (field.IsUnique)
                    {
                        if (uniqueConstraints.Length > 0)
                            uniqueConstraints.AppendFormat(",{0}", Environment.NewLine);

                        uniqueConstraints.AppendFormat(" unique({0})", field.FieldName);
                    }

                    if (!field.IsNullable)
                        fields.Append(" not null");
                }

                if (field.IsAutoincrement)
                    fields.AppendFormat(" identity({0}, {1})", field.IncrementSeed, field.IncrementSpeed);

                if (!string.IsNullOrWhiteSpace(field.CheckConstraint))
                    fields.AppendFormat(" check({0})", field.CheckConstraint);

                if (!string.IsNullOrWhiteSpace(field.Explain))
                {
                    if (extendedPropertys.Length > 0)
                        extendedPropertys.Append(Environment.NewLine);

                    extendedPropertys.Append($"exec sp_addExtendedProperty 'MS_Description', N'{field.Explain}', N'user', N'dbo', N'table', N'{dataSource.Name}', N'column', N'{field.FieldName}'");
                }
            }

            if (dataSource.Indices != null && dataSource.Indices.Count > 0)
            {
                foreach (var item in dataSource.Indices)
                {
                    if (indices.Length > 0)
                        indices.Append(Environment.NewLine);

                    indices.Append($"create index {item.Name} on [dbo].{dataSource.Name}(");

                    for (int i = 0; i < item.Fields.Count; i++)
                    {
                        if (i > 0)
                            indices.Append(",");

                        indices.Append(item.Fields[i].FieldName);

                        if (item.Sort == SortType.Ascending)
                            indices.Append(" asc");
                        else
                            indices.Append(" desc");
                    }

                    indices.Append(")");
                }
            }

            StringBuilder createSql = new StringBuilder("create table [dbo].");

            createSql.Append(dataSource.Name);

            createSql.AppendLine("(");

            createSql.Append(fields);

            if (uniqueConstraints.Length > 0)
                createSql.AppendFormat(",{0}{1}", Environment.NewLine, uniqueConstraints);

            createSql.AppendFormat("{0}){0}", Environment.NewLine);

            if (indices.Length > 0)
                createSql.AppendFormat("{0}{1}", Environment.NewLine, indices);

            if (extendedPropertys.Length > 0)
                createSql.AppendFormat("{0}{1}", Environment.NewLine, extendedPropertys);

            return createSql.ToString();
        }
    }
}