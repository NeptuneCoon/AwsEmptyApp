using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DocumentModel;
using Newtonsoft.Json;

namespace AwsEmptyApp1
{
    /// <summary>
    /// Amazon DynamoDB数据操作类
    /// 该类实现了DynamoDB有关数据操作的基本方法，基于Amazon DynamoDB SDK高级编程接口实现
    /// 如果需要更为底层的实现，请访问DynamoDB底层API：
    /// http://docs.amazonaws.cn/amazondynamodb/latest/developerguide/LowLevelDotNetItemsExample.html
    /// </summary>
    public class Class1
    {
        private static AmazonDynamoDBClient client = new AmazonDynamoDBClient();

        // create
        #region 向指定的表中添加项目
        /// <summary>
        /// 向指定的表中添加项目
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="obj">项目</param>
        /// <returns>操作结果</returns>
        public static DataResult PutItem(string tableName, object obj)
        {
            DataResult result = new DataResult();

            // 判断操作的对象是否为null
            if (obj == null) {
                result.Status = 202;
                result.Message = "操作的对象为 null";
                return result;
            }
            // 获取表对象，并判断表对象是否为null
            Table table = Table.LoadTable(client, tableName);
            if (table == null) {
                result.Status = 201;
                result.Message = "未找到表" + tableName;
                return result;
            }

            // 转化对象为json字符串，使用Document.FromJson方法将json字符串转化为Document对象
            string jsonString = JsonConvert.SerializeObject(obj);
            Document document = Document.FromJson(jsonString);

            // 执行插入操作
            try {
                table.PutItem(document);
            }
            catch (Exception ex) {
                result.Status = 205;
                result.Message = "失败";
                result.Exception = ex;
            }
            return result;
        }
        #endregion

        // delete
        #region 从指定的表中删除项目(按主键删除)
        /// <summary>
        /// 从指定的表中删除项目(按主键删除)
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="primaryKey">主键</param>
        /// <param name="config">删除配置</param>
        /// <returns>操作结果</returns>
        public static DataResult DeleteItem(string tableName, Primitive primaryKey, DeleteItemOperationConfig config = null)
        {
            DataResult result = new DataResult();

            // 获取表对象，并判断表对象是否为null
            Table table = Table.LoadTable(client, tableName);
            if (table == null) {
                result.Status = 201;
                result.Message = "未找到表" + tableName;
                return result;
            }
            try {
                table.DeleteItem(primaryKey, config);
            }
            catch (Exception ex) {
                result.Status = 205;
                result.Message = "失败";
                result.Exception = ex;
            }

            return result;
        }
        #endregion

        #region 从指定的表中删除项目(按主键+排序键删除)
        /// <summary>
        /// 从指定的表中删除项目(按主键+排序键删除)
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="primaryKey">主键</param>
        /// <param name="sortKey">排序键</param>
        /// <param name="config">删除配置</param>
        /// <returns>操作结果</returns>
        public static DataResult DeleteItem(string tableName, Primitive primaryKey, Primitive sortKey, DeleteItemOperationConfig config = null)
        {
            DataResult result = new DataResult();

            // 获取表对象，并判断表对象是否为null
            Table table = Table.LoadTable(client, tableName);
            if (table == null) {
                result.Status = 201;
                result.Message = "未找到表" + tableName;
                return result;
            }
            try {
                table.DeleteItem(primaryKey, sortKey, config);
            }
            catch (Exception ex) {
                result.Status = 205;
                result.Message = "失败";
                result.Exception = ex;
            }

            return result;
        }
        #endregion

        // retrieve
        #region 从指定的表中读取项目(按主键+排序键读取)
        /// <summary>
        /// 从指定的表中读取项目(按主键+排序键读取)
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="primaryKey">主键</param>
        /// <param name="config">读取配置</param>
        /// <returns>读取结果</returns>
        public static DataResult GetItem(string tableName, Primitive primaryKey, GetItemOperationConfig config = null)
        {
            DataResult result = new DataResult();

            // 获取表对象，并判断表对象是否为null
            Table table = Table.LoadTable(client, tableName);
            if (table == null) {
                result.Status = 201;
                result.Message = "未找到表" + tableName;
                return result;
            }

            try {
                table.GetItem(primaryKey, config);
            }
            catch (Exception ex) {
                result.Status = 205;
                result.Message = "失败";
                result.Exception = ex;
            }
            return result;
        }
        #endregion

        #region 从指定的表中读取项目(按主键+排序键读取)
        /// <summary>
        /// 从指定的表中读取项目(按主键+排序键读取)
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="primaryKey">主键</param>
        /// <param name="sortKey">排序键</param>
        /// <param name="config">读取配置</param>
        /// <returns>读取结果</returns>
        public static DataResult GetItem(string tableName, Primitive primaryKey, Primitive sortKey, GetItemOperationConfig config = null)
        {
            DataResult result = new DataResult();

            // 获取表对象，并判断表对象是否为null
            Table table = Table.LoadTable(client, tableName);
            if (table == null) {
                result.Status = 201;
                result.Message = "未找到表" + tableName;
                return result;
            }

            try {
                table.GetItem(primaryKey, sortKey, config);
            }
            catch (Exception ex) {
                result.Status = 205;
                result.Message = "失败";
                result.Exception = ex;
            }
            return result;
        }
        #endregion

        // TableQuery方法用来查询表，您只能查询具有复合主键(分区键和排序键)的表，如果表的主键只有分区键，则不支持Query操作。
        // http://docs.aws.amazon.com/zh_cn/amazondynamodb/latest/developerguide/QueryMidLevelDotNet.html
        public static DataResult TableQuery()
        {
            DataResult result = new DataResult();



            return result;
        }

        // update
        // http://docs.aws.amazon.com/zh_cn/amazondynamodb/latest/developerguide/ItemCRUDDotNetDocumentAPI.html
        // http://docs.aws.amazon.com/zh_cn/amazondynamodb/latest/gettingstartedguide/GettingStarted.NET.03.html#GettingStarted.NET.03.03
        public static DataResult UpdateConditionally()
        {

        }
    }
}
