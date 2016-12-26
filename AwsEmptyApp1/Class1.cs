using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Runtime;
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
        public static DataResult TableQuery(string tableName)
        {
            DataResult result = new DataResult();
            // 获取表对象，并判断表对象是否为null
            Table table = Table.LoadTable(client, tableName);
            if (table == null)
            {
                result.Status = 201;
                result.Message = "未找到表" + tableName;
                return result;
            }
            QueryFilter filter = new QueryFilter("Id", QueryOperator.Equal, 201);

            Search search = table.Query(filter);
            List<Document> documentSet = new List<Document>();
            do
            {
                documentSet = search.GetNextSet();
            } while (!search.IsDone);

            List<string> documentJsons = new List<string>();
            if (documentSet != null)
            {
                foreach (var doc in documentSet)
                {
                    documentJsons.Add(doc.ToJson());
                }
                result.JsonResult = "[" + string.Join(",", documentJsons) + "]";
            }

            return result;
        }


        // TableScan方法用来扫描表，Scan方法只有一个必需参数：扫描筛选条件
        // http://docs.aws.amazon.com/zh_cn/amazondynamodb/latest/developerguide/QueryMidLevelDotNet.html
        public static DataResult TableScan(string tableName)
        {
            DataResult result = new DataResult();
            // 获取表对象，并判断表对象是否为null
            Table table = Table.LoadTable(client, tableName);
            if (table == null)
            {
                result.Status = 201;
                result.Message = "未找到表" + tableName;
                return result;
            }

            ScanFilter filter = new ScanFilter();
            filter.AddCondition("Id", ScanOperator.GreaterThan, 200);

            Search search = table.Scan(filter);
            List<Document> documentSet = new List<Document>();
            do
            {
                documentSet = search.GetNextSet();
            } while (!search.IsDone);

            List<string> documentJsons = new List<string>();
            
            if (documentSet != null)
            {
                foreach(var doc in documentSet)
                {
                    documentJsons.Add(doc.ToJson());
                }
                result.JsonResult = "[" + string.Join(",", documentJsons) + "]";
            }

            string str = JsonConvert.SerializeObject(result);
            return result;
        }

        // update
        // http://docs.aws.amazon.com/zh_cn/amazondynamodb/latest/developerguide/LowLevelDotNetItemCRUD.html#UpdateItemLowLevelDotNet
        // http://docs.aws.amazon.com/zh_cn/amazondynamodb/latest/developerguide/ItemCRUDDotNetDocumentAPI.html
        // http://docs.aws.amazon.com/zh_cn/amazondynamodb/latest/gettingstartedguide/GettingStarted.NET.03.html#GettingStarted.NET.03.03
        public static DataResult UpdateConditionally()
        {
            return null;
        }

        public static DataResult UpdateMultipleAttributes(string tableName)
        {
            DataResult result = new DataResult();

            Table table = Table.LoadTable(client, tableName);
            if (table == null)
            {
                result.Status = 201;
                result.Message = "未找到表" + tableName;
                return result;
            }
            int partitionKey = 201;

            var stu = new Document();
            stu["Id"] = partitionKey;
            stu["Zone"] = new List<string> { "Author x", "Author y" };
            stu["Age"] = 38;

            // Optional parameters(可选参数)
            UpdateItemOperationConfig config = new UpdateItemOperationConfig
            {
                ReturnValues = ReturnValues.AllNewAttributes
            };
            Document updatedBook = table.UpdateItem(stu, config);

            return result;
        }

        public static DataResult UpdateConditionally(string tableName)
        {
            DataResult result = new DataResult();
            Table table = Table.LoadTable(client, tableName);
            if (table == null)
            {
                result.Status = 201;
                result.Message = "未找到表" + tableName;
                return result;
            }

            //int partitionKey = sampleBookId;

            var book = new Document();
            book["Id"] = 201;
            book["Age"] = 18;

            // For conditional price update, creating a condition expression. 
            Expression expr = new Expression();
            expr.ExpressionStatement = "Zone = :val";
            expr.ExpressionAttributeValues[":val"] = "Taiwan";

            // Optional parameters.
            UpdateItemOperationConfig config = new UpdateItemOperationConfig
            {
                ConditionalExpression = expr,
                ReturnValues = ReturnValues.AllNewAttributes
            };
            Document updatedBook = table.UpdateItem(book, config);

            return result;
        }

        /// <summary>
        /// 使用低级API实现的Update
        /// </summary>
        /// <param name="tableName"></param>
        public static void Update(string tableName)
        {
            var request = new UpdateItemRequest
            {
                TableName = tableName,
                Key = new Dictionary<string, AttributeValue>() { { "Id", new AttributeValue { N = "202" } } },
                ExpressionAttributeNames = new Dictionary<string, string>()
                {
                    {"#A", "Authors"},
                    {"#P", "Price"},
                    {"#NA", "NewAttribute"},
                    {"#I", "ISBN"}
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                {
                    {":auth",new AttributeValue { SS = {"Author YY","Author ZZ"}}},
                    {":p",new AttributeValue {N = "1"}},
                    {":newattr",new AttributeValue {S = "someValue"}},
                },

                // This expression does the following:
                // 1) Adds two new authors to the list
                // 2) Reduces the price
                // 3) Adds a new attribute to the item
                // 4) Removes the ISBN attribute from the item
                UpdateExpression = "ADD #A :auth SET #P = #P - :p, #NA = :newattr REMOVE #I"
            };
            var response = client.UpdateItem(request);
        }
    }
}
