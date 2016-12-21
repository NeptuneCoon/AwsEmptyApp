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

    public class DynamoDBHelper
    {
        private static AmazonDynamoDBClient client = new AmazonDynamoDBClient();

        public static void CreateTable(string tableName, List<KeySchemaElement> keySchema, List<AttributeDefinition> attributeDefinitions, ProvisionedThroughput provisionedThroughput)
        {
            /*
            var request2 = new CreateTableRequest
            {
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                  new AttributeDefinition
                  {
                    AttributeName = "Id",
                    AttributeType = "N"
                  },
                  new AttributeDefinition
                  {
                    AttributeName = "ReplyDateTime",
                    AttributeType = "N"
                  }
                },
                KeySchema = new List<KeySchemaElement>
                {
                  new KeySchemaElement
                  {
                    AttributeName = "Id",
                    KeyType = "HASH"  //Partition key
                  },
                  new KeySchemaElement
                  {
                    AttributeName = "ReplyDateTime",
                    KeyType = "RANGE"  //Sort key
                  }
                },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 5,
                    WriteCapacityUnits = 6
                },
                TableName = tableName
            };

            var response2 = client.CreateTable(request2);

            var tableDescription2 = response2.TableDescription;
            */

            var request = new CreateTableRequest();
            request.KeySchema = keySchema;
            request.AttributeDefinitions = attributeDefinitions;
            request.ProvisionedThroughput = provisionedThroughput;
            request.TableName = tableName;

            var response = client.CreateTable(request);

            var tableDescription = response.TableDescription;
        }

        /// <summary>
        /// 获取所有表的表名
        /// </summary>
        /// <returns></returns>
        public static List<string> ListAllTables()
        {
            List<string> tableNames = new List<string>();
            string lastTableNameEvaluated = null;
            do
            {
                var request = new ListTablesRequest
                {
                    Limit = 10,
                    ExclusiveStartTableName = lastTableNameEvaluated
                };

                var response = client.ListTables(request);
                foreach (string name in response.TableNames)
                    tableNames.Add(name);

                lastTableNameEvaluated = response.LastEvaluatedTableName;

            } while (lastTableNameEvaluated != null);
            return tableNames;
        }

        // 通过创建PutItemRequest对象创建Item
        public static void PutItem()
        {
            //AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
            //ddbConfig.RegionEndpoint = RegionEndpoint.APNortheast1;
            //AmazonDynamoDBClient client = new AmazonDynamoDBClient(ddbConfig);

            //AmazonDynamoDBClient client = new AmazonDynamoDBClient();
            string tableName = "ProductCatalog";

            var request = new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue>()
                {
                    { "Id", new AttributeValue { N = "201" }},
                    { "Title", new AttributeValue { S = "Book 201 Title" }},
                    { "ISBN", new AttributeValue { S = "11-11-11-11" }},
                    { "Price", new AttributeValue { S = "20.00" }},
                    {
                        "Authors",
                        new AttributeValue
                        {
                            SS = new List<string>{"Author1", "Author2"}
                        }
                    }
                }
            };

            var response = client.PutItem(request);
        }

        // 通过Table类的PutItem创建项目
        public static void PutItem2()
        {
            Table table = Table.LoadTable(client, "Student");
            Document document = new Document();
            document["Id"] = 201;
            document["Name"] = "Jay Chou";
            document["Age"] = 32;

            try
            {
                Document doc = table.PutItem(document);
            }
            catch (Exception ex)
            {

            }
        }

        /// <summary>
        /// 通过json转化为Document对象来创建项目
        /// </summary>
        public static void PutItem3()
        {
            Table table = Table.LoadTable(client, "ProductCatalog");
            Model.Book book = new Model.Book() { Id = 204, Title = "History Study2", Address = new Model.Address() { Street = "朝阳区北大街1号", Telecome = new Model.Telecome() { Area = "021", Tel = "64986666" } } };
            string jsonString = JsonConvert.SerializeObject(book);

            Document document = Document.FromJson(jsonString);
            Document doc = table.PutItem(document);
        }

        // 使用DeleteItemRequest来删除Item
        public static void DeleteItem()
        {
            var request = new DeleteItemRequest
            {
                TableName = "ProductCatalog",
                Key = new Dictionary<string, AttributeValue>()
                {
                    { "Id", new AttributeValue { N = "1000" } }
                },

                // Return the entire item as it appeared before the update.
                ReturnValues = "ALL_OLD",
                ExpressionAttributeNames = new Dictionary<string, string>()
                {
                    {"#IP", "InPublication"}
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                {
                    {":inpub",new AttributeValue {BOOL = false}}
                },
                ConditionExpression = "#IP = :inpub"
            };

            var response = client.DeleteItem(request);

            // Check the response.
            var attributeList = response.Attributes; // Attribute list in the response.
            // Print item.
            Console.WriteLine("\nPrinting item that was just deleted ............");
        }
        // 使用Table类的DeleteItem来删除Item
        public static void DeleteItem2()
        {
            Table table = Table.LoadTable(client, "Student");
            if (table != null)
            {
                //Document doc = table.DeleteItem(204, "History Study2", null);


                Document doc = table.DeleteItem(201);



                //Document doc = table.DeleteItem(
                //    new Primitive() { Type = DynamoDBEntryType.Numeric, Value = 203 },
                //    new Primitive() { Type = DynamoDBEntryType.String, Value = "History Study" },
                //    null
                //    );//按主键删除

            }
        }

        public static Table GetTableObject(string tableName)
        {


            // Now, create a Table object for the specified table
            Table table = null;
            try { table = Table.LoadTable(client, tableName); }
            catch (Exception ex)
            {
                Console.WriteLine("\n Error: failed to load the 'Movies' table; " + ex.Message);
                return (null);
            }
            return (table);
        }
    }
}
