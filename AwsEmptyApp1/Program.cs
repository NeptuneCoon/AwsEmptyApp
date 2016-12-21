using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DocumentModel;

// Add using statements to access AWS SDK for .NET services. 
// Both the Service and its Model namespace need to be added 
// in order to gain access to a service. For example, to access
// the EC2 service, add:
// using Amazon.EC2;
// using Amazon.EC2.Model;

namespace AwsEmptyApp1
{
    class Program
    {
        public static void Main(string[] args)
        {
            //CreateTable();
            DynamoDBHelper.PutItem2();
            //DynamoDBHelper.DeleteItem2();
            Console.WriteLine("finished");
            //DynamoDBHelper.PutItem();
            
            Console.Read();
        }

        public static void CreateTable()
        {
            string tableName = "ProductCatalog";
            // 表结构
            List<KeySchemaElement> keySchema = new List<KeySchemaElement>();
            keySchema.Add(new KeySchemaElement() { AttributeName = "Id", KeyType = "HASH" });//Partition key(分区键)
            keySchema.Add(new KeySchemaElement() { AttributeName = "Title", KeyType = "RANGE" });//Sort key(排序键)
            // 属性描述
            List<AttributeDefinition> attributeDefinition = new List<AttributeDefinition>();
            attributeDefinition.Add(new AttributeDefinition() { AttributeName = "Id", AttributeType = "N" });
            attributeDefinition.Add(new AttributeDefinition() { AttributeName = "Title", AttributeType = "S" });
            // 预留吞吐量
            ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput() { ReadCapacityUnits = 1, WriteCapacityUnits = 1 };

            DynamoDBHelper.CreateTable(tableName, keySchema, attributeDefinition, provisionedThroughput);
        }
    }
}